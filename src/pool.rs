use std::collections::VecDeque;
use std::future::Future;
use std::ops::{Deref, DerefMut};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use tokio::sync::{Notify, oneshot};

use crate::config::{PoolConfig, Status};
use crate::error::{Error, Result};

pub(crate) type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// То, чем пул открывает и проверяет соединения. Реализация для PostgreSQL — в `postgres`.
pub(crate) trait Connector: Send + Sync + 'static {
    type Conn: Send + Sync + 'static;

    fn connect(&self) -> BoxFuture<'static, Result<Self::Conn>>;

    fn is_closed(conn: &Self::Conn) -> bool;

    fn check<'a>(&'a self, conn: &'a Self::Conn) -> BoxFuture<'a, bool>;
}

struct Slot<C> {
    conn: C,
    born: Instant,
    idle_since: Instant,
}

struct State<C> {
    /// Число живых соединений: на руках, в простое и в процессе открытия.
    size: usize,
    idle: VecDeque<Slot<C>>,
    waiters: VecDeque<oneshot::Sender<Wake<C>>>,
}

enum Wake<C> {
    Ready(Slot<C>),
    /// Освободилось место: нужно снова попытаться взять или открыть соединение.
    Retry,
    Closed,
}

enum Step<C> {
    Go(Slot<C>),
    Connect,
    Wait(oneshot::Receiver<Wake<C>>),
    TimedOut,
}

struct Shared<R: Connector> {
    connector: R,
    config: PoolConfig,
    state: Mutex<State<R::Conn>>,
    closed: AtomicBool,
    /// Сдвигает часы простоя и срока жизни. В бою остаётся нулём.
    offset_ms: AtomicU64,
    close_notify: Arc<Notify>,
}

pub(crate) struct Pool<R: Connector> {
    shared: Arc<Shared<R>>,
}

impl<R: Connector> Clone for Pool<R> {
    fn clone(&self) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
        }
    }
}

impl<R: Connector> std::fmt::Debug for Pool<R> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Pool")
            .field("status", &self.status())
            .field("closed", &self.is_closed())
            .finish()
    }
}

impl<R: Connector> std::fmt::Debug for PooledConnection<R> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PooledConnection")
            .finish_non_exhaustive()
    }
}

pub(crate) struct PooledConnection<R: Connector> {
    slot: Option<Slot<R::Conn>>,
    shared: Arc<Shared<R>>,
}

impl<R: Connector> Pool<R> {
    pub(crate) async fn new(connector: R, config: PoolConfig) -> Result<Self> {
        config.validate()?;
        let shared = Arc::new(Shared {
            connector,
            config,
            state: Mutex::new(State {
                size: 0,
                idle: VecDeque::new(),
                waiters: VecDeque::new(),
            }),
            closed: AtomicBool::new(false),
            offset_ms: AtomicU64::new(0),
            close_notify: Arc::new(Notify::new()),
        });
        if let Err(err) = shared.warmup().await {
            shared.close();
            return Err(err);
        }
        spawn_maintenance(Arc::clone(&shared));
        Ok(Self { shared })
    }

    pub(crate) async fn get(&self) -> Result<PooledConnection<R>> {
        self.get_timeout(self.shared.config.acquire_timeout).await
    }

    pub(crate) async fn get_timeout(&self, timeout: Duration) -> Result<PooledConnection<R>> {
        self.shared.acquire(timeout).await
    }

    pub(crate) fn status(&self) -> Status {
        self.shared.status()
    }

    pub(crate) fn close(&self) {
        self.shared.close();
    }

    pub(crate) fn is_closed(&self) -> bool {
        self.shared.closed.load(Ordering::Acquire)
    }

    #[cfg(test)]
    fn advance(&self, by: Duration) {
        self.shared.offset_ms.fetch_add(
            u64::try_from(by.as_millis()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
    }
}

impl<R: Connector> Shared<R> {
    fn lock(&self) -> MutexGuard<'_, State<R::Conn>> {
        self.state.lock().unwrap_or_else(|err| err.into_inner())
    }

    fn now(&self) -> Instant {
        Instant::now() + Duration::from_millis(self.offset_ms.load(Ordering::Relaxed))
    }

    fn new_slot(&self, conn: R::Conn) -> Slot<R::Conn> {
        let now = self.now();
        Slot {
            conn,
            born: now,
            idle_since: now,
        }
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    async fn warmup(self: &Arc<Self>) -> Result<()> {
        let mut opened = Vec::with_capacity(self.config.min_size);
        for _ in 0..self.config.min_size {
            if self.is_closed() {
                self.drop_opened(opened);
                return Err(Error::Closed);
            }
            {
                let mut state = self.lock();
                state.size += 1;
            }
            match self.connect_one().await {
                Ok(conn) => opened.push(self.new_slot(conn)),
                Err(err) => {
                    self.note_connect_failed();
                    self.drop_opened(opened);
                    return Err(err);
                }
            }
        }
        let mut state = self.lock();
        if self.is_closed() {
            drop(state);
            self.drop_opened(opened);
            return Err(Error::Closed);
        }
        state.idle.extend(opened);
        Ok(())
    }

    fn drop_opened(&self, opened: Vec<Slot<R::Conn>>) {
        let count = opened.len();
        drop(opened);
        if count == 0 {
            return;
        }
        let mut state = self.lock();
        state.size = state.size.saturating_sub(count);
    }

    async fn acquire(self: &Arc<Self>, timeout: Duration) -> Result<PooledConnection<R>> {
        if self.is_closed() {
            return Err(Error::Closed);
        }
        let started = tokio::time::Instant::now();
        loop {
            if self.is_closed() {
                return Err(Error::Closed);
            }
            let can_wait = !timeout.is_zero() && started.elapsed() < timeout;
            match self.reserve(can_wait) {
                Step::Go(slot) => {
                    if self.usable(&slot).await {
                        return Ok(PooledConnection {
                            slot: Some(slot),
                            shared: Arc::clone(self),
                        });
                    }
                    self.destroy(slot);
                }
                Step::Connect => match self.connect_one().await {
                    Ok(conn) => {
                        let slot = self.new_slot(conn);
                        if self.is_closed() {
                            self.destroy(slot);
                            return Err(Error::Closed);
                        }
                        return Ok(PooledConnection {
                            slot: Some(slot),
                            shared: Arc::clone(self),
                        });
                    }
                    Err(err) => {
                        self.note_connect_failed();
                        return Err(err);
                    }
                },
                Step::Wait(rx) => {
                    let spent = started.elapsed();
                    if spent >= timeout {
                        return Err(Error::AcquireTimeout(timeout));
                    }
                    match tokio::time::timeout(timeout - spent, rx).await {
                        Ok(Ok(Wake::Ready(slot))) => {
                            if self.usable(&slot).await {
                                return Ok(PooledConnection {
                                    slot: Some(slot),
                                    shared: Arc::clone(self),
                                });
                            }
                            self.destroy(slot);
                        }
                        Ok(Ok(Wake::Retry) | Err(_)) => {}
                        Ok(Ok(Wake::Closed)) => return Err(Error::Closed),
                        Err(_) => return Err(Error::AcquireTimeout(timeout)),
                    }
                }
                Step::TimedOut => return Err(Error::AcquireTimeout(timeout)),
            }
        }
    }

    fn reserve(self: &Arc<Self>, can_wait: bool) -> Step<R::Conn> {
        let (step, stale, deficit) = {
            let mut state = self.lock();
            sweep(&mut state);
            let stale = drain_unusable(&mut state.idle, |slot| self.idle_unusable(slot));
            state.size = state.size.saturating_sub(stale.len());
            self.wake(&mut state, stale.len());
            let ready = state.idle.pop_back();
            let step = if let Some(slot) = ready {
                Step::Go(slot)
            } else if state.size < self.config.max_size {
                state.size += 1;
                Step::Connect
            } else if !can_wait {
                Step::TimedOut
            } else {
                let (tx, rx) = oneshot::channel();
                state.waiters.push_back(tx);
                Step::Wait(rx)
            };
            let deficit = if self.is_closed() {
                0
            } else {
                self.config.min_size.saturating_sub(state.size)
            };
            (step, stale, deficit)
        };
        drop(stale);
        self.schedule_top_up(deficit);
        step
    }

    async fn usable(&self, slot: &Slot<R::Conn>) -> bool {
        !R::is_closed(&slot.conn)
            && !self.lifetime_exceeded(slot)
            && self.connector.check(&slot.conn).await
    }

    fn idle_unusable(&self, slot: &Slot<R::Conn>) -> bool {
        R::is_closed(&slot.conn) || self.lifetime_exceeded(slot) || self.idle_exceeded(slot)
    }

    fn lifetime_exceeded(&self, slot: &Slot<R::Conn>) -> bool {
        self.config
            .max_lifetime
            .is_some_and(|limit| self.now().saturating_duration_since(slot.born) >= limit)
    }

    fn idle_exceeded(&self, slot: &Slot<R::Conn>) -> bool {
        self.config
            .idle_timeout
            .is_some_and(|limit| self.now().saturating_duration_since(slot.idle_since) >= limit)
    }

    async fn connect_one(&self) -> Result<R::Conn> {
        let connect = self.connector.connect();
        match self.config.connect_timeout {
            Some(limit) => match tokio::time::timeout(limit, connect).await {
                Ok(result) => result,
                Err(_) => Err(Error::ConnectTimeout(limit)),
            },
            None => connect.await,
        }
    }

    fn release(self: &Arc<Self>, mut slot: Slot<R::Conn>) {
        if self.is_closed() || R::is_closed(&slot.conn) || self.lifetime_exceeded(&slot) {
            self.destroy(slot);
            return;
        }
        slot.idle_since = self.now();
        let mut state = self.lock();
        if self.is_closed() {
            drop(state);
            self.destroy(slot);
            return;
        }
        loop {
            let Some(tx) = pop_waiter(&mut state) else {
                state.idle.push_back(slot);
                return;
            };
            match tx.send(Wake::Ready(slot)) {
                Ok(()) => return,
                Err(Wake::Ready(returned)) => slot = returned,
                Err(Wake::Retry | Wake::Closed) => {
                    unreachable!("в очередь ожидания уходит только готовое соединение")
                }
            }
        }
    }

    fn destroy(self: &Arc<Self>, slot: Slot<R::Conn>) {
        drop(slot);
        let deficit = {
            let mut state = self.lock();
            state.size = state.size.saturating_sub(1);
            self.wake(&mut state, 1);
            if self.is_closed() {
                0
            } else {
                self.config.min_size.saturating_sub(state.size)
            }
        };
        self.schedule_top_up(deficit);
    }

    fn note_connect_failed(&self) {
        let mut state = self.lock();
        state.size = state.size.saturating_sub(1);
        self.wake(&mut state, 1);
    }

    fn schedule_top_up(self: &Arc<Self>, deficit: usize) {
        for _ in 0..deficit {
            let shared = Arc::clone(self);
            tokio::spawn(async move {
                shared.top_up_one().await;
            });
        }
    }

    async fn top_up_one(self: &Arc<Self>) {
        {
            let mut state = self.lock();
            if self.is_closed()
                || state.size >= self.config.min_size
                || state.size >= self.config.max_size
            {
                return;
            }
            state.size += 1;
        }
        match self.connect_one().await {
            Ok(conn) => self.release(self.new_slot(conn)),
            Err(_) => self.note_connect_failed(),
        }
    }

    async fn maintain(self: &Arc<Self>) {
        let stale = {
            let mut state = self.lock();
            let stale = drain_unusable(&mut state.idle, |slot| self.idle_unusable(slot));
            state.size = state.size.saturating_sub(stale.len());
            self.wake(&mut state, stale.len());
            stale
        };
        drop(stale);
        let attempts = self.config.min_size.max(1);
        for _ in 0..attempts {
            if self.is_closed() || self.status().size >= self.config.min_size {
                return;
            }
            let before = self.status().size;
            self.top_up_one().await;
            if self.status().size <= before {
                return;
            }
        }
    }

    fn status(&self) -> Status {
        let mut state = self.lock();
        sweep(&mut state);
        Status {
            size: state.size,
            idle: state.idle.len(),
            waiting: state.waiters.len(),
        }
    }

    fn close(&self) {
        self.closed.store(true, Ordering::Release);
        let (idle, waiters) = {
            let mut state = self.lock();
            let idle = std::mem::take(&mut state.idle);
            state.size = state.size.saturating_sub(idle.len());
            let waiters = std::mem::take(&mut state.waiters);
            (idle, waiters)
        };
        drop(idle);
        for tx in waiters {
            let _ = tx.send(Wake::Closed);
        }
        self.close_notify.notify_waiters();
    }

    fn wake(&self, state: &mut State<R::Conn>, mut count: usize) {
        while count > 0 {
            let Some(tx) = pop_waiter(state) else {
                return;
            };
            if tx.send(Wake::Retry).is_ok() {
                count -= 1;
            }
        }
    }
}

impl<R: Connector> Drop for Shared<R> {
    fn drop(&mut self) {
        self.close_notify.notify_waiters();
    }
}

impl<R: Connector> Drop for PooledConnection<R> {
    fn drop(&mut self) {
        if let Some(slot) = self.slot.take() {
            self.shared.release(slot);
        }
    }
}

impl<R: Connector> Deref for PooledConnection<R> {
    type Target = R::Conn;

    fn deref(&self) -> &R::Conn {
        &self
            .slot
            .as_ref()
            .expect("соединение ещё лежит в guard")
            .conn
    }
}

impl<R: Connector> DerefMut for PooledConnection<R> {
    fn deref_mut(&mut self) -> &mut R::Conn {
        &mut self
            .slot
            .as_mut()
            .expect("соединение ещё лежит в guard")
            .conn
    }
}

fn spawn_maintenance<R: Connector>(shared: Arc<Shared<R>>) {
    let interval = shared.config.maintenance_interval;
    let notify = Arc::clone(&shared.close_notify);
    let weak = Arc::downgrade(&shared);
    drop(shared);
    tokio::spawn(async move {
        loop {
            let notified = notify.notified();
            tokio::pin!(notified);
            let gone = match weak.upgrade() {
                None => true,
                Some(shared) => shared.is_closed(),
            };
            if gone {
                break;
            }
            tokio::select! {
                _ = tokio::time::sleep(interval) => {}
                _ = &mut notified => break,
            }
            let Some(shared) = weak.upgrade() else {
                break;
            };
            if shared.is_closed() {
                break;
            }
            shared.maintain().await;
        }
    });
}

fn sweep<C>(state: &mut State<C>) {
    state.waiters.retain(|tx| !tx.is_closed());
}

fn pop_waiter<C>(state: &mut State<C>) -> Option<oneshot::Sender<Wake<C>>> {
    while let Some(tx) = state.waiters.pop_front() {
        if !tx.is_closed() {
            return Some(tx);
        }
    }
    None
}

fn drain_unusable<C>(
    idle: &mut VecDeque<Slot<C>>,
    mut unusable: impl FnMut(&Slot<C>) -> bool,
) -> Vec<Slot<C>> {
    let mut kept = VecDeque::with_capacity(idle.len());
    let mut stale = Vec::new();
    while let Some(slot) = idle.pop_front() {
        if unusable(&slot) {
            stale.push(slot);
        } else {
            kept.push_back(slot);
        }
    }
    *idle = kept;
    stale
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use super::*;

    struct MockConnector {
        inner: Arc<MockInner>,
    }

    struct MockInner {
        created: AtomicUsize,
        live: AtomicUsize,
        peak: AtomicUsize,
        fail_remaining: AtomicUsize,
        /// Сколько успешных открытий пропустить, прежде чем начать отказывать.
        allow_before_fail: AtomicUsize,
        next_id: AtomicU64,
        delay: Mutex<Duration>,
    }

    struct MockConn {
        id: u64,
        closed: AtomicBool,
        inner: Arc<MockInner>,
    }

    impl Drop for MockConn {
        fn drop(&mut self) {
            self.inner.live.fetch_sub(1, Ordering::AcqRel);
        }
    }

    impl MockConnector {
        fn new() -> Self {
            Self {
                inner: Arc::new(MockInner {
                    created: AtomicUsize::new(0),
                    live: AtomicUsize::new(0),
                    peak: AtomicUsize::new(0),
                    fail_remaining: AtomicUsize::new(0),
                    allow_before_fail: AtomicUsize::new(0),
                    next_id: AtomicU64::new(0),
                    delay: Mutex::new(Duration::ZERO),
                }),
            }
        }

        fn created(&self) -> usize {
            self.inner.created.load(Ordering::Acquire)
        }

        fn live(&self) -> usize {
            self.inner.live.load(Ordering::Acquire)
        }

        fn peak(&self) -> usize {
            self.inner.peak.load(Ordering::Acquire)
        }

        fn fail_times(&self, count: usize) {
            self.inner.allow_before_fail.store(0, Ordering::Release);
            self.inner.fail_remaining.store(count, Ordering::Release);
        }

        fn fail_after_successes(&self, successes: usize) {
            self.inner
                .allow_before_fail
                .store(successes, Ordering::Release);
            self.inner.fail_remaining.store(1, Ordering::Release);
        }

        fn set_delay(&self, delay: Duration) {
            *self
                .inner
                .delay
                .lock()
                .unwrap_or_else(|err| err.into_inner()) = delay;
        }
    }

    impl Clone for MockConnector {
        fn clone(&self) -> Self {
            Self {
                inner: Arc::clone(&self.inner),
            }
        }
    }

    impl Connector for MockConnector {
        type Conn = MockConn;

        fn connect(&self) -> BoxFuture<'static, Result<MockConn>> {
            let inner = Arc::clone(&self.inner);
            Box::pin(async move {
                let delay = *inner.delay.lock().unwrap_or_else(|err| err.into_inner());
                if !delay.is_zero() {
                    tokio::time::sleep(delay).await;
                }
                loop {
                    let allow = inner.allow_before_fail.load(Ordering::Acquire);
                    if allow > 0 {
                        if inner
                            .allow_before_fail
                            .compare_exchange(allow, allow - 1, Ordering::AcqRel, Ordering::Acquire)
                            .is_ok()
                        {
                            break;
                        }
                        continue;
                    }
                    let left = inner.fail_remaining.load(Ordering::Acquire);
                    if left == 0 {
                        break;
                    }
                    if inner
                        .fail_remaining
                        .compare_exchange(left, left - 1, Ordering::AcqRel, Ordering::Acquire)
                        .is_ok()
                    {
                        return Err(Error::Connect("mock: соединение отклонено".to_owned()));
                    }
                }
                let id = inner.next_id.fetch_add(1, Ordering::AcqRel) + 1;
                let live = inner.live.fetch_add(1, Ordering::AcqRel) + 1;
                inner.peak.fetch_max(live, Ordering::AcqRel);
                inner.created.fetch_add(1, Ordering::AcqRel);
                Ok(MockConn {
                    id,
                    closed: AtomicBool::new(false),
                    inner,
                })
            })
        }

        fn is_closed(conn: &MockConn) -> bool {
            conn.closed.load(Ordering::Acquire)
        }

        fn check<'a>(&'a self, conn: &'a MockConn) -> BoxFuture<'a, bool> {
            Box::pin(async move { !conn.closed.load(Ordering::Acquire) })
        }
    }

    fn config(max_size: usize) -> PoolConfig {
        PoolConfig {
            max_size,
            min_size: 0,
            acquire_timeout: Duration::from_secs(2),
            connect_timeout: Some(Duration::from_secs(2)),
            idle_timeout: None,
            max_lifetime: None,
            maintenance_interval: Duration::from_secs(3600),
        }
    }

    fn failure<T>(result: Result<T>) -> Error {
        match result {
            Ok(_) => panic!("ожидалась ошибка"),
            Err(err) => err,
        }
    }

    async fn pool(max_size: usize) -> (Pool<MockConnector>, MockConnector) {
        let connector = MockConnector::new();
        let pool = Pool::new(connector.clone(), config(max_size))
            .await
            .unwrap();
        (pool, connector)
    }

    #[tokio::test]
    async fn reuses_the_same_connection() {
        let (pool, connector) = pool(2).await;
        let first = pool.get().await.unwrap();
        let first_id = first.id;
        drop(first);

        let second = pool.get().await.unwrap();
        assert_eq!(second.id, first_id);
        assert_eq!(connector.created(), 1);
        assert_eq!(pool.status().idle, 0);
        assert_eq!(pool.status().in_use(), 1);
        drop(second);
        assert_eq!(pool.status().idle, 1);
        assert_eq!(pool.status().in_use(), 0);
    }

    #[tokio::test]
    async fn checkout_is_lifo() {
        let connector = MockConnector::new();
        let mut cfg = config(2);
        cfg.min_size = 2;
        let pool = Pool::new(connector, cfg).await.unwrap();
        let conn = pool.get().await.unwrap();
        assert_eq!(conn.id, 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn does_not_exceed_max_size() {
        let (pool, connector) = pool(4).await;
        let (release_tx, release_rx) = tokio::sync::watch::channel(false);
        let mut tasks = Vec::new();
        for _ in 0..32 {
            let pool = pool.clone();
            let mut release_rx = release_rx.clone();
            tasks.push(tokio::spawn(async move {
                let conn = pool.get().await.unwrap();
                let _ = release_rx.changed().await;
                drop(conn);
            }));
        }

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let status = pool.status();
                if status.in_use() == 4 && status.waiting >= 28 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("пул не вышел на max_size");

        assert_eq!(connector.created(), 4);
        assert_eq!(connector.peak(), 4);
        release_tx.send(true).unwrap();
        for task in tasks {
            task.await.unwrap();
        }
        assert_eq!(connector.peak(), 4);
        assert_eq!(pool.status().size, 4);
        assert_eq!(pool.status().idle, 4);
    }

    #[tokio::test]
    async fn acquire_timeout_does_not_open_a_second_connection() {
        let (pool, connector) = pool(1).await;
        let held = pool.get().await.unwrap();
        let err = failure(pool.get_timeout(Duration::from_millis(40)).await);
        assert!(matches!(err, Error::AcquireTimeout(_)));
        assert_eq!(pool.status().waiting, 0);
        assert_eq!(connector.created(), 1);
        drop(held);
        let again = pool.get().await.unwrap();
        assert_eq!(again.id, 1);
        assert_eq!(connector.created(), 1);
    }

    #[tokio::test]
    async fn try_get_returns_immediately_when_pool_is_full() {
        let (pool, connector) = pool(1).await;
        let held = pool.get().await.unwrap();
        let err = failure(pool.get_timeout(Duration::ZERO).await);
        assert!(matches!(err, Error::AcquireTimeout(timeout) if timeout.is_zero()));
        assert_eq!(connector.created(), 1);
        drop(held);
    }

    #[tokio::test]
    async fn closed_connection_is_replaced() {
        let (pool, connector) = pool(2).await;
        let conn = pool.get().await.unwrap();
        conn.closed.store(true, Ordering::Release);
        drop(conn);
        assert_eq!(connector.live(), 0);

        let fresh = pool.get().await.unwrap();
        assert_eq!(fresh.id, 2);
        assert_eq!(connector.created(), 2);
        assert_eq!(connector.live(), 1);
    }

    #[tokio::test]
    async fn idle_timeout_discards_connection() {
        let connector = MockConnector::new();
        let mut cfg = config(2);
        cfg.idle_timeout = Some(Duration::from_secs(30));
        let pool = Pool::new(connector.clone(), cfg).await.unwrap();
        drop(pool.get().await.unwrap());
        pool.advance(Duration::from_secs(30));
        let fresh = pool.get().await.unwrap();
        assert_eq!(fresh.id, 2);
        assert_eq!(connector.created(), 2);
        assert_eq!(connector.live(), 1);
    }

    #[tokio::test]
    async fn max_lifetime_discards_connection_after_use() {
        let connector = MockConnector::new();
        let mut cfg = config(2);
        cfg.max_lifetime = Some(Duration::from_secs(60));
        let pool = Pool::new(connector.clone(), cfg).await.unwrap();
        let conn = pool.get().await.unwrap();
        pool.advance(Duration::from_secs(60));
        drop(conn);
        let fresh = pool.get().await.unwrap();
        assert_eq!(fresh.id, 2);
        assert_eq!(connector.live(), 1);
    }

    #[tokio::test]
    async fn connect_failure_does_not_stick_a_slot() {
        let connector = MockConnector::new();
        connector.fail_times(1);
        let pool = Pool::new(connector.clone(), config(2)).await.unwrap();
        let err = failure(pool.get().await);
        assert!(matches!(err, Error::Connect(_)));
        assert_eq!(pool.status().size, 0);
        assert_eq!(connector.live(), 0);

        let conn = pool.get().await.unwrap();
        assert_eq!(conn.id, 1);
        assert_eq!(pool.status().size, 1);
    }

    #[tokio::test]
    async fn connect_timeout_does_not_stick_a_slot() {
        let connector = MockConnector::new();
        connector.set_delay(Duration::from_secs(5));
        let mut cfg = config(1);
        cfg.connect_timeout = Some(Duration::from_millis(30));
        let pool = Pool::new(connector.clone(), cfg).await.unwrap();
        let err = failure(pool.get().await);
        assert!(matches!(err, Error::ConnectTimeout(_)));
        assert_eq!(pool.status().size, 0);
        assert_eq!(connector.live(), 0);
        assert_eq!(connector.created(), 0);
    }

    #[tokio::test]
    async fn min_size_opens_connections_up_front() {
        let connector = MockConnector::new();
        let mut cfg = config(4);
        cfg.min_size = 3;
        let pool = Pool::new(connector.clone(), cfg).await.unwrap();
        let status = pool.status();
        assert_eq!(status.size, 3);
        assert_eq!(status.idle, 3);
        assert_eq!(connector.created(), 3);
        assert_eq!(connector.live(), 3);
    }

    #[tokio::test]
    async fn min_size_failure_drops_what_it_opened() {
        let connector = MockConnector::new();
        connector.fail_after_successes(1);
        let mut cfg = config(4);
        cfg.min_size = 2;
        let err = failure(Pool::new(connector.clone(), cfg).await);
        assert!(matches!(err, Error::Connect(_)));
        assert_eq!(connector.created(), 1);
        assert_eq!(connector.live(), 0);
    }

    #[tokio::test]
    async fn close_unblocks_waiter_and_rejects_new_checkout() {
        let (pool, connector) = pool(1).await;
        let held = pool.get().await.unwrap();
        let waiting = {
            let pool = pool.clone();
            tokio::spawn(async move { pool.get().await })
        };
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if pool.status().waiting == 1 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("ожидающий не встал в очередь");

        pool.close();
        let err = failure(waiting.await.unwrap());
        assert!(matches!(err, Error::Closed));
        assert!(pool.is_closed());
        assert!(matches!(failure(pool.get().await), Error::Closed));
        drop(held);
        assert_eq!(connector.live(), 0);
    }

    #[tokio::test]
    async fn dropping_the_pool_drops_idle_connections() {
        let (pool, connector) = pool(2).await;
        let conn = pool.get().await.unwrap();
        let extra = pool.clone();
        drop(pool);
        assert_eq!(connector.live(), 1);
        drop(conn);
        drop(extra);
        assert_eq!(connector.live(), 0);
    }

    #[tokio::test]
    async fn rejects_invalid_config_without_connecting() {
        let connector = MockConnector::new();
        let mut cfg = config(1);
        cfg.min_size = 2;
        let err = failure(Pool::new(connector.clone(), cfg).await);
        assert!(matches!(err, Error::Config(_)));
        assert_eq!(connector.created(), 0);
    }
}
