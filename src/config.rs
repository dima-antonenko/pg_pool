use std::time::Duration;

use crate::error::{Error, Result};

/// Пределы пула. Ноль в полях `Option` недопустим: чтобы выключить предел, передайте `None`.
///
/// `acquire_timeout`, равный нулю, допустим: [`crate::PgPool::get_timeout`] тогда не ждёт,
/// пока другое задание вернёт соединение.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PoolConfig {
    /// Верхняя граница открытых соединений, включая выданные вызывающему.
    pub max_size: usize,
    /// Сколько соединений держать открытыми, когда пул простаивает.
    ///
    /// Ноль означает «не открывать ничего заранее». Если соединения закрылись
    /// из-за простоя или срока жизни и пул опустился ниже этого числа,
    /// фоновая задача добирает недостающие.
    pub min_size: usize,
    /// Сколько ждать, пока вернут уже открытое соединение, когда пул упёрся в `max_size`.
    pub acquire_timeout: Duration,
    /// Предел ожидания нового TCP-соединения. `None` — ждать, пока решит драйвер.
    pub connect_timeout: Option<Duration>,
    /// Сколько соединение может простоять в пуле без выдачи. `None` — не закрывать за простой.
    pub idle_timeout: Option<Duration>,
    /// Сколько соединение живёт с момента открытия, даже если им пользуются. `None` — без предела.
    pub max_lifetime: Option<Duration>,
    /// Как часто выбрасывать протухшие простаивающие соединения и добирать пул до `min_size`.
    pub maintenance_interval: Duration,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            max_size: 10,
            min_size: 0,
            acquire_timeout: Duration::from_secs(30),
            connect_timeout: Some(Duration::from_secs(10)),
            idle_timeout: Some(Duration::from_secs(10 * 60)),
            max_lifetime: Some(Duration::from_secs(30 * 60)),
            maintenance_interval: Duration::from_secs(30),
        }
    }
}

impl PoolConfig {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.max_size == 0 {
            return Err(Error::Config("max_size должен быть не меньше 1".to_owned()));
        }
        if self.min_size > self.max_size {
            return Err(Error::Config(
                "min_size не может быть больше max_size".to_owned(),
            ));
        }
        if self.maintenance_interval.is_zero() {
            return Err(Error::Config(
                "maintenance_interval должен быть больше нуля".to_owned(),
            ));
        }
        if self.connect_timeout.is_some_and(|limit| limit.is_zero()) {
            return Err(Error::Config(
                "connect_timeout должен быть больше нуля либо None".to_owned(),
            ));
        }
        if self.idle_timeout.is_some_and(|limit| limit.is_zero()) {
            return Err(Error::Config(
                "idle_timeout должен быть больше нуля либо None".to_owned(),
            ));
        }
        if self.max_lifetime.is_some_and(|limit| limit.is_zero()) {
            return Err(Error::Config(
                "max_lifetime должен быть больше нуля либо None".to_owned(),
            ));
        }
        Ok(())
    }
}

/// Снимок пула. Числа могли измениться сразу после чтения: снимок не блокирует выдачу.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Status {
    /// Открытые соединения: и те, что на руках, и те, что ждут в пуле.
    pub size: usize,
    /// Соединения в пуле, которые можно выдать сразу.
    pub idle: usize,
    /// Задания, которые ждут, пока кто-нибудь вернёт соединение.
    pub waiting: usize,
}

impl Status {
    /// Соединения, которые сейчас находятся у вызывающего кода.
    pub fn in_use(self) -> usize {
        self.size.saturating_sub(self.idle)
    }
}
