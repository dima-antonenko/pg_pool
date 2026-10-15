use std::ops::{Deref, DerefMut};
use std::time::Duration;

use tokio_postgres::Client;
use tokio_postgres::NoTls;

use crate::config::{PoolConfig, Status};
use crate::error::{Error, Result};
use crate::pool::{BoxFuture, Connector, Pool, PooledConnection};

struct PgConnector {
    conn_str: String,
    test_on_checkout: bool,
}

impl Connector for PgConnector {
    type Conn = Client;

    fn connect(&self) -> BoxFuture<'static, Result<Client>> {
        let conn_str = self.conn_str.clone();
        Box::pin(async move {
            let config: tokio_postgres::Config = conn_str.parse()?;
            let (client, connection) = config.connect(NoTls).await?;
            tokio::spawn(async move {
                // Обрыв сокета закрывает `Client`: следующая выдача увидит `is_closed`.
                let _ = connection.await;
            });
            Ok(client)
        })
    }

    fn is_closed(conn: &Client) -> bool {
        conn.is_closed()
    }

    fn check<'a>(&'a self, conn: &'a Client) -> BoxFuture<'a, bool> {
        let test_on_checkout = self.test_on_checkout;
        Box::pin(async move {
            if conn.is_closed() {
                return false;
            }
            if !test_on_checkout {
                return true;
            }
            conn.simple_query("SELECT 1").await.is_ok()
        })
    }
}

/// Пул сессий PostgreSQL. Клонируется дёшево: клоны делят один набор соединений.
///
/// Строка подключения — URL (`postgres://user:pass@localhost:5432/db`) или
/// ключи libpq (`host=localhost user=app dbname=app`). Параметр `connect_timeout`
/// в строке ограничивает сам драйвер; [`PgPoolBuilder::connect_timeout`] ограничивает
/// ожидание на стороне пула.
///
/// TLS нет: `sslmode=require` завершится ошибкой драйвера. Для локального сервера
/// подойдёт `sslmode=disable`.
///
/// ```no_run
/// # async fn demo() -> pg_pool::Result<()> {
/// let pool = pg_pool::PgPool::connect("postgres://localhost/postgres").await?;
/// let conn = pool.get().await?;
/// conn.simple_query("SELECT 1").await?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct PgPool {
    inner: Pool<PgConnector>,
}

/// Соединение, выданное из [`PgPool`].
///
/// Пока значение живёт, место в пуле занято. `Drop` возвращает сессию в пул,
/// если она ещё открыта и не вышла за `max_lifetime`. Иначе пул её закрывает.
pub struct PooledClient {
    inner: PooledConnection<PgConnector>,
}

/// Сборщик [`PgPool`]. Строка подключения проверяется сразу, TCP ещё не открывается.
///
/// Отладочный вывод не содержит строку подключения: в ней может быть пароль.
pub struct PgPoolBuilder {
    connector: PgConnector,
    config: PoolConfig,
}

impl PgPool {
    /// Пул с пределами по умолчанию.
    pub async fn connect(conn_str: impl Into<String>) -> Result<Self> {
        Self::builder(conn_str)?.build().await
    }

    /// Разбор строки подключения без открытия соединений.
    pub fn builder(conn_str: impl Into<String>) -> Result<PgPoolBuilder> {
        let conn_str = conn_str.into();
        let _: tokio_postgres::Config = conn_str.parse().map_err(Error::Postgres)?;
        Ok(PgPoolBuilder {
            connector: PgConnector {
                conn_str,
                test_on_checkout: false,
            },
            config: PoolConfig::default(),
        })
    }

    /// Взять соединение, ожидая не дольше `acquire_timeout` из конфигурации.
    pub async fn get(&self) -> Result<PooledClient> {
        Ok(PooledClient {
            inner: self.inner.get().await?,
        })
    }

    /// Взять соединение с отдельным пределом ожидания.
    ///
    /// Ноль значит «не вставать в очередь». Если открытых соединений меньше `max_size`,
    /// пул всё равно попробует открыть новое; это ожидание ограничивает `connect_timeout`.
    pub async fn get_timeout(&self, timeout: Duration) -> Result<PooledClient> {
        Ok(PooledClient {
            inner: self.inner.get_timeout(timeout).await?,
        })
    }

    /// То же, что [`Self::get_timeout`] с нулевым ожиданием очереди.
    pub async fn try_get(&self) -> Result<PooledClient> {
        self.get_timeout(Duration::ZERO).await
    }

    /// Сколько соединений открыто, свободно и сколько заданий их ждут.
    pub fn status(&self) -> Status {
        self.inner.status()
    }

    /// Закрыть пул.
    ///
    /// Простаивающие соединения уничтожаются сразу. Кто ждал в очереди, получает
    /// [`Error::Closed`]. Уже выданное соединение можно доиспользовать: в пул оно не вернётся.
    pub fn close(&self) {
        self.inner.close();
    }

    /// Пул уже закрыт.
    pub fn is_closed(&self) -> bool {
        self.inner.is_closed()
    }
}

impl std::fmt::Debug for PgPoolBuilder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PgPoolBuilder")
            .field("max_size", &self.config.max_size)
            .field("min_size", &self.config.min_size)
            .field("test_on_checkout", &self.connector.test_on_checkout)
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for PgPool {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PgPool")
            .field("status", &self.status())
            .field("closed", &self.is_closed())
            .finish()
    }
}

impl PgPoolBuilder {
    /// Не больше стольких сессий одновременно.
    pub fn max_size(mut self, value: usize) -> Self {
        self.config.max_size = value;
        self
    }

    /// Сколько сессий открыть сразу и поддерживать в простое.
    pub fn min_size(mut self, value: usize) -> Self {
        self.config.min_size = value;
        self
    }

    /// Сколько ждать свободную сессию, когда пул уже полный.
    pub fn acquire_timeout(mut self, value: Duration) -> Self {
        self.config.acquire_timeout = value;
        self
    }

    /// Сколько ждать установку нового TCP-соединения. `None` отключает предел пула.
    pub fn connect_timeout(mut self, value: Option<Duration>) -> Self {
        self.config.connect_timeout = value;
        self
    }

    /// Через сколько простоя сессия закрывается. `None` — не закрывать за простой.
    pub fn idle_timeout(mut self, value: Option<Duration>) -> Self {
        self.config.idle_timeout = value;
        self
    }

    /// Максимальный возраст сессии с момента открытия. `None` — без предела.
    pub fn max_lifetime(mut self, value: Option<Duration>) -> Self {
        self.config.max_lifetime = value;
        self
    }

    /// Период фоновой чистки простаивающих сессий.
    pub fn maintenance_interval(mut self, value: Duration) -> Self {
        self.config.maintenance_interval = value;
        self
    }

    /// Перед выдачей простаивавшей сессии выполнять `SELECT 1`.
    pub fn test_on_checkout(mut self, value: bool) -> Self {
        self.connector.test_on_checkout = value;
        self
    }

    /// Открыть `min_size` соединений и запустить чистку.
    ///
    /// Вызывать внутри runtime Tokio: пул ставит фоновые задачи через `tokio::spawn`.
    pub async fn build(self) -> Result<PgPool> {
        Ok(PgPool {
            inner: Pool::new(self.connector, self.config).await?,
        })
    }
}

impl Deref for PooledClient {
    type Target = Client;

    fn deref(&self) -> &Client {
        &self.inner
    }
}

impl DerefMut for PooledClient {
    fn deref_mut(&mut self) -> &mut Client {
        &mut self.inner
    }
}

impl AsRef<Client> for PooledClient {
    fn as_ref(&self) -> &Client {
        self
    }
}

impl std::fmt::Debug for PooledClient {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PooledClient")
            .field("closed", &self.is_closed())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_rejects_a_bad_connection_string() {
        let err = PgPool::builder("это не строка подключения").unwrap_err();
        assert!(matches!(err, Error::Postgres(_)));
    }

    #[tokio::test]
    async fn builder_rejects_zero_max_size_before_connect() {
        let err = PgPool::builder("postgres://localhost/postgres")
            .unwrap()
            .max_size(0)
            .build()
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Config(_)));
    }
}
