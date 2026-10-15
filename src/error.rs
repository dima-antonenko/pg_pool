use std::time::Duration;

/// Результат операции пула.
pub type Result<T> = std::result::Result<T, Error>;

/// Ошибка пула или драйвера PostgreSQL.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Драйвер PostgreSQL вернул ошибку: разбор строки, TCP, протокол или запрос.
    #[error(transparent)]
    Postgres(#[from] tokio_postgres::Error),

    /// За `acquire_timeout` никто не вернул соединение, а новое открыть уже нельзя.
    #[error("не дождались свободного соединения за {0:?}")]
    AcquireTimeout(Duration),

    /// Открытие нового TCP-соединения не уложилось в `connect_timeout`.
    #[error("не удалось открыть соединение за {0:?}")]
    ConnectTimeout(Duration),

    /// Пул закрыт вызовом [`crate::PgPool::close`] либо ещё не успел открыться.
    #[error("пул закрыт")]
    Closed,

    /// Некорректные пределы пула: размеры, нулевые таймауты.
    #[error("{0}")]
    Config(String),

    /// Сбой установки соединения, который сообщил не драйвер PostgreSQL.
    #[error("соединение: {0}")]
    Connect(String),
}
