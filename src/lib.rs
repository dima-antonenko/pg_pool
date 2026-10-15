//! Асинхронный пул соединений PostgreSQL.
//!
//! Пул живёт в процессе приложения и выдаёт готовые соединения `tokio-postgres`.
//! Это не прокси вроде PgBouncer: каждое соединение — обычная сессия с сервером.
//!
//! Соединение возвращается в пул, когда guard [`PooledClient`] уничтожается.
//! Если сессия уже закрыта или вышла за `max_lifetime`, пул её выбрасывает.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

mod config;
mod error;
mod pool;
mod postgres;

pub use config::{PoolConfig, Status};
pub use error::{Error, Result};
pub use postgres::{PgPool, PgPoolBuilder, PooledClient};

/// Драйвер, поверх которого пул открывает соединения.
pub use tokio_postgres;
