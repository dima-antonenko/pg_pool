# pg_pool

Асинхронный пул сессий PostgreSQL для Tokio. Пул держит ограниченное число соединений `tokio-postgres` и выдаёт их задачам. Это клиентский пул внутри процесса, не прокси вроде PgBouncer.

Соединение возвращается в пул, когда уничтожается guard. Закрытая сессия и сессия старше `max_lifetime` в пул не попадают: вместо них открывается новая, если есть запас до `max_size`.

TLS нет. Строка с `sslmode=require` завершится ошибкой драйвера. Для локального сервера укажите `sslmode=disable` или не задавайте TLS.

## Сборка

Нужен Rust edition 2024.

```bash
cargo test
cargo build --examples
```

Проверка на живом сервере включается переменной `DATABASE_URL`:

```bash
DATABASE_URL=postgres://localhost/postgres cargo test --test postgres
DATABASE_URL=postgres://localhost/postgres cargo run --example query
```

## Выдача

```rust
let pool = pg_pool::PgPool::builder("postgres://localhost/postgres")?
    .max_size(8)
    .min_size(1)
    .build()
    .await?;

let conn = pool.get().await?;
let row = conn.query_one("SELECT $1::int4", &[&1_i32]).await?;
let value: i32 = row.get(0);
drop(conn);
```

`build` нужно вызывать внутри runtime Tokio: пул ставит фоновую чистку через `tokio::spawn`.

`PooledClient` разыменовывается в [`tokio_postgres::Client`](https://docs.rs/tokio-postgres). Пока guard жив, одно место в `max_size` занято. `mem::forget` это место не возвращает.

`try_get` и `get_timeout(Duration::ZERO)` не встают в очередь. Если соединений меньше `max_size`, пул может открыть новое; это ожидание режет уже `connect_timeout`.

`close` уничтожает простаивающие сессии и будит тех, кто ждал: они получают ошибку «пул закрыт». Уже выданную сессию можно доиспользовать, обратно она не примется.

## Пределы по умолчанию

| Параметр | Значение | Смысл |
| --- | --- | --- |
| `max_size` | 10 | потолок открытых сессий |
| `min_size` | 0 | сколько держать открытыми в простое |
| `acquire_timeout` | 30 с | ожидание, когда пул уже полный |
| `connect_timeout` | 10 с | ожидание нового TCP, `None` снимает предел пула |
| `idle_timeout` | 10 мин | простой в пуле, `None` отключает |
| `max_lifetime` | 30 мин | возраст с момента открытия, `None` отключает |
| `maintenance_interval` | 30 с | период чистки и добора до `min_size` |

Ноль в `Option`-полях не принимается. Чтобы предел выключить, передайте `None`. Ноль в `acquire_timeout` разрешён и означает отказ ждать очередь.

Перед выдачей сессии, которая лежала в пуле, можно гонять `SELECT 1`: `test_on_checkout(true)`. Свежее соединение так не проверяется. Без этой опции пул смотрит только `Client::is_closed`, возраст и простой.

Простаивающие сессии выдаются стопкой: последняя возвращённая уходит первой.
