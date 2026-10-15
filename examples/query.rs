use std::time::Duration;

use pg_pool::PgPool;

#[tokio::main]
async fn main() -> pg_pool::Result<()> {
    let url = std::env::var("DATABASE_URL").expect("задайте DATABASE_URL");
    let pool = PgPool::builder(url)?
        .max_size(8)
        .min_size(1)
        .acquire_timeout(Duration::from_secs(5))
        .build()
        .await?;

    let conn = pool.get().await?;
    let row = conn.query_one("SELECT $1::int4", &[&1_i32]).await?;
    let value: i32 = row.get(0);
    println!("{value}");

    let status = pool.status();
    println!(
        "size {} idle {} in_use {}",
        status.size,
        status.idle,
        status.in_use()
    );
    Ok(())
}
