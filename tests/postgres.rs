use pg_pool::PgPool;

#[tokio::test]
async fn checkout_roundtrip_against_server() {
    let Some(url) = std::env::var("DATABASE_URL").ok() else {
        eprintln!("DATABASE_URL не задан, проверка с сервером пропущена");
        return;
    };

    let pool = PgPool::builder(url)
        .expect("строка подключения")
        .max_size(2)
        .min_size(1)
        .test_on_checkout(true)
        .build()
        .await
        .expect("сервер PostgreSQL");

    assert_eq!(pool.status().size, 1);

    let conn = pool.get().await.expect("выдача");
    let row = conn
        .query_one("SELECT $1::int4", &[&7_i32])
        .await
        .expect("запрос");
    let value: i32 = row.get(0);
    assert_eq!(value, 7);
    drop(conn);

    let again = pool.get().await.expect("повторная выдача");
    again.simple_query("SELECT 1").await.expect("ping");
    drop(again);

    pool.close();
    assert!(pool.is_closed());
    assert!(pool.get().await.is_err());
}
