use sqlx::mssql::MssqlConnectOptions;
use sqlx::{ConnectOptions, MssqlPool};
use std::time::Duration;
use tokio::time::sleep;

#[tokio::main]
async fn main() {
    dotenv::dotenv().unwrap();
    let password = std::env::var("SQL_SERVER_PASS").unwrap();

    let pool = create_database(&password).await;
    setup_database(&pool).await;

    let mut counter = 0;
    if let Some(value) = get_value(&pool, "a").await {
        counter = value;
    } else {
        println!("Inserting id 'a' with value '{counter}'");
        insert_value(&pool, "a", counter).await;
        counter += 1;
        sleep(Duration::from_secs(1)).await;
    }

    loop {
        println!("Updating id 'a' with value '{counter}'");
        update_value(&pool, "a", counter).await;
        counter += 1;
        sleep(Duration::from_secs(1)).await;
    }
}

async fn create_database(password: &str) -> MssqlPool {
    let mut conn = MssqlConnectOptions::new()
        .username("sa")
        .password(&password)
        .connect()
        .await
        .unwrap();

    let database_result: Option<(String,)> =
        sqlx::query_as("SELECT [name] FROM sys.databases WHERE [name] = 'tester'")
            .fetch_optional(&mut conn)
            .await
            .unwrap();
    if database_result.is_none() {
        println!("Creating database");
        sqlx::query("CREATE DATABASE tester")
            .execute(&mut conn)
            .await
            .unwrap();

        println!("Enabling change tracking");
        sqlx::query(
            "ALTER DATABASE tester \
SET CHANGE_TRACKING = ON \
(CHANGE_RETENTION = 2 DAYS, AUTO_CLEANUP = ON)",
        )
        .execute(&mut conn)
        .await
        .unwrap();
    }

    let pool = MssqlPool::connect_with(
        MssqlConnectOptions::new()
            .username("sa")
            .password(&password)
            .database("tester"),
    )
    .await
    .unwrap();

    pool
}

async fn setup_database(connection: &MssqlPool) {
    sqlx::query(
        r#"
    IF NOT EXISTS (
        SELECT * FROM sys.tables t
        JOIN sys.schemas s ON (t.schema_id = s.schema_id)
        WHERE s.name = 'dbo' AND t.name = 'pairs')
    CREATE TABLE pairs (id NVARCHAR(256) PRIMARY KEY, value INT)"#,
    )
    .execute(connection)
    .await
    .unwrap();
}

async fn get_value(connection: &MssqlPool, id: &str) -> Option<i32> {
    let (result,): (i32,) = sqlx::query_as("SELECT value FROM pairs WHERE id = @p1")
        .bind(id)
        .fetch_optional(connection)
        .await
        .unwrap()?;
    Some(result)
}

async fn insert_value(connection: &MssqlPool, id: &str, value: i32) {
    sqlx::query("INSERT INTO pairs (id, [value]) values (@p1, @p2)")
        .bind(id)
        .bind(&value)
        .execute(connection)
        .await
        .unwrap();
}

async fn update_value(connection: &MssqlPool, id: &str, value: i32) {
    sqlx::query("UPDATE pairs SET [value] = @p1 WHERE id = @p2")
        .bind(&value)
        .bind(id)
        .execute(connection)
        .await
        .unwrap();
}
