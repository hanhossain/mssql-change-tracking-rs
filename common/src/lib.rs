use sqlx::mssql::MssqlConnectOptions;
use sqlx::{ConnectOptions, MssqlPool};

pub struct Tester {
    pub pool: MssqlPool,
}

impl Tester {
    pub async fn create_from_env() -> Self {
        dotenv::dotenv().unwrap();
        let password = std::env::var("SQL_SERVER_PASS").unwrap();
        Tester::create(&password).await.setup().await
    }

    pub async fn get_value(&self, id: &str) -> Option<i32> {
        let (result,): (i32,) = sqlx::query_as("SELECT value FROM pairs WHERE id = @p1")
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .unwrap()?;
        Some(result)
    }

    pub async fn insert_value(&self, id: &str, value: i32) {
        sqlx::query("INSERT INTO pairs (id, [value]) values (@p1, @p2)")
            .bind(id)
            .bind(&value)
            .execute(&self.pool)
            .await
            .unwrap();
    }

    pub async fn update_value(&self, id: &str, value: i32) {
        sqlx::query("UPDATE pairs SET [value] = @p1 WHERE id = @p2")
            .bind(&value)
            .bind(id)
            .execute(&self.pool)
            .await
            .unwrap();
    }

    pub async fn get_tracking_version(&self) -> i64 {
        let (version,): (i64,) = sqlx::query_as("SELECT CHANGE_TRACKING_CURRENT_VERSION()")
            .fetch_one(&self.pool)
            .await
            .unwrap();
        version
    }

    async fn create(password: &str) -> Self {
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

        Tester { pool }
    }

    async fn setup(self) -> Self {
        sqlx::query(
            r#"
    IF NOT EXISTS (
        SELECT * FROM sys.tables t
        JOIN sys.schemas s ON (t.schema_id = s.schema_id)
        WHERE s.name = 'dbo' AND t.name = 'pairs')
    CREATE TABLE pairs (id NVARCHAR(256) PRIMARY KEY, value INT)"#,
        )
        .execute(&self.pool)
        .await
        .unwrap();

        self
    }
}
