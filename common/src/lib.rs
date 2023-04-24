use sqlx::mssql::MssqlConnectOptions;
use sqlx::{ConnectOptions, FromRow, MssqlPool};

pub struct Tester {
    pub pool: MssqlPool,
}

impl Tester {
    pub async fn create_from_env() -> Self {
        dotenv::dotenv().unwrap();
        let password = std::env::var("SQL_SERVER_PASS").unwrap();
        Tester::create(&password).await
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

    pub async fn get_last_tracked_version(&self) -> Option<i64> {
        let (version,): (i64,) = sqlx::query_as("SELECT version FROM tracking WHERE id = 'pairs'")
            .fetch_optional(&self.pool)
            .await
            .unwrap()?;
        Some(version)
    }

    pub async fn set_last_tracked_version(&self, version: i64, was_previously_tracked: bool) {
        if was_previously_tracked {
            println!("Updating last tracked version to {version}");
            sqlx::query("UPDATE tracking SET version = @p1 WHERE id = 'pairs'")
                .bind(&version)
                .execute(&self.pool)
                .await
                .unwrap();
        } else {
            println!("Inserting last tracked version to {version}");
            sqlx::query("INSERT INTO tracking (id, version) VALUES ('pairs', @p1)")
                .bind(&version)
                .execute(&self.pool)
                .await
                .unwrap();
        }
    }

    pub async fn get_changes(&self, last_tracked_version: Option<i64>) -> Vec<Change> {
        let last_version = last_tracked_version.unwrap_or(0);
        sqlx::query_as(
            r#"
        SELECT
            id,
            SYS_CHANGE_VERSION AS version,
            SYS_CHANGE_OPERATION AS operation
        FROM CHANGETABLE(CHANGES pairs, @p1)
        AS CT"#,
        )
        .bind(&last_version)
        .fetch_all(&self.pool)
        .await
        .unwrap()
    }

    async fn create(password: &str) -> Self {
        let mut conn = MssqlConnectOptions::new()
            .username("sa")
            .password(&password)
            .connect()
            .await
            .unwrap();

        let database_exists = sqlx::query_as::<_, (String,)>(
            "SELECT [name] FROM sys.databases WHERE [name] = 'tester'",
        )
        .fetch_optional(&mut conn)
        .await
        .unwrap()
        .is_some();

        if !database_exists {
            println!("Creating database");
            sqlx::query("CREATE DATABASE tester")
                .execute(&mut conn)
                .await
                .unwrap();

            println!("Enabling change tracking for database");
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

        if !database_exists {
            println!("Creating table 'pairs'");
            sqlx::query("CREATE TABLE pairs (id NVARCHAR(256) PRIMARY KEY, value INT)")
                .execute(&pool)
                .await
                .unwrap();

            println!("Enabling change tracking for table 'pairs'");
            sqlx::query("ALTER TABLE pairs ENABLE CHANGE_TRACKING")
                .execute(&pool)
                .await
                .unwrap();

            println!("Creating table 'tracking'");
            sqlx::query("CREATE TABLE tracking (id NVARCHAR(256) PRIMARY KEY, version BIGINT)")
                .execute(&pool)
                .await
                .unwrap();
        }

        Tester { pool }
    }
}

#[derive(Debug, FromRow)]
pub struct Change {
    pub id: String,
    pub version: i64,
    pub operation: String,
}
