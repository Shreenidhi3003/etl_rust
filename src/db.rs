use anyhow::Result;
use sqlx::postgres::{PgConnectOptions,PgPoolOptions};
use std::env;
use sqlx::PgPool;




pub async fn create_db_connection() -> Result<PgPool> {

    dotenvy::dotenv().ok();
    let host = env::var("PG_HOST")?;
    let port = env::var("PG_PORT")?
        .parse::<u16>()?;
    let database = env::var("PG_DATABASE")?;
    let username = env::var("PG_USERNAME")?;
    let password = env::var("PG_PASSWORD")?;

    let options = PgConnectOptions::new()
                  .host(&host)
                  .port(port)
                  .database(&database)
                  .username(&username)
                  .password(&password);

    let pool= PgPoolOptions::new()
                                      .max_connections(5)
                                      .connect_with(options)
                                      .await?;

    Ok(pool)

    }