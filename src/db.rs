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
    let credentials = crate::secretmanager::get_db_credentials().await?;

    let username = &credentials.username;
    let password = &credentials.password;

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