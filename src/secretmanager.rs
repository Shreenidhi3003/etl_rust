use anyhow::{anyhow, Result};
use aws_sdk_secretsmanager::Client as SecretClient;
use serde::Deserialize;

use crate::config;

#[derive(Debug, Deserialize)]
pub struct DbCredentials {
    pub username: String,
    pub password: String,
}

pub async fn get_db_credentials() -> Result<DbCredentials> {

    let secret_client : SecretClient = crate::aws::make_secret_client().await;

    let response = secret_client
        .get_secret_value()
        .secret_id(config::SECRET_NAME)
        .send()
        .await?;

    let secret_string = response
        .secret_string()
        .ok_or_else(|| anyhow!("SecretString is missing in AWS Secrets Manager"))?;

    let credentials: DbCredentials =
        serde_json::from_str(secret_string)?;

    if credentials.username.trim().is_empty() {
        return Err(anyhow!(
            "PostgreSQL username is missing in AWS Secrets Manager"
        ));
    }

    if credentials.password.trim().is_empty() {
        return Err(anyhow!(
            "PostgreSQL password is missing in AWS Secrets Manager"
        ));
    }

    println!("PostgreSQL credentials retrieved successfully");

    Ok(credentials)
}