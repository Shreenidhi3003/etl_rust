use aws_config::BehaviorVersion;
use aws_sdk_s3::{Client, primitives::ByteStream};
use anyhow::Result;

pub async fn make_s3_client() -> Client {
    let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
    Client::new(&config)
}

pub async fn list_of_xml_from_s3(
    client: &Client,
    bucket: &str,
    prefix: &str,
) -> Result<Vec<String>> {

    let list_of_objects = client
        .list_objects_v2()
        .bucket(bucket)
        .prefix(prefix)
        .send()
        .await?;

    // YOUR SDK VERSION: contents() -> &[Object]
    let objects = list_of_objects.contents();

    let keys_in_vec: Vec<String> = objects
        .iter()
        .filter_map(|obj| obj.key().map(|s| s.to_string()))
        .filter(|k| k.to_lowercase().ends_with(".xml"))
        .collect();

    Ok(keys_in_vec)
}


pub async fn get_object_body(client: &Client, key: &str, bucket: &str) -> Result<ByteStream> {
    let resp = client.get_object().bucket(bucket).key(key).send().await?;
    Ok(resp.body)
}

pub async fn upload_s3_bytes(client: &Client, key: &str, bucket: &str, data: Vec<u8>) -> Result<()> {
    client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from(data))
        .send()
        .await?;

    Ok(())
}
