use awscreds::Credentials;
use awsregion::Region;
use s3::error::S3Error;
use s3::Bucket;

pub async fn _save_chunk(
    bucket_name: &str,
    path: &str,
    chunk: &[u8],
    credentials: Credentials,
) -> Result<(), S3Error> {
    let region = Region::Custom {
        region: "eu-central-1".to_owned(),
        endpoint: "http://localhost:9000".to_owned(),
    };
    let bucket = Bucket::new(bucket_name, region, credentials)?.with_path_style();
    bucket.put_object(path, chunk).await?;
    Ok(())
}

pub async fn _list_objects() -> Result<(), S3Error> {
    // This requires a running minio server at localhost:9000

    let bucket_name = "test-rust-s3";
    let region = Region::Custom {
        region: "eu-central-1".to_owned(),
        endpoint: "http://localhost:9000".to_owned(),
    };
    let credentials =
        Credentials::new(Some("admin"), Some("password"), None, None, Some("example"))?;

    let bucket = Bucket::new(bucket_name, region.clone(), credentials.clone())?.with_path_style();

    let table1_objects = bucket
        .list("table_copies/table1/".to_string(), Some("/".to_string()))
        .await?;

    println!("{table1_objects:?}");

    // let s3_path = "table_copies/table1/test.file";
    // let test = b"I'm going to S3!";

    // let response_data = bucket.put_object(s3_path, test).await?;
    // assert_eq!(response_data.status_code(), 200);

    // let response_data = bucket.get_object(s3_path).await?;
    // assert_eq!(response_data.status_code(), 200);
    // assert_eq!(test, response_data.as_slice());

    // let response_data = bucket
    //     .get_object_range(s3_path, 0, Some(1000))
    //     .await
    //     .unwrap();
    // assert_eq!(response_data.status_code(), 206);
    // let (head_object_result, code) = bucket.head_object(s3_path).await?;
    // assert_eq!(code, 200);
    // assert_eq!(
    //     head_object_result.content_type.unwrap_or_default(),
    //     "application/octet-stream".to_owned()
    // );

    // let response_data = bucket.delete_object(s3_path).await?;
    // assert_eq!(response_data.status_code(), 204);
    Ok(())
}

pub async fn _create_object() -> Result<(), S3Error> {
    let bucket_name = "test-rust-s3";
    let region = Region::Custom {
        region: "eu-central-1".to_owned(),
        endpoint: "http://localhost:9000".to_owned(),
    };
    let credentials =
        Credentials::new(Some("admin"), Some("password"), None, None, Some("example"))?;

    let bucket = Bucket::new(bucket_name, region.clone(), credentials.clone())?.with_path_style();
    // if !bucket.exists().await? {
    // let bucket = Bucket::create_with_path_style(
    //     bucket_name,
    //     region,
    //     credentials,
    //     BucketConfiguration::default(),
    // )
    // .await?
    // .bucket;
    // }

    let s3_path = "table_copies/table2/test.file";
    let test = b"I'm going to S3!";

    let response_data = bucket.put_object(s3_path, test).await?;
    assert_eq!(response_data.status_code(), 200);

    let response_data = bucket.get_object(s3_path).await?;
    assert_eq!(response_data.status_code(), 200);
    assert_eq!(test, response_data.as_slice());

    let response_data = bucket
        .get_object_range(s3_path, 0, Some(1000))
        .await
        .unwrap();
    assert_eq!(response_data.status_code(), 206);
    let (head_object_result, code) = bucket.head_object(s3_path).await?;
    assert_eq!(code, 200);
    assert_eq!(
        head_object_result.content_type.unwrap_or_default(),
        "application/octet-stream".to_owned()
    );
    Ok(())
}
