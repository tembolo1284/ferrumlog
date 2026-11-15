//! The S3 backend. Compiled only with the `s3` feature.

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;

use crate::error::{Error, Result};

/// A [`Store`](crate::Store) in an S3 bucket.
pub struct S3Store {
    client: Client,
    bucket: String,
    /// Empty, or ends with a slash.
    prefix: String,
}

impl S3Store {
    /// Opens `bucket/prefix`. Credentials and region come from the
    /// environment, the shared config files, or the instance role, the
    /// same way the AWS CLI finds them.
    pub(crate) async fn open(location: &str) -> Result<Self> {
        let (bucket, prefix) = location.split_once('/').unwrap_or((location, ""));
        if bucket.is_empty() {
            return Err(Error::Unsupported(
                "an s3 location needs a bucket: s3://bucket/prefix".into(),
            ));
        }
        let prefix = prefix.trim_matches('/');
        let prefix = if prefix.is_empty() {
            String::new()
        } else {
            format!("{prefix}/")
        };
        let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
        Ok(S3Store {
            client: Client::new(&config),
            bucket: bucket.to_owned(),
            prefix,
        })
    }

    pub(crate) fn describe(&self) -> String {
        format!("s3://{}/{}", self.bucket, self.prefix)
    }

    fn key(&self, key: &str) -> String {
        format!("{}{key}", self.prefix)
    }

    /// One PutObject. S3 makes the new object visible all at once, so a
    /// reader sees the old contents or the new, never part of each.
    pub(crate) async fn put(&self, key: &str, bytes: Vec<u8>) -> Result<()> {
        let key = self.key(key);
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(&key)
            .body(ByteStream::from(bytes))
            .send()
            .await
            .map_err(|e| failed("put", &key, &e))?;
        Ok(())
    }

    pub(crate) async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let key = self.key(key);
        let request = self.client.get_object().bucket(&self.bucket).key(&key);
        let object = match request.send().await {
            Ok(object) => object,
            Err(e) => {
                let missing = e
                    .as_service_error()
                    .is_some_and(|service| service.is_no_such_key());
                if missing {
                    return Ok(None);
                }
                return Err(failed("get", &key, &e));
            }
        };
        let body = object
            .body
            .collect()
            .await
            .map_err(|e| failed("read", &key, &e))?;
        Ok(Some(body.into_bytes().to_vec()))
    }
}

fn failed(action: &str, key: &str, error: &dyn std::error::Error) -> Error {
    // The SDK's top-level message is terse. The cause underneath usually
    // says what actually went wrong.
    let mut message = error.to_string();
    let mut source = error.source();
    while let Some(cause) = source {
        message.push_str(": ");
        message.push_str(&cause.to_string());
        source = cause.source();
    }
    Error::Store(format!("{action} {key} failed: {message}"))
}
