//! Client configuration.

use std::time::Duration;

use tonic::transport::Endpoint;

use crate::error::{Error, Result};

/// Settings for a [`Client`](crate::Client).
#[derive(Clone, Debug)]
pub struct ClientConfig {
    /// Addresses of the nodes to talk to, as `host:port` or
    /// `http://host:port`. List every node you know. The client discovers
    /// the leader from there.
    pub endpoints: Vec<String>,
    /// How long to wait for a TCP connection.
    pub connect_timeout: Duration,
    /// How long to wait for the reply to one call. For a subscription this
    /// covers opening the stream, not the stream's lifetime.
    pub request_timeout: Duration,
    /// How many times a failed call is retried. The call is tried at most
    /// this many times plus one.
    pub max_retries: u32,
    /// Wait before the first retry. Each later retry waits twice as long.
    pub backoff_base: Duration,
    /// Longest wait between retries.
    pub backoff_max: Duration,
    /// Retry produce calls whose outcome is unknown. This gives
    /// at-least-once delivery and can write a record twice. Off by default.
    pub retry_writes: bool,
}

impl ClientConfig {
    /// A configuration with the given endpoints and default timeouts.
    pub fn new<I, S>(endpoints: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        ClientConfig {
            endpoints: endpoints.into_iter().map(Into::into).collect(),
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(10),
            max_retries: 5,
            backoff_base: Duration::from_millis(100),
            backoff_max: Duration::from_secs(2),
            retry_writes: false,
        }
    }

    /// The wait before retry number `retry`, counting from one.
    pub(crate) fn backoff(&self, retry: u32) -> Duration {
        let doublings = retry.saturating_sub(1).min(20);
        self.backoff_base
            .saturating_mul(1 << doublings)
            .min(self.backoff_max)
    }
}

/// Turns an address into the URL form tonic expects and checks it.
///
/// `host:port` becomes `http://host:port`. A value that already has a
/// scheme is kept as it is.
pub(crate) fn normalize(endpoint: &str) -> Result<String> {
    let invalid = |reason: &str| Error::InvalidEndpoint {
        endpoint: endpoint.to_owned(),
        reason: reason.to_owned(),
    };
    let trimmed = endpoint.trim();
    let (scheme, rest) = trimmed.split_once("://").unwrap_or(("http", trimmed));
    let rest = rest.trim_end_matches('/');
    if rest.is_empty() {
        return Err(invalid("it has no host"));
    }
    let url = format!("{scheme}://{rest}");
    let parsed = Endpoint::from_shared(url.clone()).map_err(|e| invalid(&e.to_string()))?;
    if parsed.uri().host().is_none() {
        return Err(invalid("it has no host"));
    }
    Ok(url)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bare_addresses_get_a_scheme() {
        assert_eq!(
            normalize("127.0.0.1:7001").unwrap(),
            "http://127.0.0.1:7001"
        );
        assert_eq!(normalize(" node1:7001 ").unwrap(), "http://node1:7001");
        assert_eq!(
            normalize("http://node1.internal:7001/").unwrap(),
            "http://node1.internal:7001"
        );
        assert_eq!(
            normalize("https://node1:7001").unwrap(),
            "https://node1:7001"
        );
        assert_eq!(normalize("[::1]:7001").unwrap(), "http://[::1]:7001");
    }

    #[test]
    fn bad_addresses_are_rejected() {
        for endpoint in ["", "   ", "has space:1", "http://"] {
            assert!(
                matches!(normalize(endpoint), Err(Error::InvalidEndpoint { .. })),
                "{endpoint:?}"
            );
        }
    }

    #[test]
    fn backoff_doubles_up_to_the_cap() {
        let config = ClientConfig::new(["a:1"]);
        let millis: Vec<u128> = (1..=7).map(|n| config.backoff(n).as_millis()).collect();
        assert_eq!(millis, [100, 200, 400, 800, 1600, 2000, 2000]);
        // Large retry counts must not overflow.
        assert_eq!(config.backoff(u32::MAX), Duration::from_secs(2));
        assert_eq!(config.backoff(0), Duration::from_millis(100));
    }
}
