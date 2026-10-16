use crate::{Error, Result, error::outcome};
use cordis_wire::{Bytes, CONTENT_TYPE, Incoming, MAX_MESSAGE, Payload, Request};
use std::time::Duration;

/// The HTTP body is the same bounded protobuf frame used by IPC.
#[derive(Clone)]
pub(crate) struct Http {
    validation: cordis_contract::Validation,
    client: reqwest::Client,
    url: String,
}
impl Http {
    pub fn local(
        url: &str,
        deadline: Duration,
        validation: cordis_contract::Validation,
    ) -> Result<Self> {
        let parsed = reqwest::Url::parse(url).map_err(|e| Error::Protocol(e.to_string()))?;
        if parsed.scheme() != "http"
            || parsed.host_str() != Some("127.0.0.1")
            || parsed.path() != "/cordis/call"
            || !parsed.username().is_empty()
            || parsed.password().is_some()
            || parsed.query().is_some()
            || parsed.fragment().is_some()
        {
            return Err(Error::Protocol(
                "process endpoint must be a loopback HTTP call URL".into(),
            ));
        }
        let client = reqwest::Client::builder()
            .no_proxy()
            .retry(reqwest::retry::never())
            .redirect(reqwest::redirect::Policy::none())
            .timeout(deadline)
            .build()
            .map_err(|e| Error::Io(e.to_string()))?;
        Ok(Self {
            validation,
            client,
            url: url.into(),
        })
    }
    pub async fn call(&self, request: Request) -> Result<Payload> {
        let bytes = request
            .encode()
            .map_err(|e| Error::InvalidInput(e.to_string()))?;
        let mut response = self
            .client
            .post(&self.url)
            .header("Content-Type", CONTENT_TYPE)
            .header("Accept", CONTENT_TYPE)
            .body(bytes)
            .send()
            .await
            .map_err(|e| Error::OutcomeUnknown(e.to_string()))?;
        let status = response.status();
        if response
            .headers()
            .get("content-type")
            .and_then(|h| h.to_str().ok())
            != Some(CONTENT_TYPE)
        {
            return Err(Error::Protocol(
                "HTTP response content-type mismatch".into(),
            ));
        }
        if response
            .content_length()
            .is_some_and(|n| n > MAX_MESSAGE as u64)
        {
            return Err(Error::Protocol("HTTP response too large".into()));
        }
        // Keep a single transport chunk directly. Coalesce only if the HTTP
        // decoder actually split the body, reserving a bounded advertised size.
        let capacity = response.content_length().unwrap_or(0) as usize;
        let mut first = None;
        let mut joined = None::<Vec<u8>>;
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|e| Error::OutcomeUnknown(e.to_string()))?
        {
            let length = joined
                .as_ref()
                .map_or_else(|| first.as_ref().map_or(0, Bytes::len), Vec::len);
            if length + chunk.len() > MAX_MESSAGE {
                return Err(Error::Protocol("HTTP response too large".into()));
            }
            if let Some(body) = joined.as_mut() {
                body.extend_from_slice(&chunk);
            } else if let Some(previous) = first.take() {
                let mut body = Vec::with_capacity(capacity.max(previous.len() + chunk.len()));
                body.extend_from_slice(&previous);
                body.extend_from_slice(&chunk);
                joined = Some(body);
            } else {
                first = Some(chunk);
            }
        }
        let body = joined.map(Bytes::from).or(first).unwrap_or_default();
        let Incoming::Response(response) = cordis_wire::decode_with(body, self.validation)? else {
            return Err(Error::Protocol("HTTP peer returned a request".into()));
        };
        request.check(&response)?;
        let result = outcome(response.outcome);
        if status.is_success() != result.is_ok() {
            return Err(Error::Protocol("HTTP status disagrees with outcome".into()));
        }
        result
    }
}
