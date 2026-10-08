use crate::config::{BODY_BUDGET_ERROR, MAX_BODY_SIZE, MAX_RETRY_DELAY, RETRY_DRAIN_LIMIT};
use crate::worker::BodyBudget;
use bytes::Bytes;
use futures_util::StreamExt;
use reqwest::header::HeaderMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;

pub struct RequestPlan {
    pub method: reqwest::Method,
    pub url: reqwest::Url,
    pub headers: HeaderMap,
    pub body: Option<Bytes>,
    pub deadline: Instant,
    pub collect_body: bool,
    pub body_budget: Arc<Semaphore>,
}

pub enum AttemptOutcome {
    Complete {
        status: u16,
        headers: HeaderMap,
        body: Option<Bytes>,
        budget: BodyBudget,
    },
    Cancelled,
    Transient {
        message: String,
        retry_after: Option<HeaderMap>,
    },
    Fatal(String),
}

enum ReadOutcome {
    Ok(Bytes, BodyBudget),
    TooLarge,
    BudgetExhausted,
    StreamError(String),
    DecodeError(String),
    Cancelled,
}

pub fn retryable_status(status: reqwest::StatusCode) -> bool {
    matches!(status.as_u16(), 408 | 429 | 500 | 502 | 503 | 504)
}

pub fn retry_delay(headers: Option<&HeaderMap>, attempt: usize, base: Duration) -> Duration {
    if let Some(delay) = headers.and_then(parse_retry_after) {
        delay
    } else {
        let multiplier = 1_u32.checked_shl(attempt.min(6) as u32).unwrap_or(u32::MAX);
        base.checked_mul(multiplier)
            .unwrap_or(MAX_RETRY_DELAY)
            .min(MAX_RETRY_DELAY)
    }
}

fn parse_retry_after(headers: &HeaderMap) -> Option<Duration> {
    let value = headers
        .get(reqwest::header::RETRY_AFTER)?
        .to_str()
        .ok()?
        .trim();

    let delay = match value.parse::<u64>() {
        Ok(seconds) => Duration::from_secs(seconds),
        Err(_) => {
            let target = httpdate::parse_http_date(value).ok()?;
            let delta = target.duration_since(SystemTime::now()).ok()?;
            if delta.is_zero() {
                return None;
            }
            delta
        }
    };

    Some(delay.min(MAX_RETRY_DELAY))
}

pub async fn run_attempt(
    client: &reqwest::Client,
    plan: &RequestPlan,
    token: &CancellationToken,
    permit: OwnedSemaphorePermit,
) -> AttemptOutcome {
    let remaining = plan.deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        drop(permit);
        return AttemptOutcome::Fatal("Request timed out".to_string());
    }

    let mut request = client
        .request(plan.method.clone(), plan.url.clone())
        .timeout(remaining)
        .headers(plan.headers.clone());
    if let Some(body) = plan.body.clone() {
        request = request.body(body);
    }

    let response = tokio::select! {
        biased;
        _ = token.cancelled() => {
            drop(permit);
            return AttemptOutcome::Cancelled;
        }
        response = request.send() => response,
    };

    let response = match response {
        Ok(response) => response,
        Err(error) => {
            drop(permit);
            let transient =
                error.is_connect() || error.is_timeout() || error.is_body() || error.is_request();
            let message = if transient {
                format!("Request error: {}", error)
            } else {
                error.to_string()
            };
            return if transient {
                AttemptOutcome::Transient {
                    message,
                    retry_after: None,
                }
            } else {
                AttemptOutcome::Fatal(message)
            };
        }
    };

    let status = response.status();
    if retryable_status(status) {
        drop(permit);
        let retry_after = response.headers().clone();
        drain_prefix(
            response,
            token,
            plan.deadline.saturating_duration_since(Instant::now()),
        )
        .await;
        return AttemptOutcome::Transient {
            message: format!("HTTP {}", status.as_u16()),
            retry_after: Some(retry_after),
        };
    }

    let headers = response.headers().clone();
    if !plan.collect_body {
        if response
            .content_length()
            .is_some_and(|length| length > MAX_BODY_SIZE as u64)
        {
            drop(permit);
            return AttemptOutcome::Fatal("Response body exceeded memory limit".to_string());
        }
        drop(permit);
        drop(response);
        return AttemptOutcome::Complete {
            status: status.as_u16(),
            headers,
            body: None,
            budget: BodyBudget::new(),
        };
    }

    let (body, budget) = match read_body(response, token, plan).await {
        ReadOutcome::Ok(body, budget) => (Some(body), budget),
        ReadOutcome::TooLarge => {
            drop(permit);
            return AttemptOutcome::Fatal("Response body exceeded memory limit".to_string());
        }
        ReadOutcome::BudgetExhausted => {
            drop(permit);
            return AttemptOutcome::Fatal(BODY_BUDGET_ERROR.to_string());
        }
        ReadOutcome::StreamError(reason) => {
            drop(permit);
            return AttemptOutcome::Transient {
                message: format!("Stream error: {}", reason),
                retry_after: None,
            };
        }
        ReadOutcome::DecodeError(reason) => {
            drop(permit);
            return AttemptOutcome::Fatal(format!("Response decode error: {}", reason));
        }
        ReadOutcome::Cancelled => {
            drop(permit);
            return AttemptOutcome::Cancelled;
        }
    };

    drop(permit);
    AttemptOutcome::Complete {
        status: status.as_u16(),
        headers,
        body,
        budget,
    }
}

async fn read_body(
    response: reqwest::Response,
    token: &CancellationToken,
    plan: &RequestPlan,
) -> ReadOutcome {
    let mut budget = BodyBudget::new();
    let mut buffer = Vec::new();
    let mut stream = response.bytes_stream();
    let mut size = 0usize;

    loop {
        let chunk = tokio::select! {
            biased;
            _ = token.cancelled() => return ReadOutcome::Cancelled,
            chunk = stream.next() => match chunk {
                Some(Ok(chunk)) => chunk,
                Some(Err(error)) => {
                    if error.is_decode() {
                        return ReadOutcome::DecodeError(error.to_string());
                    }
                    return ReadOutcome::StreamError(error.to_string());
                }
                None => break,
            },
        };

        size += chunk.len();
        if size > MAX_BODY_SIZE {
            return ReadOutcome::TooLarge;
        }

        if buffer.capacity() - buffer.len() < chunk.len() {
            let missing = chunk.len() - (buffer.capacity() - buffer.len());
            let pre = match plan
                .body_budget
                .clone()
                .try_acquire_many_owned(missing as u32)
            {
                Ok(pre) => pre,
                Err(_) => return ReadOutcome::BudgetExhausted,
            };
            let before = buffer.capacity();
            if buffer.try_reserve(chunk.len()).is_err() {
                drop(pre);
                return ReadOutcome::TooLarge;
            }
            let growth = buffer.capacity() - before;
            debug_assert!(growth >= missing);
            budget.reserve(pre, missing);
            let extra = growth.saturating_sub(missing);
            if extra > 0 {
                match plan
                    .body_budget
                    .clone()
                    .try_acquire_many_owned(extra as u32)
                {
                    Ok(top_up) => budget.reserve(top_up, extra),
                    Err(_) => return ReadOutcome::BudgetExhausted,
                }
            }
        }

        buffer.extend_from_slice(&chunk);
    }

    buffer.shrink_to_fit();
    ReadOutcome::Ok(Bytes::from(buffer), budget)
}

async fn drain_prefix(response: reqwest::Response, token: &CancellationToken, budget: Duration) {
    let budget = budget.min(Duration::from_secs(2));
    if budget.is_zero() {
        return;
    }
    let drain = async {
        let mut stream = response.bytes_stream();
        let mut seen = 0usize;
        loop {
            let chunk = tokio::select! {
                biased;
                _ = token.cancelled() => break,
                chunk = stream.next() => chunk,
            };
            match chunk {
                Some(Ok(chunk)) => {
                    seen += chunk.len();
                    if seen >= RETRY_DRAIN_LIMIT {
                        break;
                    }
                }
                _ => break,
            }
        }
    };
    let _ = tokio::time::timeout(budget, drain).await;
}

#[cfg(test)]
mod tests {
    use super::{parse_retry_after, retry_delay, retryable_status};
    use crate::config::{DEFAULT_RETRY_DELAY, MAX_RETRY_DELAY};
    use reqwest::header::{HeaderMap, HeaderValue};
    use std::time::Duration;

    #[test]
    fn retries_transient_statuses() {
        assert!(retryable_status(reqwest::StatusCode::TOO_MANY_REQUESTS));
        assert!(retryable_status(reqwest::StatusCode::SERVICE_UNAVAILABLE));
        assert!(!retryable_status(reqwest::StatusCode::BAD_REQUEST));
    }

    #[test]
    fn numeric_retry_after_wins() {
        let mut headers = HeaderMap::new();
        headers.insert("retry-after", HeaderValue::from_static("7"));
        assert_eq!(
            retry_delay(Some(&headers), 0, Duration::from_secs(30)),
            Duration::from_secs(7)
        );
    }

    #[test]
    fn past_http_date_retry_after_falls_back_to_backoff() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "retry-after",
            HeaderValue::from_static("Wed, 21 Oct 2015 07:28:00 GMT"),
        );
        assert!(parse_retry_after(&headers).is_none());
        assert_eq!(
            retry_delay(Some(&headers), 0, Duration::from_secs(2)),
            Duration::from_secs(2)
        );
    }

    #[test]
    fn future_http_date_retry_after_is_honoured() {
        let mut headers = HeaderMap::new();
        let future = httpdate::fmt_http_date(
            std::time::SystemTime::now() + std::time::Duration::from_secs(12),
        );
        headers.insert("retry-after", HeaderValue::from_str(&future).unwrap());
        let delay = parse_retry_after(&headers).expect("future date is usable");
        assert!(delay >= Duration::from_secs(10) && delay <= Duration::from_secs(13));
    }

    #[test]
    fn retry_after_is_clamped() {
        let mut headers = HeaderMap::new();
        headers.insert("retry-after", HeaderValue::from_static("100000"));
        assert_eq!(
            retry_delay(Some(&headers), 0, DEFAULT_RETRY_DELAY),
            MAX_RETRY_DELAY
        );
    }

    #[test]
    fn backoff_is_exponential_and_capped() {
        assert_eq!(
            retry_delay(None, 0, Duration::from_secs(1)),
            Duration::from_secs(1)
        );
        assert_eq!(
            retry_delay(None, 1, Duration::from_secs(1)),
            Duration::from_secs(2)
        );
        assert_eq!(
            retry_delay(None, 2, Duration::from_secs(1)),
            Duration::from_secs(4)
        );
        assert_eq!(
            retry_delay(None, 9, Duration::from_secs(1)),
            MAX_RETRY_DELAY
        );
    }
}
