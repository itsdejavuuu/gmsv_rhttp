use crate::config::{
    DEFAULT_REQUEST_TIMEOUT, DEFAULT_RETRY_DELAY, MAX_BODY_SIZE, MAX_CONTENT_TYPE_LEN, MAX_HEADERS,
    MAX_HEADER_NAME_LEN, MAX_HEADER_VALUE_LEN, MAX_METHOD_LEN, MAX_PARAMETERS,
    MAX_PARAMETER_KEY_LEN, MAX_PARAMETER_VALUE_LEN, MAX_REQUEST_TIMEOUT, MAX_RETRIES,
    MAX_RETRY_DELAY, MAX_URL_LENGTH, MIN_REQUEST_TIMEOUT, MIN_RETRY_DELAY,
};
use bytes::Bytes;
use gmod::lua::{
    LuaReference, State, LUA_TBOOLEAN, LUA_TNIL, LUA_TNUMBER, LUA_TSTRING, LUA_TTABLE,
};
use gmod::lua_string;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use reqwest::{Method, Url};
use std::time::Duration;

pub struct RequestOptions {
    pub method: Method,
    pub url: Url,
    pub headers: HeaderMap,
    pub body: Option<Bytes>,
    pub timeout: Duration,
    pub retries: usize,
    pub retry_base_delay: Duration,
    pub success: Option<LuaReference>,
    pub failed: Option<LuaReference>,
}

pub struct OptionsError {
    pub success: Option<LuaReference>,
    pub failed: Option<LuaReference>,
    pub message: String,
}

fn default_retries(method: &Method) -> usize {
    match *method {
        Method::GET | Method::HEAD | Method::PUT | Method::DELETE | Method::OPTIONS => 2,
        _ => 0,
    }
}

fn is_supported_scheme(url: &Url) -> bool {
    matches!(url.scheme(), "http" | "https")
}

fn is_managed_header(name: &HeaderName) -> bool {
    matches!(
        name.as_str(),
        "host"
            | "content-length"
            | "transfer-encoding"
            | "connection"
            | "proxy-connection"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "keep-alive"
            | "upgrade"
            | "trailer"
            | "te"
    )
}

fn clamp_timeout(seconds: f64) -> Duration {
    if !seconds.is_finite() {
        return DEFAULT_REQUEST_TIMEOUT;
    }
    if seconds <= MIN_REQUEST_TIMEOUT.as_secs_f64() {
        return MIN_REQUEST_TIMEOUT;
    }
    if seconds >= MAX_REQUEST_TIMEOUT.as_secs_f64() {
        return MAX_REQUEST_TIMEOUT;
    }
    Duration::from_secs_f64(seconds)
}

fn clamp_retry_delay(seconds: f64) -> Duration {
    if !seconds.is_finite() {
        return DEFAULT_RETRY_DELAY;
    }
    if seconds <= MIN_RETRY_DELAY.as_secs_f64() {
        return MIN_RETRY_DELAY;
    }
    if seconds >= MAX_RETRY_DELAY.as_secs_f64() {
        return MAX_RETRY_DELAY;
    }
    Duration::from_secs_f64(seconds)
}

unsafe fn parse_string_table(
    lua: State,
    index: i32,
    max_entries: usize,
    max_key_len: usize,
    max_value_len: usize,
    allow_bool: bool,
) -> Result<Vec<(String, String)>, String> {
    let mut pairs = Vec::new();
    let abs_index = if index < 0 {
        lua.get_top() + index + 1
    } else {
        index
    };

    let mut seen = 0usize;
    lua.push_nil();
    while lua.next(abs_index) != 0 {
        // Stack: [key, value].
        seen += 1;
        if seen > max_entries {
            lua.pop_n(2);
            return Err(format!("Too many entries (limit: {max_entries})"));
        }

        lua.push_value(-2);
        let key = if lua.lua_type(-1) == LUA_TSTRING {
            lua.get_string(-1).map(|s| s.into_owned())
        } else {
            None
        };
        lua.pop_n(1);

        lua.push_value(-1);
        let value = match lua.lua_type(-1) {
            LUA_TSTRING | LUA_TNUMBER => lua.get_string(-1).map(|s| s.into_owned()),
            LUA_TBOOLEAN if allow_bool => {
                Some(if lua.get_boolean(-1) { "true" } else { "false" }.to_string())
            }
            _ => None,
        };
        lua.pop_n(1);

        if let (Some(key), Some(value)) = (key, value) {
            if key.len() > max_key_len || value.len() > max_value_len {
                lua.pop_n(2);
                return Err("Entry exceeded size limit".to_string());
            }
            pairs.push((key, value));
        }
        lua.pop_n(1);
    }
    Ok(pairs)
}

impl RequestOptions {
    pub unsafe fn parse(
        lua: State,
        success: Option<LuaReference>,
        failed: Option<LuaReference>,
    ) -> Result<Self, OptionsError> {
        macro_rules! bail {
            ($msg:expr) => {
                return Err(OptionsError {
                    success,
                    failed,
                    message: $msg.into(),
                })
            };
        }

        lua.get_field(1, lua_string!("url"));
        let url_str = lua
            .get_string(-1)
            .map(|s| s.into_owned())
            .unwrap_or_default();
        lua.pop_n(1);

        if url_str.len() > MAX_URL_LENGTH {
            bail!(format!(
                "URL exceeded length limit ({MAX_URL_LENGTH} bytes)"
            ));
        }
        let mut url = match Url::parse(&url_str) {
            Ok(url) => url,
            Err(e) => {
                if url_str.trim().is_empty() {
                    bail!("rhttp: `url` is required and must be a non-empty string");
                } else if url_str.len() > MAX_URL_LENGTH {
                    bail!(format!(
                        "URL exceeded length limit ({MAX_URL_LENGTH} bytes)"
                    ));
                } else {
                    bail!(format!("Invalid URL: {e}"));
                }
            }
        };

        if url.as_str().len() > MAX_URL_LENGTH {
            bail!(format!(
                "URL exceeded length limit ({MAX_URL_LENGTH} bytes)"
            ));
        }
        if !url.username().is_empty() || url.password().is_some() {
            bail!("URL must not contain credentials; use the Authorization header instead");
        }
        if !is_supported_scheme(&url) {
            bail!("Only http and https URLs are supported");
        }

        lua.get_field(1, lua_string!("method"));
        let method_str = lua
            .get_string(-1)
            .map(|s| s.into_owned())
            .unwrap_or_else(|| "GET".to_string());
        lua.pop_n(1);
        if method_str.len() > MAX_METHOD_LEN {
            bail!(format!(
                "HTTP method exceeded length limit ({MAX_METHOD_LEN})"
            ));
        }
        let method = match Method::from_bytes(method_str.to_uppercase().as_bytes()) {
            Ok(method) => method,
            Err(e) => bail!(format!("Invalid HTTP method: {e}")),
        };

        lua.get_field(1, lua_string!("headers"));
        let headers = if lua.lua_type(-1) == LUA_TTABLE {
            match parse_string_table(
                lua,
                -1,
                MAX_HEADERS,
                MAX_HEADER_NAME_LEN,
                MAX_HEADER_VALUE_LEN,
                false,
            ) {
                Ok(headers) => headers,
                Err(message) => {
                    lua.pop_n(1);
                    bail!(message);
                }
            }
        } else {
            Vec::new()
        };
        lua.pop_n(1);

        let mut request_headers = HeaderMap::new();
        for (key, value) in headers {
            let name = match HeaderName::from_bytes(key.as_bytes()) {
                Ok(name) => name,
                Err(e) => bail!(format!("Invalid header name: {e}")),
            };
            if is_managed_header(&name) {
                bail!(format!(
                    "Header '{}' is managed by rhttp and cannot be set",
                    name.as_str()
                ));
            }
            match HeaderValue::from_str(&value) {
                Ok(value) => request_headers.insert(name, value),
                Err(e) => bail!(format!("Invalid header value: {e}")),
            };
        }

        lua.get_field(1, lua_string!("parameters"));
        let parameters = if lua.lua_type(-1) == LUA_TTABLE {
            match parse_string_table(
                lua,
                -1,
                MAX_PARAMETERS,
                MAX_PARAMETER_KEY_LEN,
                MAX_PARAMETER_VALUE_LEN,
                true,
            ) {
                Ok(parameters) => parameters,
                Err(message) => {
                    lua.pop_n(1);
                    bail!(message);
                }
            }
        } else {
            Vec::new()
        };
        lua.pop_n(1);

        lua.get_field(1, lua_string!("body"));
        let body_type = lua.lua_type(-1);
        let mut body = if body_type == LUA_TSTRING {
            lua.get_binary_string(-1).map(Bytes::copy_from_slice)
        } else if body_type == LUA_TNIL {
            None
        } else {
            lua.pop_n(1);
            bail!("rhttp: `body` must be a string");
        };
        lua.pop_n(1);

        lua.get_field(1, lua_string!("type"));
        let body_type = if lua.lua_type(-1) == LUA_TSTRING {
            lua.get_string(-1).map(|value| value.into_owned())
        } else {
            None
        };
        lua.pop_n(1);
        if body_type
            .as_ref()
            .is_some_and(|t| t.len() > MAX_CONTENT_TYPE_LEN)
        {
            bail!(format!(
                "Content type exceeded length limit ({MAX_CONTENT_TYPE_LEN})"
            ));
        }

        let mut generated_body_type = None;
        if !parameters.is_empty() {
            if method == Method::POST {
                if body.is_none() {
                    match serde_urlencoded::to_string(&parameters) {
                        Ok(encoded) => {
                            body = Some(Bytes::from(encoded.into_bytes()));
                            generated_body_type =
                                Some("application/x-www-form-urlencoded".to_string());
                        }
                        Err(e) => bail!(format!("Failed to encode parameters: {e}")),
                    }
                }
            } else {
                let mut query = url.query_pairs_mut();
                for (key, value) in &parameters {
                    query.append_pair(key, value);
                }
                drop(query);
                if url.as_str().len() > MAX_URL_LENGTH {
                    bail!(format!(
                        "URL exceeded length limit ({MAX_URL_LENGTH} bytes)"
                    ));
                }
            }
        }

        if body.as_ref().is_some_and(|body| body.len() > MAX_BODY_SIZE) {
            bail!("Request body exceeded memory limit");
        }
        if body.is_some() && !request_headers.contains_key(reqwest::header::CONTENT_TYPE) {
            let content_type = generated_body_type
                .or(body_type)
                .unwrap_or_else(|| "text/plain; charset=utf-8".to_string());
            match HeaderValue::from_str(&content_type) {
                Ok(value) => {
                    request_headers.insert(reqwest::header::CONTENT_TYPE, value);
                }
                Err(e) => bail!(format!("Invalid content type: {e}")),
            }
        }

        lua.get_field(1, lua_string!("timeout"));
        let timeout = if lua.lua_type(-1) == LUA_TNUMBER {
            clamp_timeout(lua.to_number(-1))
        } else {
            DEFAULT_REQUEST_TIMEOUT
        };
        lua.pop_n(1);

        lua.get_field(1, lua_string!("retries"));
        let retries = if lua.lua_type(-1) == LUA_TNUMBER {
            lua.to_integer(-1).max(0) as usize
        } else {
            default_retries(&method)
        }
        .min(MAX_RETRIES);
        lua.pop_n(1);

        lua.get_field(1, lua_string!("retry_delay"));
        let retry_base_delay = if lua.lua_type(-1) == LUA_TNUMBER {
            clamp_retry_delay(lua.to_number(-1))
        } else {
            DEFAULT_RETRY_DELAY
        };
        lua.pop_n(1);

        Ok(Self {
            method,
            url,
            headers: request_headers,
            body,
            timeout,
            retries,
            retry_base_delay,
            success,
            failed,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{
        clamp_retry_delay, clamp_timeout, default_retries, is_managed_header, is_supported_scheme,
    };
    use crate::config::{
        DEFAULT_REQUEST_TIMEOUT, DEFAULT_RETRY_DELAY, MAX_REQUEST_TIMEOUT, MIN_REQUEST_TIMEOUT,
        MIN_RETRY_DELAY,
    };
    use std::time::Duration;

    #[test]
    fn retries_only_idempotent_methods_by_default() {
        assert_eq!(default_retries(&reqwest::Method::GET), 2);
        assert_eq!(default_retries(&reqwest::Method::POST), 0);
    }

    #[test]
    fn only_http_and_https_urls_are_supported() {
        for url in ["http://example.com/", "https://example.com/x?q=1"] {
            assert!(is_supported_scheme(&url.parse().unwrap()), "{url}");
        }
        for url in [
            "file:///etc/passwd",
            "ftp://example.com/x",
            "gopher://example.com/",
        ] {
            assert!(!is_supported_scheme(&url.parse().unwrap()), "{url}");
        }
    }

    #[test]
    fn transport_headers_are_managed() {
        for name in [
            "host",
            "content-length",
            "transfer-encoding",
            "connection",
            "upgrade",
        ] {
            let header: reqwest::header::HeaderName = name.parse().unwrap();
            assert!(is_managed_header(&header), "{name}");
        }
        for name in ["content-type", "authorization", "x-api-key", "accept"] {
            let header: reqwest::header::HeaderName = name.parse().unwrap();
            assert!(!is_managed_header(&header), "{name}");
        }
    }

    #[test]
    fn timeout_supports_fractions_and_clamps() {
        assert_eq!(clamp_timeout(1.5), Duration::from_millis(1500));
        assert_eq!(clamp_timeout(0.0), MIN_REQUEST_TIMEOUT);
        assert_eq!(clamp_timeout(-5.0), MIN_REQUEST_TIMEOUT);
        assert_eq!(clamp_timeout(1_000_000.0), MAX_REQUEST_TIMEOUT);
        assert_eq!(clamp_timeout(f64::INFINITY), DEFAULT_REQUEST_TIMEOUT);
        assert_eq!(clamp_timeout(f64::NAN), DEFAULT_REQUEST_TIMEOUT);
    }

    #[test]
    fn negative_retry_delay_clamps_instead_of_panicking() {
        assert_eq!(clamp_retry_delay(-1.0), MIN_RETRY_DELAY);
        assert_eq!(clamp_retry_delay(0.0), MIN_RETRY_DELAY);
        assert_eq!(clamp_retry_delay(1.0), Duration::from_secs(1));
        assert_eq!(clamp_retry_delay(1e9), crate::config::MAX_RETRY_DELAY);
        assert_eq!(clamp_retry_delay(f64::NAN), DEFAULT_RETRY_DELAY);
    }
}
