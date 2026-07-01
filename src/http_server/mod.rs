use http::StatusCode;
use serde::Serialize;
use tarantool::session::with_su;

pub mod auth;
mod cluster;
mod health;
mod instance;

pub(crate) use auth::{http_api_config, http_api_login, http_api_refresh_session};
pub(crate) use cluster::{
    http_api_cluster_with_auth, http_api_memory_with_auth, http_api_tiers_with_auth,
};
pub(crate) use health::{
    http_api_health_live, http_api_health_ready, http_api_health_startup, http_api_health_status,
    http_api_health_status_with_auth, HealthStatus,
};
pub(crate) use instance::http_api_instance_detail_with_auth;

const DEFAULT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(1);
const CONTENT_TYPE_JSON: &'static str = "application/json";

/// Newtype wrapper around `http::Response<String>` for HTTP API responses.
/// We have to make it a newtype and not just an alias to provide From/Into
/// imlementations for HttpResponseTable and not hit the orphan rule.
pub(crate) struct HttpResponse(pub(crate) http::Response<String>);

impl HttpResponse {
    /// Creates a JSON response with the given status code and body.
    pub fn to_json_response(status: StatusCode, body: String) -> Self {
        /* Building HTTP response can't fail here because it would fail only for invalid status
         * code (outside 100-999 range), invalid HTTP version or header name/value issues such as
         * \r\n characters. None of these can possibly happen here.
         */
        Self(
            http::Response::builder()
                .status(status)
                .header(http::header::CONTENT_TYPE, CONTENT_TYPE_JSON)
                .body(body)
                .expect("building HTTP response should not fail"),
        )
    }
}

/// Type alias for the `AsTable` structure expected by Lua HTTP server.
pub(crate) type HttpResponseTable = tarantool::tlua::AsTable<(
    (&'static str, u16),
    (&'static str, String),
    (
        &'static str,
        tarantool::tlua::AsTable<((&'static str, String),)>,
    ),
)>;

impl From<HttpResponse> for HttpResponseTable {
    fn from(response: HttpResponse) -> Self {
        let status = response.0.status().as_u16();
        let content_type = response
            .0
            .headers()
            .get(http::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or(CONTENT_TYPE_JSON)
            .to_owned();
        let body = response.0.into_body();

        tarantool::tlua::AsTable((
            ("status", status),
            ("body", body),
            (
                "headers",
                tarantool::tlua::AsTable((("content-type", content_type),)),
            ),
        ))
    }
}

fn as_admin<T>(f: impl FnOnce() -> T) -> T {
    with_su(1, f).expect("becoming admin should not fail")
}

#[derive(Debug, Serialize)]
struct ErrorResponse {
    error: String,
    #[serde(rename = "errorMessage")]
    error_message: String,
}

impl std::fmt::Display for ErrorResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match serde_json::to_string(self) {
            Ok(value) => write!(f, "{}", value),
            Err(e) => write!(f, r#"{{"error":"{}","errorMessage":"Internal error"}}"#, e),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ApiError {
    #[error("{0}")]
    Auth(#[from] auth::AuthError),
    #[error("{0}")]
    Internal(#[from] crate::traft::error::Error),
    #[error("{0}")]
    HealthCheck(#[from] health::HealthCheckError),
    #[error("{0}")]
    NotFound(String),
}

impl From<crate::traft::error::Error> for HttpResponse {
    fn from(value: crate::traft::error::Error) -> Self {
        let body = ErrorResponse {
            error: String::from("error"),
            error_message: value.to_string(),
        }
        .to_string();
        HttpResponse::to_json_response(StatusCode::INTERNAL_SERVER_ERROR, body)
    }
}

impl From<ApiError> for HttpResponse {
    fn from(value: ApiError) -> Self {
        match value {
            ApiError::Auth(e) => e.into(),
            ApiError::Internal(e) => e.into(),
            ApiError::HealthCheck(e) => e.into(),
            ApiError::NotFound(msg) => {
                let body = ErrorResponse {
                    error: String::from("notFound"),
                    error_message: msg,
                }
                .to_string();
                HttpResponse::to_json_response(StatusCode::NOT_FOUND, body)
            }
        }
    }
}

pub(crate) type ApiResult<T> = std::result::Result<T, ApiError>;

pub(crate) fn wrap_api_result<T, E>(result: crate::traft::Result<T, E>) -> HttpResponseTable
where
    T: Serialize,
    E: Into<HttpResponse>,
{
    let response: HttpResponse = match result {
        Ok(res) => match serde_json::to_string(&res) {
            Ok(body) => HttpResponse::to_json_response(StatusCode::OK, body),
            Err(err) => HttpResponse::to_json_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!(r#"{{"error":"{err}","errorMessage":"Internal error"}}"#),
            ),
        },
        Err(err) => err.into(),
    };

    response.into()
}
