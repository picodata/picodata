use super::{as_admin, ApiError, ApiResult, ErrorResponse, HttpResponse};
use crate::storage::Catalog;
use crate::traft::Result;
use http::StatusCode;
use serde::{Deserialize, Serialize};

const AUTH_TOKEN_EXPIRY: chrono::Duration = chrono::Duration::hours(24);
const REFRESH_TOKEN_EXPIRY: chrono::Duration = chrono::Duration::days(365);

#[derive(Debug, Serialize, Deserialize)]
struct JwtClaims {
    sub: String,
    typ: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    roles: Option<Vec<String>>,
}

#[derive(Debug, Default, Serialize)]
pub(crate) struct TokenResponse {
    auth: String,
    refresh: String,
}

#[derive(Debug, Default, Clone)]
pub struct AuthContext {
    pub username: String,
    pub roles: Vec<String>,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum AuthError {
    #[error("{0}")]
    Inner(String),
    #[error("auth disabled")]
    Disabled,
    #[error("invalid authorization header")]
    InvalidHeader,
    #[error("invalid token type")]
    InvalidType,
    #[error("invalid credentials")]
    InvalidCredentials,
    #[error("insufficient privileges")]
    InsufficientPrivileges,
    #[error("failed to encode jwt: {0}")]
    JWTEncoding(jwt_compact::CreationError),
    #[error("failed to decode jwt: {0}")]
    JWTParsing(jwt_compact::ParseError),
    #[error("invalid jwt: {0}")]
    JWTValidation(jwt_compact::ValidationError),
    #[error("failed to find user jwt: {0}")]
    UserLookup(String),
    #[error("failed to find privileges: {0}")]
    PrivilegesLookup(String),
}

impl AuthError {
    fn error_type(&self) -> String {
        String::from(match self {
            AuthError::Inner(..) => "error",
            AuthError::InvalidCredentials => "wrongCredentials",
            AuthError::JWTValidation(jwt_compact::ValidationError::Expired) => "sessionExpired",
            AuthError::Disabled => "authDisabled",
            _ => "authError",
        })
    }
    fn status_code(&self) -> StatusCode {
        match self {
            AuthError::Inner(_) => StatusCode::INTERNAL_SERVER_ERROR,
            AuthError::InsufficientPrivileges | AuthError::PrivilegesLookup(_) => {
                StatusCode::FORBIDDEN
            }
            _ => StatusCode::UNAUTHORIZED,
        }
    }

    fn error_msg(&self) -> String {
        self.to_string()
    }
}

type AuthResult<T> = std::result::Result<T, AuthError>;

impl From<AuthError> for HttpResponse {
    fn from(value: AuthError) -> Self {
        let status = value.status_code();
        let body = ErrorResponse {
            error: value.error_type(),
            error_message: value.error_msg(),
        }
        .to_string();

        HttpResponse::to_json_response(status, body)
    }
}

fn get_jwt_secret() -> AuthResult<Option<jwt_compact::alg::Hs256Key>> {
    let node = crate::traft::node::global().map_err(|e| AuthError::Inner(e.to_string()))?;
    let secret = node.alter_system_parameters.borrow().jwt_secret.clone();

    // Given successful db connection, secret cannot technically be None at this point,
    // as it is set to empty string to disable auth instead of NULL
    if secret.is_empty() {
        return Ok(None);
    }

    let secret = jwt_compact::alg::Hs256Key::new(secret.as_bytes());

    Ok(Some(secret))
}

fn create_jwt_token(
    username: &str,
    token_type: &str,
    roles: Option<Vec<String>>,
) -> AuthResult<String> {
    use jwt_compact::AlgorithmExt as _;

    let Some(secret_key) = get_jwt_secret()? else {
        return Err(AuthError::Disabled);
    };

    let time_options = jwt_compact::TimeOptions::default();

    let duration = match token_type {
        "auth" => AUTH_TOKEN_EXPIRY,
        "refresh" => REFRESH_TOKEN_EXPIRY,
        _ => return Err(AuthError::InvalidType),
    };

    let claims = JwtClaims {
        sub: username.to_string(),
        typ: token_type.to_string(),
        roles,
    };

    let header = jwt_compact::Header::empty();
    let claims = jwt_compact::Claims::new(claims).set_duration(&time_options, duration);

    jwt_compact::alg::Hs256
        .token(&header, &claims, &secret_key)
        .map_err(AuthError::JWTEncoding)
}

fn get_jwt_claims(auth_header: &str) -> AuthResult<JwtClaims> {
    use jwt_compact::AlgorithmExt as _;

    let Some(secret_key) = get_jwt_secret()? else {
        return Err(AuthError::Disabled);
    };

    let time_options = jwt_compact::TimeOptions::default();

    let token = auth_header
        .strip_prefix("Bearer ")
        .ok_or(AuthError::InvalidHeader)?;

    let validator = jwt_compact::alg::Hs256.validator(&secret_key);

    let token = jwt_compact::UntrustedToken::new(&token).map_err(AuthError::JWTParsing)?;

    let token = validator
        .validate(&token)
        .map_err(AuthError::JWTValidation)?;

    let (_, claims) = token.into_parts();

    claims
        .validate_expiration(&time_options)
        .map_err(AuthError::JWTValidation)?;

    Ok(claims.custom)
}

fn authenticate_user(username: &str, password: Option<&str>) -> AuthResult<Vec<String>> {
    if username.is_empty() {
        return Err(AuthError::InvalidCredentials);
    }

    let storage = Catalog::get();

    let user = storage
        .users
        .by_name(username)
        .map_err(|e| AuthError::UserLookup(e.to_string()))?
        .ok_or(AuthError::InvalidCredentials)?;

    let privileges = storage
        .privileges
        .by_grantee_id(user.id)
        .map_err(|e| AuthError::PrivilegesLookup(e.to_string()))?
        .map(|p| p.privilege().as_str().to_string())
        .collect::<Vec<String>>();

    if !privileges.contains(&"login".to_string()) {
        return Err(AuthError::InsufficientPrivileges);
    }

    if let Some(pwd) = password {
        crate::auth::authenticate_with_password(username, pwd)
            .map_err(|_| AuthError::InvalidCredentials)?;
    }

    Ok(privileges)
}

macro_rules! allow_disabled_auth {
    ($maybe_auth_disabled_error:expr, $fallback:expr) => {
        match $maybe_auth_disabled_error {
            Ok(tokens) => Ok(tokens),
            Err(e) => match e {
                AuthError::Disabled => Ok($fallback),
                _ => Err(e),
            },
        }
    };
}

pub(crate) fn http_api_login(username: String, password: String) -> AuthResult<TokenResponse> {
    allow_disabled_auth!(
        as_admin(|| {
            let roles = authenticate_user(&username, Some(password.as_str()))?;

            Ok(TokenResponse {
                auth: create_jwt_token(&username, "auth", Some(roles))?,
                refresh: create_jwt_token(&username, "refresh", None)?,
            })
        }),
        TokenResponse::default()
    )
}

pub(crate) fn http_api_refresh_session(auth_header: String) -> AuthResult<TokenResponse> {
    allow_disabled_auth!(
        as_admin(|| {
            let claims = get_jwt_claims(&auth_header)?;

            if claims.typ != "refresh" {
                return Err(AuthError::InvalidType);
            }

            let user_roles = authenticate_user(&claims.sub, None)?;

            let auth_token = create_jwt_token(&claims.sub, "auth", Some(user_roles))?;
            let refresh_token = create_jwt_token(&claims.sub, "refresh", None)?;

            Ok(TokenResponse {
                auth: auth_token,
                refresh: refresh_token,
            })
        }),
        TokenResponse::default()
    )
}

fn validate_auth(auth_header: &str) -> AuthResult<AuthContext> {
    let claims = get_jwt_claims(auth_header)?;

    if claims.typ != "auth" {
        return Err(AuthError::InvalidType);
    }

    let roles = claims.roles.unwrap_or_default();
    if !roles.contains(&"login".to_string()) {
        return Err(AuthError::InsufficientPrivileges);
    }

    Ok(AuthContext {
        username: claims.sub,
        roles,
    })
}

pub(super) fn auth_middleware<F, T, E>(auth_header: String, handler: F) -> ApiResult<T>
where
    F: FnOnce() -> Result<T, E>,
    ApiError: From<E>,
{
    allow_disabled_auth!(validate_auth(&auth_header), AuthContext::default())?;
    Ok(handler()?)
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct UiConfig {
    is_auth_enabled: bool,
}

pub(crate) fn http_api_config() -> AuthResult<UiConfig> {
    let is_auth_enabled = get_jwt_secret().is_ok_and(|secret| secret.is_some());

    Ok(UiConfig { is_auth_enabled })
}
