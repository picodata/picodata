use std::str::FromStr;
use tarantool::tlua;

pub const DEFAULT_USERNAME: &str = "guest";
pub const DEFAULT_LISTEN_HOST: &str = "127.0.0.1";
pub const DEFAULT_HTTP_PORT: &str = "8080";
pub const DEFAULT_IPROTO_PORT: &str = "3301";
pub const DEFAULT_PGPROTO_PORT: &str = "4327";

////////////////////
// IPROTO ADDRESS //
////////////////////

#[derive(Debug, Clone, PartialEq, Eq, tlua::Push, tlua::PushInto)]
pub struct IprotoAddress {
    pub user: Option<String>,
    pub host: String,
    pub port: String,
}

impl Default for IprotoAddress {
    fn default() -> Self {
        Self {
            user: None,
            host: DEFAULT_LISTEN_HOST.into(),
            port: DEFAULT_IPROTO_PORT.into(),
        }
    }
}

impl IprotoAddress {
    #[inline(always)]
    pub const fn default_host_port() -> &'static str {
        // TODO: "only literals can be passed to `concat!()`"
        // concat!(DEFAULT_LISTEN_HOST, ":", DEFAULT_IPROTO_PORT)
        "127.0.0.1:3301"
    }

    #[inline(always)]
    pub fn to_host_port(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }

    fn parse_address(addr: &str) -> Result<Self, String> {
        let format_err = || Err("valid format: [user@][host][:port]".to_string());
        let (user, host_port) = match addr.rsplit_once('@') {
            Some((user, host_port)) => {
                if user.contains(':') || user.contains('@') {
                    return format_err();
                }
                if user.is_empty() {
                    return format_err();
                }
                (Some(user), host_port)
            }
            None => (None, addr),
        };
        let (host, port) = match host_port.rsplit_once(':') {
            Some((host, port)) => {
                if host.contains(':') {
                    return format_err();
                }
                if port.is_empty() {
                    return format_err();
                }
                if port == DEFAULT_HTTP_PORT {
                    return Err(format!(
                        "IPROTO cannot listen on port {DEFAULT_HTTP_PORT} because it is default port for HTTP"
                    ));
                } else if port == DEFAULT_PGPROTO_PORT {
                    return Err(format!(
                        "IPROTO cannot listen on port {DEFAULT_PGPROTO_PORT} because it is default port for PGPROTO"
                    ));
                }
                let host = if host.is_empty() { None } else { Some(host) };
                (host, Some(port))
            }
            None => (Some(host_port), None),
        };
        Ok(Self {
            user: user.map(Into::into),
            host: host.unwrap_or(DEFAULT_LISTEN_HOST).into(),
            port: port.unwrap_or(DEFAULT_IPROTO_PORT).into(),
        })
    }
}

impl std::fmt::Display for IprotoAddress {
    #[inline(always)]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(user) = &self.user {
            write!(f, "{user}@{}:{}", self.host, self.port)
        } else {
            write!(f, "{}:{}", self.host, self.port)
        }
    }
}

impl FromStr for IprotoAddress {
    type Err = String;

    fn from_str(addr: &str) -> Result<Self, Self::Err> {
        Self::parse_address(addr)
    }
}

impl serde::Serialize for IprotoAddress {
    #[inline(always)]
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.to_string().serialize(serializer)
    }
}

impl<'de> serde::Deserialize<'de> for IprotoAddress {
    #[inline(always)]
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s: &str = serde::Deserialize::deserialize(deserializer)?;
        Self::from_str(s).map_err(serde::de::Error::custom)
    }
}

//////////////////
// HTTP ADDRESS //
//////////////////

#[derive(Debug, Clone, PartialEq, Eq, tlua::Push, tlua::PushInto)]
pub struct HttpAddress {
    pub host: String,
    pub port: String,
}

impl Default for HttpAddress {
    fn default() -> Self {
        Self {
            host: DEFAULT_LISTEN_HOST.into(),
            port: DEFAULT_HTTP_PORT.into(),
        }
    }
}

impl HttpAddress {
    #[inline(always)]
    pub const fn default_host_port() -> &'static str {
        // TODO: "only literals can be passed to `concat!()`"
        // concat!(DEFAULT_LISTEN_HOST, ":", DEFAULT_HTTP_PORT)
        "127.0.0.1:8080"
    }

    #[inline(always)]
    pub fn to_host_port(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }

    fn parse_address(addr: &str) -> Result<Self, String> {
        let format_err = || Err("valid format: [host][:port]".to_string());
        let (host, port) = match addr.rsplit_once(':') {
            Some((host, port)) => {
                if host.contains(':') {
                    return format_err();
                }
                if port.is_empty() {
                    return format_err();
                }
                if port == DEFAULT_IPROTO_PORT {
                    return Err(format!(
                        "HTTP cannot listen on port {DEFAULT_IPROTO_PORT} because it is default port for IPROTO"
                    ));
                } else if port == DEFAULT_PGPROTO_PORT {
                    return Err(format!(
                        "HTTP cannot listen on port {DEFAULT_PGPROTO_PORT} because it is default port for PGPROTO"
                    ));
                }
                let host = if host.is_empty() { None } else { Some(host) };
                (host, Some(port))
            }
            None => (Some(addr), None),
        };
        Ok(Self {
            host: host.unwrap_or(DEFAULT_LISTEN_HOST).into(),
            port: port.unwrap_or(DEFAULT_HTTP_PORT).into(),
        })
    }
}

impl std::fmt::Display for HttpAddress {
    #[inline(always)]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.host, self.port)
    }
}

impl FromStr for HttpAddress {
    type Err = String;

    fn from_str(addr: &str) -> Result<Self, Self::Err> {
        Self::parse_address(addr)
    }
}

impl serde::Serialize for HttpAddress {
    #[inline(always)]
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.to_string().serialize(serializer)
    }
}

impl<'de> serde::Deserialize<'de> for HttpAddress {
    #[inline(always)]
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s: &str = serde::Deserialize::deserialize(deserializer)?;
        Self::from_str(s).map_err(serde::de::Error::custom)
    }
}

/////////////////////
// PGPROTO ADDRESS //
/////////////////////

#[derive(Debug, Clone, PartialEq, Eq, tlua::Push, tlua::PushInto)]
pub struct PgprotoAddress {
    pub host: String,
    pub port: String,
}

impl Default for PgprotoAddress {
    fn default() -> Self {
        Self {
            host: DEFAULT_LISTEN_HOST.into(),
            port: DEFAULT_PGPROTO_PORT.into(),
        }
    }
}

impl PgprotoAddress {
    #[inline(always)]
    pub const fn default_host_port() -> &'static str {
        // TODO: "only literals can be passed to `concat!()`"
        // concat!(DEFAULT_LISTEN_HOST, ":", DEFAULT_PGPROTO_PORT)
        "127.0.0.1:4327"
    }

    #[inline(always)]
    pub fn to_host_port(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }

    fn parse_address(addr: &str) -> Result<Self, String> {
        let format_err = || Err("valid format: [host][:port]".to_string());
        let (host, port) = match addr.rsplit_once(':') {
            Some((host, port)) => {
                if host.contains(':') {
                    return format_err();
                }
                if port.is_empty() {
                    return format_err();
                }
                if port == DEFAULT_IPROTO_PORT {
                    return Err(format!(
                        "PGPROTO cannot listen on port {DEFAULT_IPROTO_PORT} because it is default port for IPROTO"
                    ));
                } else if port == DEFAULT_HTTP_PORT {
                    return Err(format!(
                        "PGPROTO cannot listen on port {DEFAULT_HTTP_PORT} because it is default port for HTTP"
                    ));
                }
                let host = if host.is_empty() { None } else { Some(host) };
                (host, Some(port))
            }
            None => (Some(addr), None),
        };
        Ok(Self {
            host: host.unwrap_or(DEFAULT_LISTEN_HOST).into(),
            port: port.unwrap_or(DEFAULT_PGPROTO_PORT).into(),
        })
    }
}

impl std::fmt::Display for PgprotoAddress {
    #[inline(always)]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.host, self.port)
    }
}

impl FromStr for PgprotoAddress {
    type Err = String;

    fn from_str(addr: &str) -> Result<Self, Self::Err> {
        Self::parse_address(addr)
    }
}

impl serde::Serialize for PgprotoAddress {
    #[inline(always)]
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.to_string().serialize(serializer)
    }
}

impl<'de> serde::Deserialize<'de> for PgprotoAddress {
    #[inline(always)]
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s: &str = serde::Deserialize::deserialize(deserializer)?;
        Self::from_str(s).map_err(serde::de::Error::custom)
    }
}

///////////
// TESTS //
///////////

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn try_parse_address() {
        //
        // check parsing of correct addresses
        //

        assert_eq!(
            "1234".parse(),
            Ok(IprotoAddress {
                user: None,
                host: "1234".into(),
                port: "3301".into()
            })
        );
        assert_eq!(
            ":1234".parse(),
            Ok(IprotoAddress {
                user: None,
                host: "127.0.0.1".into(),
                port: "1234".into()
            })
        );
        assert_eq!(
            "example".parse(),
            Ok(IprotoAddress {
                user: None,
                host: "example".into(),
                port: "3301".into()
            })
        );
        assert_eq!(
            "127.0.0.1:1234".parse(),
            Ok(IprotoAddress {
                user: None,
                host: "127.0.0.1".into(),
                port: "1234".into()
            })
        );
        assert_eq!(
            "1.2.3.4:1234".parse(),
            Ok(IprotoAddress {
                user: None,
                host: "1.2.3.4".into(),
                port: "1234".into()
            })
        );
        assert_eq!(
            "example:1234".parse(),
            Ok(IprotoAddress {
                user: None,
                host: "example".into(),
                port: "1234".into()
            })
        );

        assert_eq!(
            "user@host:port".parse(),
            Ok(IprotoAddress {
                user: Some("user".into()),
                host: "host".into(),
                port: "port".into()
            })
        );

        assert_eq!(
            "user@:port".parse(),
            Ok(IprotoAddress {
                user: Some("user".into()),
                host: "127.0.0.1".into(),
                port: "port".into()
            })
        );

        assert_eq!(
            "user@host".parse(),
            Ok(IprotoAddress {
                user: Some("user".into()),
                host: "host".into(),
                port: "3301".into()
            })
        );

        //
        // check parsing of incorrect addresses
        //

        assert!("example::1234".parse::<IprotoAddress>().is_err());
        assert!("user@@example".parse::<IprotoAddress>().is_err());
        assert!("user:pass@host:port".parse::<IprotoAddress>().is_err());
        assert!("a:b@c".parse::<IprotoAddress>().is_err());
        assert!("user:pass@host".parse::<IprotoAddress>().is_err());
        assert!("@host".parse::<IprotoAddress>().is_err());
        assert!("host:".parse::<IprotoAddress>().is_err());

        //
        // check conflicting ports in address parsing
        //

        let iproto_conflict_with_http = format!("host:{DEFAULT_HTTP_PORT}");
        assert_eq!(
            iproto_conflict_with_http.parse::<IprotoAddress>().unwrap_err(),
            format!("IPROTO cannot listen on port {DEFAULT_HTTP_PORT} because it is default port for HTTP")
        );
        let iproto_conflict_with_pg = format!("host:{DEFAULT_PGPROTO_PORT}");
        assert_eq!(
            iproto_conflict_with_pg.parse::<IprotoAddress>().unwrap_err(),
            format!("IPROTO cannot listen on port {DEFAULT_PGPROTO_PORT} because it is default port for PGPROTO")
        );

        let http_conflict_with_iproto = format!("host:{DEFAULT_IPROTO_PORT}");
        assert_eq!(
            http_conflict_with_iproto.parse::<HttpAddress>().unwrap_err(),
            format!("HTTP cannot listen on port {DEFAULT_IPROTO_PORT} because it is default port for IPROTO")
        );
        let http_conflict_with_pg = format!("host:{DEFAULT_PGPROTO_PORT}");
        assert_eq!(
            http_conflict_with_pg.parse::<HttpAddress>().unwrap_err(),
            format!("HTTP cannot listen on port {DEFAULT_PGPROTO_PORT} because it is default port for PGPROTO")
        );

        let pg_conflict_with_iproto = format!("host:{DEFAULT_IPROTO_PORT}");
        assert_eq!(
            pg_conflict_with_iproto.parse::<PgprotoAddress>().unwrap_err(),
            format!("PGPROTO cannot listen on port {DEFAULT_IPROTO_PORT} because it is default port for IPROTO")
        );
        let pg_conflict_with_http = format!("host:{DEFAULT_HTTP_PORT}");
        assert_eq!(
            pg_conflict_with_http.parse::<PgprotoAddress>().unwrap_err(),
            format!("PGPROTO cannot listen on port {DEFAULT_HTTP_PORT} because it is default port for HTTP")
        );

        //
        // check correctness of default values to avoid human factor
        //

        let default_iproto_addr = DEFAULT_LISTEN_HOST.parse::<IprotoAddress>().unwrap();
        assert!(default_iproto_addr.host == DEFAULT_LISTEN_HOST);
        assert!(default_iproto_addr.port == DEFAULT_IPROTO_PORT);

        let default_http_addr = DEFAULT_LISTEN_HOST.parse::<HttpAddress>().unwrap();
        assert!(default_http_addr.host == DEFAULT_LISTEN_HOST);
        assert!(default_http_addr.port == DEFAULT_HTTP_PORT);

        let default_pgproto_addr = DEFAULT_LISTEN_HOST.parse::<PgprotoAddress>().unwrap();
        assert!(default_pgproto_addr.host == DEFAULT_LISTEN_HOST);
        assert!(default_pgproto_addr.port == DEFAULT_PGPROTO_PORT);
    }
}
