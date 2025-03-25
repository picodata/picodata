use std::str::FromStr;
use tarantool::tlua;

pub const DEFAULT_USERNAME: &str = "guest";
pub const DEFAULT_LISTEN_HOST: &str = "127.0.0.1";
pub const DEFAULT_HTTP_PORT: &str = "8080";
pub const DEFAULT_IPROTO_PORT: &str = "3301";
pub const DEFAULT_PGPROTO_PORT: &str = "4327";

fn parse_host_and_port(addr: &str) -> Result<(String, String), String> {
    let format_err = || Err("valid format: HOST:PORT".to_string());
    let Some((host, port)) = addr.rsplit_once(':') else {
        return format_err();
    };
    if host.is_empty() || port.is_empty() || host.contains(':') {
        return format_err();
    }
    Ok((host.into(), port.into()))
}

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
        let format_err = || Err("valid format: [USER@]HOST:PORT".to_string());
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
        match parse_host_and_port(host_port) {
            Ok((host, port)) => Ok(Self {
                user: user.map(str::to_owned),
                host,
                port,
            }),
            Err(_) => format_err(),
        }
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
        let (host, port) = parse_host_and_port(addr)?;
        Ok(Self { host, port })
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
        let (host, port) = parse_host_and_port(addr)?;
        Ok(Self { host, port })
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
        assert!("1234".parse::<IprotoAddress>().is_err());
        assert!(":1234".parse::<IprotoAddress>().is_err());
        assert!("example".parse::<IprotoAddress>().is_err());
        assert!("user@:port".parse::<IprotoAddress>().is_err());
        assert!("user@host".parse::<IprotoAddress>().is_err());

        assert_eq!(
            "1234".parse::<IprotoAddress>(),
            Err("valid format: [USER@]HOST:PORT".into()),
        );
        assert_eq!(
            "1234".parse::<HttpAddress>(),
            Err("valid format: HOST:PORT".into()),
        );
        assert_eq!(
            "1234".parse::<PgprotoAddress>(),
            Err("valid format: HOST:PORT".into()),
        );

        //
        // check conflicting ports in address parsing
        //

        let iproto_conflict_with_http = format!("host:{DEFAULT_HTTP_PORT}");
        assert!(iproto_conflict_with_http.parse::<IprotoAddress>().is_ok(),);
        let iproto_conflict_with_pg = format!("host:{DEFAULT_PGPROTO_PORT}");
        assert!(iproto_conflict_with_pg.parse::<IprotoAddress>().is_ok());

        let http_conflict_with_iproto = format!("host:{DEFAULT_IPROTO_PORT}");
        assert!(http_conflict_with_iproto.parse::<HttpAddress>().is_ok(),);
        let http_conflict_with_pg = format!("host:{DEFAULT_PGPROTO_PORT}");
        assert!(http_conflict_with_pg.parse::<HttpAddress>().is_ok(),);

        let pg_conflict_with_iproto = format!("host:{DEFAULT_IPROTO_PORT}");
        assert!(pg_conflict_with_iproto.parse::<PgprotoAddress>().is_ok(),);
        let pg_conflict_with_http = format!("host:{DEFAULT_HTTP_PORT}");
        assert!(pg_conflict_with_http.parse::<PgprotoAddress>().is_ok(),);

        //
        // check correctness of default values to avoid human factor
        //

        let default_http_addr = HttpAddress::default_host_port()
            .parse::<HttpAddress>()
            .unwrap();
        assert!(default_http_addr.host == DEFAULT_LISTEN_HOST);
        assert!(default_http_addr.port == DEFAULT_HTTP_PORT);

        let default_pgproto_addr = PgprotoAddress::default_host_port()
            .parse::<PgprotoAddress>()
            .unwrap();
        assert!(default_pgproto_addr.host == DEFAULT_LISTEN_HOST);
        assert!(default_pgproto_addr.port == DEFAULT_PGPROTO_PORT);
    }
}
