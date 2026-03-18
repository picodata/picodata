use smol_str::format_smolstr;
use smol_str::SmolStr;
use std::str::FromStr;
use tarantool::tlua;

pub const DEFAULT_USERNAME: &str = "guest";
pub const DEFAULT_LISTEN_HOST: &str = "127.0.0.1";
pub const DEFAULT_HTTP_PORT: &str = "5327";
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

pub trait ListenAddress {
    fn host(&self) -> &str;
    fn port(&self) -> &str;
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
    pub fn to_host_port(&self) -> SmolStr {
        format_smolstr!("{}:{}", self.host, self.port)
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

impl ListenAddress for IprotoAddress {
    fn host(&self) -> &str {
        &self.host
    }
    fn port(&self) -> &str {
        &self.port
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
        "127.0.0.1:5327"
    }

    #[inline(always)]
    pub fn to_host_port(&self) -> SmolStr {
        format_smolstr!("{}:{}", self.host, self.port)
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

impl ListenAddress for HttpAddress {
    fn host(&self) -> &str {
        &self.host
    }
    fn port(&self) -> &str {
        &self.port
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
    pub fn to_host_port(&self) -> SmolStr {
        format_smolstr!("{}:{}", self.host, self.port)
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

impl ListenAddress for PgprotoAddress {
    fn host(&self) -> &str {
        &self.host
    }
    fn port(&self) -> &str {
        &self.port
    }
}

//////////////////////
// PLUGIN ADDRESS //
//////////////////////

/// Address type for plugin listener configuration.
///
/// Validates HOST:PORT format during deserialization, consistent with
/// `IprotoAddress`, `HttpAddress`, and `PgprotoAddress`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PluginAddress {
    pub host: String,
    pub port: String,
}

impl PluginAddress {
    #[inline(always)]
    pub fn to_host_port(&self) -> SmolStr {
        format_smolstr!("{}:{}", self.host, self.port)
    }
}

impl std::fmt::Display for PluginAddress {
    #[inline(always)]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.host, self.port)
    }
}

impl FromStr for PluginAddress {
    type Err = String;

    fn from_str(addr: &str) -> Result<Self, Self::Err> {
        let (host, port) = parse_host_and_port(addr)?;
        Ok(Self { host, port })
    }
}

impl serde::Serialize for PluginAddress {
    #[inline(always)]
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.to_string().serialize(serializer)
    }
}

impl<'de> serde::Deserialize<'de> for PluginAddress {
    #[inline(always)]
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s: &str = serde::Deserialize::deserialize(deserializer)?;
        Self::from_str(s).map_err(serde::de::Error::custom)
    }
}

impl ListenAddress for PluginAddress {
    fn host(&self) -> &str {
        &self.host
    }
    fn port(&self) -> &str {
        &self.port
    }
}

///////////////////////////////
// Address conflict checking //
///////////////////////////////

#[derive(Clone)]
struct ConflictCheckerItem {
    source: String,
    host: String,
    port: String,
}

impl ConflictCheckerItem {
    fn conflicts_with(&self, other: &ConflictCheckerItem) -> bool {
        const IPV4_WILDCARD: &str = "0.0.0.0";

        let shares_host =
            self.host == other.host || self.host == IPV4_WILDCARD || other.host == IPV4_WILDCARD;

        shares_host && self.port == other.port
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AddressConflict {
    pub source_a: String,
    pub source_b: String,
    pub host_a: String,
    pub host_b: String,
    pub port_a: String,
    pub port_b: String,
}

impl AddressConflict {
    fn new(a: ConflictCheckerItem, b: ConflictCheckerItem) -> Self {
        Self {
            source_a: a.source,
            source_b: b.source,
            host_a: a.host,
            host_b: b.host,
            port_a: a.port,
            port_b: b.port,
        }
    }
}

pub struct AddressConflictChecker {
    items: Vec<ConflictCheckerItem>,
}

impl AddressConflictChecker {
    #[allow(clippy::new_without_default)] // this is a very specific type with a very specific expected usage flow
    pub fn new() -> Self {
        Self { items: Vec::new() }
    }

    pub fn push(&mut self, source: impl Into<String>, address: &impl ListenAddress) {
        self.items.push(ConflictCheckerItem {
            source: source.into(),
            host: address.host().into(),
            port: address.port().into(),
        })
    }

    pub fn check(self) -> Option<AddressConflict> {
        // Perhaps, doing a quadratic search is not the wisest
        // For now we don't expect to have many listeners, but this may become slow if it changes

        // Check all pairs for conflicts.
        for i in 0..self.items.len() {
            for j in (i + 1)..self.items.len() {
                if self.items[i].conflicts_with(&self.items[j]) {
                    return Some(AddressConflict::new(
                        self.items[i].clone(),
                        self.items[j].clone(),
                    ));
                }
            }
        }

        None
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
    #[test]
    fn addresses_conflict_same() {
        let mut checker = AddressConflictChecker::new();

        checker.push(
            "http.listen",
            &HttpAddress {
                host: "127.0.0.1".into(),
                port: "5327".into(),
            },
        );
        checker.push(
            "pgproto.listen",
            &PgprotoAddress {
                host: "127.0.0.1".into(),
                port: "5327".into(),
            },
        );

        assert_eq!(
            checker.check().unwrap(),
            AddressConflict {
                source_a: "http.listen".to_string(),
                source_b: "pgproto.listen".to_string(),
                host_a: "127.0.0.1".to_string(),
                host_b: "127.0.0.1".to_string(),
                port_a: "5327".to_string(),
                port_b: "5327".to_string(),
            }
        );
    }
    #[test]
    fn addresses_conflict_different_ports() {
        let mut checker = AddressConflictChecker::new();
        checker.push(
            "http.listen",
            &HttpAddress {
                host: "127.0.0.1".into(),
                port: "5327".into(),
            },
        );
        checker.push(
            "pgproto.listen",
            &PgprotoAddress {
                host: "127.0.0.1".into(),
                port: "9090".into(),
            },
        );
        assert_eq!(checker.check(), None);
    }

    #[test]
    fn addresses_conflict_wildcard_ipv4() {
        let mut checker = AddressConflictChecker::new();
        checker.push(
            "iproto.listen",
            &IprotoAddress {
                user: None,
                host: "0.0.0.0".into(),
                port: "3301".into(),
            },
        );
        checker.push(
            "http.listen",
            &HttpAddress {
                host: "127.0.0.1".into(),
                port: "3301".into(),
            },
        );
        assert_eq!(
            checker.check().unwrap(),
            AddressConflict {
                source_a: "iproto.listen".to_string(),
                source_b: "http.listen".to_string(),
                host_a: "0.0.0.0".to_string(),
                host_b: "127.0.0.1".to_string(),
                port_a: "3301".to_string(),
                port_b: "3301".to_string(),
            }
        );
    }

    #[test]
    fn addresses_no_conflict_different_hosts() {
        let mut checker = AddressConflictChecker::new();
        checker.push(
            "http.listen",
            &HttpAddress {
                host: "192.168.1.1".into(),
                port: "5327".into(),
            },
        );
        checker.push(
            "pgproto.listen",
            &PgprotoAddress {
                host: "127.0.0.1".into(),
                port: "5327".into(),
            },
        );
        assert_eq!(checker.check(), None);
    }
}
