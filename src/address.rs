use std::str::FromStr;
use tarantool::tlua;

const DEFAULT_HOST: &str = "localhost";
const DEFAULT_PORT: &str = "3301";

#[derive(Debug, Clone, PartialEq, Eq, tlua::Push, tlua::PushInto)]
pub struct Address {
    pub user: Option<String>,
    pub host: String,
    pub port: String,
}

impl Address {
    #[inline(always)]
    pub fn to_host_port(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

impl std::fmt::Display for Address {
    #[inline(always)]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(user) = &self.user {
            write!(f, "{user}@{}:{}", self.host, self.port)
        } else {
            write!(f, "{}:{}", self.host, self.port)
        }
    }
}

impl FromStr for Address {
    type Err = String;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let err = Err("valid format: [user@][host][:port]".to_string());
        let (user, host_port) = match text.rsplit_once('@') {
            Some((user, host_port)) => {
                if user.contains(':') || user.contains('@') {
                    return err;
                }
                if user.is_empty() {
                    return err;
                }
                (Some(user), host_port)
            }
            None => (None, text),
        };
        let (host, port) = match host_port.rsplit_once(':') {
            Some((host, port)) => {
                if host.contains(':') {
                    return err;
                }
                if port.is_empty() {
                    return err;
                }
                let host = if host.is_empty() { None } else { Some(host) };
                (host, Some(port))
            }
            None => (Some(host_port), None),
        };
        Ok(Self {
            user: user.map(Into::into),
            host: host.unwrap_or(DEFAULT_HOST).into(),
            // FIXME: this is wrong for a general `Address` struct. There's
            // currently a bug, if we run with `--http-listen=localhost`, it
            // will attempt to bind the http server to port 3301.
            port: port.unwrap_or(DEFAULT_PORT).into(),
        })
    }
}

impl serde::Serialize for Address {
    #[inline(always)]
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.to_string().serialize(serializer)
    }
}

impl<'de> serde::Deserialize<'de> for Address {
    #[inline(always)]
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s: &str = serde::Deserialize::deserialize(deserializer)?;
        Self::from_str(s).map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn try_parse_address() {
        assert_eq!(
            "1234".parse(),
            Ok(Address {
                user: None,
                host: "1234".into(),
                port: "3301".into()
            })
        );
        assert_eq!(
            ":1234".parse(),
            Ok(Address {
                user: None,
                host: "localhost".into(),
                port: "1234".into()
            })
        );
        assert_eq!(
            "example".parse(),
            Ok(Address {
                user: None,
                host: "example".into(),
                port: "3301".into()
            })
        );
        assert_eq!(
            "localhost:1234".parse(),
            Ok(Address {
                user: None,
                host: "localhost".into(),
                port: "1234".into()
            })
        );
        assert_eq!(
            "1.2.3.4:1234".parse(),
            Ok(Address {
                user: None,
                host: "1.2.3.4".into(),
                port: "1234".into()
            })
        );
        assert_eq!(
            "example:1234".parse(),
            Ok(Address {
                user: None,
                host: "example".into(),
                port: "1234".into()
            })
        );

        assert_eq!(
            "user@host:port".parse(),
            Ok(Address {
                user: Some("user".into()),
                host: "host".into(),
                port: "port".into()
            })
        );

        assert_eq!(
            "user@:port".parse(),
            Ok(Address {
                user: Some("user".into()),
                host: "localhost".into(),
                port: "port".into()
            })
        );

        assert_eq!(
            "user@host".parse(),
            Ok(Address {
                user: Some("user".into()),
                host: "host".into(),
                port: "3301".into()
            })
        );

        assert!("example::1234".parse::<Address>().is_err());
        assert!("user@@example".parse::<Address>().is_err());
        assert!("user:pass@host:port".parse::<Address>().is_err());
        assert!("a:b@c".parse::<Address>().is_err());
        assert!("user:pass@host".parse::<Address>().is_err());
        assert!("@host".parse::<Address>().is_err());
        assert!("host:".parse::<Address>().is_err());
    }
}
