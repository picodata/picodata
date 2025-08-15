use crate::introspection::Introspection;
use crate::static_ref;
use crate::traft::error::Error;
use std::path::PathBuf;
use tarantool::network::client::tls;

#[derive(
    Clone, Debug, Default, Eq, Introspection, PartialEq, serde::Deserialize, serde::Serialize,
)]
pub struct TlsConfig {
    #[introspection(config_default = false)]
    pub enabled: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cert_file: Option<PathBuf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key_file: Option<PathBuf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ca_file: Option<PathBuf>,
}

static mut TLS_CONTEXT: Option<TlsContext> = None;

struct TlsContext {
    tls_connector: Option<tls::TlsConnector>,
}

pub fn tls_init_once(config: &TlsConfig) -> Result<(), Error> {
    let context = TlsContext::new(config.clone())?;

    // SAFETY: safe as long as only called from tx thread.
    unsafe {
        // This check protects us from use-after-free in client fibers.
        if static_ref!(const TLS_CONTEXT).is_some() {
            panic!("iproto tls cannot be initialized more than once!");
        }
        TLS_CONTEXT = Some(context);
    }

    Ok(())
}

pub fn get_tls_connector() -> Option<&'static tls::TlsConnector> {
    let context = unsafe { static_ref!(const TLS_CONTEXT) }
        .as_ref()
        .expect("iproto tls context is uninitialized");
    context.tls_connector.as_ref()
}

impl TlsContext {
    fn new(config: TlsConfig) -> Result<Self, Error> {
        if !config.enabled {
            return Ok(Self {
                tls_connector: None,
            });
        }

        let cert_file = config
            .cert_file
            .as_ref()
            .ok_or_else(|| Error::invalid_configuration("argument 'cert_file' is missing"))?;
        let key_file = config
            .key_file
            .as_ref()
            .ok_or_else(|| Error::invalid_configuration("argument 'key_file' is missing"))?;
        let ca_file = config
            .ca_file
            .as_ref()
            .ok_or_else(|| Error::invalid_configuration("argument 'ca_file' is missing"))?;
        let tls_config = tls::TlsConfig {
            cert_file,
            key_file,
            ca_file: Some(ca_file),
        };
        let tls_connector = tls::TlsConnector::new(tls_config)?;

        Ok(Self {
            tls_connector: Some(tls_connector),
        })
    }
}
