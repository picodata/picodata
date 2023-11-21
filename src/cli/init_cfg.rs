use crate::tier::Tier;

#[derive(Debug, serde::Deserialize, PartialEq, Clone)]
pub struct InitCfg {
    #[serde(deserialize_with = "map_to_vec_of_tiers")]
    pub tier: Vec<Tier>,
}

impl Default for InitCfg {
    fn default() -> Self {
        InitCfg {
            tier: vec![Tier::default()],
        }
    }
}

impl InitCfg {
    pub fn try_from_yaml_file(path: &str) -> Result<InitCfg, String> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| format!("can't read from {path}, error: {e}"))?;

        let cfg: InitCfg = serde_yaml::from_str(&content)
            .map_err(|e| format!("error while parsing {path}, reason: {e}"))?;

        if cfg.tier.is_empty() {
            return Err(format!("empty `tier` field in init-cfg by path: {path}"));
        }

        Ok(cfg)
    }
}

pub fn map_to_vec_of_tiers<'de, D>(des: D) -> Result<Vec<Tier>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(serde::Deserialize)]
    struct TierConf {
        replication_factor: u8,
    }

    struct Vis;

    impl<'de> serde::de::Visitor<'de> for Vis {
        type Value = Vec<Tier>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("a map of tier name to tier's config")
        }

        fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
        where
            A: serde::de::MapAccess<'de>,
        {
            let mut names = std::collections::HashSet::new();
            let mut tiers = Vec::new();
            while let Some((name, tier_conf)) = map.next_entry::<String, TierConf>()? {
                if !names.insert(name.clone()) {
                    return Err(serde::de::Error::custom(format!(
                        "duplicated tier name `{name}` found"
                    )));
                }

                tiers.push(Tier {
                    name,
                    replication_factor: tier_conf.replication_factor,
                });
            }

            Ok(tiers)
        }
    }

    des.deserialize_map(Vis)
}
