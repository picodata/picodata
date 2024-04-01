use crate::info::InstanceInfo;
use crate::instance::{Instance, InstanceId};
use crate::plugin;
use crate::plugin::PluginError;
use crate::schema::ServiceDef;
use crate::traft::node;

pub struct TopologyContext {
    instance_tier: String,
    instance_id: InstanceId,
}

impl TopologyContext {
    /// Return topology context for current instance.
    pub fn current() -> plugin::Result<Self> {
        let node = node::global().expect("node must be already initialized");
        let current_instance_info = InstanceInfo::try_get(node, None)
            .map_err(|e| PluginError::TopologyError(e.to_string()))?;

        Ok(Self {
            instance_id: current_instance_info.instance_id,
            instance_tier: current_instance_info.tier,
        })
    }

    /// Return topology context for instance.
    pub fn for_instance(instance: &Instance) -> Self {
        Self {
            instance_id: instance.instance_id.clone(),
            instance_tier: instance.tier.clone(),
        }
    }

    pub fn instance_id(&self) -> &InstanceId {
        &self.instance_id
    }
}

/// Check that service is available in given topology context.
pub fn probe_service(ctx: &TopologyContext, svc_def: &ServiceDef) -> bool {
    svc_def.tiers.contains(&ctx.instance_tier)
}

/// Check that tiers is available in given topology context.
pub fn probe_tiers(ctx: &TopologyContext, tiers: &[String]) -> bool {
    tiers.contains(&ctx.instance_tier)
}
