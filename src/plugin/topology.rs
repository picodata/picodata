use crate::instance::{Instance, InstanceName};
use crate::schema::ServiceDef;
use crate::traft::node;
use smol_str::SmolStr;

pub struct TopologyContext {
    instance_tier: SmolStr,
    instance_name: InstanceName,
}

impl TopologyContext {
    /// Return topology context for current instance.
    pub fn current() -> Self {
        let node = node::global().expect("node must be already initialized");
        let instance_name = node.topology_cache.my_instance_name().into();
        let instance_tier = node.topology_cache.my_tier_name().into();

        Self {
            instance_name,
            instance_tier,
        }
    }

    /// Return topology context for instance.
    pub fn for_instance(instance: &Instance) -> Self {
        Self {
            instance_name: instance.name.clone(),
            instance_tier: instance.tier.clone(),
        }
    }

    pub fn instance_name(&self) -> &InstanceName {
        &self.instance_name
    }
}

/// Check that service is available in given topology context.
pub fn probe_service(ctx: &TopologyContext, svc_def: &ServiceDef) -> bool {
    svc_def.tiers.contains(&ctx.instance_tier)
}

/// Check that tiers is available in given topology context.
pub fn probe_tiers(ctx: &TopologyContext, tiers: &[SmolStr]) -> bool {
    tiers.contains(&ctx.instance_tier)
}
