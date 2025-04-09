#![allow(unused_parens)]
use crate::instance::Instance;
use crate::replicaset::Replicaset;
use crate::schema::ServiceRouteItem;
use crate::storage::Catalog;
use crate::storage::ToEntryIter;
use crate::tier::Tier;
#[allow(unused_imports)]
use crate::tlog;
use crate::traft::error::Error;
use crate::traft::error::IdOfInstance;
#[allow(unused_imports)]
use crate::traft::node::NodeImpl;
use crate::traft::RaftId;
use crate::traft::Result;
use std::cell::OnceCell;
use std::collections::HashMap;
use tarantool::fiber::safety::NoYieldsRef;
use tarantool::fiber::safety::NoYieldsRefCell;

pub type TopologyCacheRef<'a> = NoYieldsRef<'a, TopologyCacheMutable>;

////////////////////////////////////////////////////////////////////////////////
// TopologyCache
////////////////////////////////////////////////////////////////////////////////

/// Stores deserialized info from system table like _pico_instance,
/// _pico_replicaset, etc. The data in this struct is automatically kept up to
/// date in [`NodeImpl::advance`]: whenever new raft entries or raft snapshots
/// are handled, the data is updated correspondingly.
///
/// Doesn't implement `Clone` by design, must be wrapped in an `Rc`.
pub struct TopologyCache {
    /// Stores topology data which may change during run time.
    ///
    /// This data is volatile, so must be guarded by `NoYieldsRefCell`. Other
    /// data in this struct is immutable, so it's safe to hold references to it
    /// across yields.
    inner: NoYieldsRefCell<TopologyCacheMutable>,

    /// Raft id is always known and never changes, so it can be cached here.
    pub my_raft_id: RaftId,

    /// Instance name never changes on running instance after it's determined, so it can be cached here.
    pub my_instance_name: OnceCell<String>,

    /// Instance uuid never changes on running instance after it's determined, so it can be cached here.
    pub my_instance_uuid: OnceCell<String>,

    /// Replicaset name never changes on running instance after it's determined, so it can be cached here.
    pub my_replicaset_name: OnceCell<String>,

    /// Replicaset uuid never changes on running instance after it's determined, so it can be cached here.
    pub my_replicaset_uuid: OnceCell<String>,

    /// Tier name never changes on running instance after it's determined, so it can be cached here.
    pub my_tier_name: OnceCell<String>,
}

impl TopologyCache {
    /// Initializes the cache by loading all of the contents from _pico_instance
    /// and _pico_replicaset system tables.
    pub fn load(storage: &Catalog, my_raft_id: RaftId) -> Result<Self> {
        let inner = TopologyCacheMutable::load(storage, my_raft_id)?;

        let my_instance_name = OnceCell::new();
        let my_instance_uuid = OnceCell::new();
        let my_replicaset_name = OnceCell::new();
        let my_replicaset_uuid = OnceCell::new();
        let my_tier_name = OnceCell::new();
        if let Some(this_instance) = inner.this_instance.get() {
            // If at this point we know this instance's info, we cache it immediately.
            // Otherwise this info will be set in `update_instance` bellow as soon as it's known.
            my_instance_name
                .set(this_instance.name.to_string())
                .expect("was empty");
            my_instance_uuid
                .set(this_instance.uuid.clone())
                .expect("was empty");
            my_replicaset_name
                .set(this_instance.replicaset_name.to_string())
                .expect("was empty");
            my_replicaset_uuid
                .set(this_instance.replicaset_uuid.clone())
                .expect("was empty");
            my_tier_name
                .set(this_instance.tier.clone())
                .expect("was empty");
        }

        Ok(Self {
            my_raft_id,
            my_instance_name,
            my_instance_uuid,
            my_replicaset_name,
            my_replicaset_uuid,
            my_tier_name,
            inner: NoYieldsRefCell::new(inner),
        })
    }

    /// Drop all cached data and load it all again from the storage.
    ///
    /// This is called from [`NodeImpl::advance`] after receiving a snapshot.
    pub fn full_reload(&self, storage: &Catalog) -> Result<()> {
        let mut inner = self.inner.borrow_mut();
        // Drop the old data and replace it with new loaded from the storage
        *inner = TopologyCacheMutable::load(storage, self.my_raft_id)?;
        Ok(())
    }

    /// Get access to the volatile topology info, i.e. info which may change
    /// during execution.
    ///
    /// Returns a reference object which **MUST NOT** be held across yields.
    /// Use this if you want to avoid redundant copies.
    ///
    /// Use [`Self::clone_instance_by_name`], [`Self::clone_replicaset_by_uuid`]
    /// if you need the copies anyway.
    ///
    /// Use [`Self::my_instance_name`], [`Self::my_tier_name`], etc. if you need
    /// access to the immutable info of current instance.
    #[inline(always)]
    #[track_caller]
    pub fn get(&self) -> NoYieldsRef<TopologyCacheMutable> {
        self.inner.borrow()
    }

    #[inline(always)]
    pub fn clone_this_instance(&self) -> Instance {
        self.get()
            .this_instance
            .get()
            .expect("don't call this until it's known")
            .clone()
    }

    #[inline(always)]
    pub fn my_instance_name(&self) -> &str {
        self.my_instance_name
            .get()
            .expect("don't call this until it's known")
    }

    #[inline(always)]
    pub fn my_instance_uuid(&self) -> &str {
        self.my_instance_uuid
            .get()
            .expect("don't call this until it's known")
    }

    #[inline(always)]
    pub fn my_replicaset_name(&self) -> &str {
        self.my_replicaset_name
            .get()
            .expect("don't call this until it's known")
    }

    #[inline(always)]
    pub fn my_replicaset_uuid(&self) -> &str {
        self.my_replicaset_uuid
            .get()
            .expect("don't call this until it's known")
    }

    #[inline(always)]
    pub fn my_tier_name(&self) -> &str {
        self.my_tier_name
            .get()
            .expect("don't call this until it's known")
    }

    #[inline(always)]
    pub fn clone_instance_by_name(&self, name: &str) -> Result<Instance> {
        self.get().instance_by_name(name).cloned()
    }

    #[inline(always)]
    pub fn clone_instance_by_uuid(&self, uuid: &str) -> Result<Instance> {
        self.get().instance_by_uuid(uuid).cloned()
    }

    #[inline(always)]
    pub fn clone_replicaset_by_name(&self, name: &str) -> Result<Replicaset> {
        self.get().replicaset_by_name(name).cloned()
    }

    #[inline(always)]
    pub fn clone_replicaset_by_uuid(&self, uuid: &str) -> Result<Replicaset> {
        self.get().replicaset_by_uuid(uuid).cloned()
    }

    /// Updates the instance record.
    ///
    /// This function should only be called from [`NodeImpl::handle_dml_entry`].
    ///
    /// [`NodeImpl::handle_dml_entry`]: crate::traft::node::NodeImpl::handle_dml_entry
    #[inline(always)]
    pub(crate) fn update_instance(&self, old: Option<Instance>, new: Option<Instance>) {
        if let Some(new) = &new {
            if self.my_instance_name.get().is_none() && new.raft_id == self.my_raft_id {
                self.my_instance_name
                    .set(new.name.to_string())
                    .expect("was empty");
                self.my_instance_uuid
                    .set(new.uuid.clone())
                    .expect("was empty");
                self.my_replicaset_name
                    .set(new.replicaset_name.to_string())
                    .expect("was empty");
                self.my_replicaset_uuid
                    .set(new.replicaset_uuid.clone())
                    .expect("was empty");
                self.my_tier_name.set(new.tier.clone()).expect("was empty");
            }
        }

        self.inner.borrow_mut().update_instance(old, new)
    }

    /// Updates the replicaset record.
    ///
    /// This function should only be called from [`NodeImpl::handle_dml_entry`].
    #[inline(always)]
    pub(crate) fn update_replicaset(&self, old: Option<Replicaset>, new: Option<Replicaset>) {
        self.inner.borrow_mut().update_replicaset(old, new)
    }

    /// Updates the tier record.
    ///
    /// This function should only be called from [`NodeImpl::handle_dml_entry`].
    #[inline(always)]
    pub(crate) fn update_tier(&self, old: Option<Tier>, new: Option<Tier>) {
        self.inner.borrow_mut().update_tier(old, new)
    }

    /// Updates the service route record.
    ///
    /// This function should only be called from [`NodeImpl::handle_dml_entry`].
    #[inline(always)]
    pub(crate) fn update_service_route(
        &self,
        old: Option<ServiceRouteItem>,
        new: Option<ServiceRouteItem>,
    ) {
        self.inner.borrow_mut().update_service_route(old, new)
    }
}

////////////////////////////////////////////////////////////////////////////////
// TopologyCacheMutable
////////////////////////////////////////////////////////////////////////////////

/// Stores topology info which may change during execution. Because the data is
/// volatile, access to it from separate fibers must be guarded by a [`NoYieldsRefCell`].
/// `fiber::Mutex` also works, but may lead to deadlocks and it's better to
/// just never hold such references across yields, which is enforced by NoYieldsRefCell.
#[derive(Default)]
pub struct TopologyCacheMutable {
    /// Raft id is always known and never changes.
    my_raft_id: RaftId,

    /// Info about the current instance.
    ///
    /// This may not be known for a short moment when the instance bootstraps a
    /// cluster after it initialized the raft node and before it handled the
    /// bootstrap entries.
    this_instance: OnceCell<Instance>,

    /// Info about the current instance's replicaset.
    ///
    /// This may not be known for a short moment when the instance joins the
    /// cluster after it initialied it's raft node and before it received the
    /// raft log entry/snapshot with the replicaset info. After that point this
    /// will never be `None` again.
    this_replicaset: OnceCell<Replicaset>,

    /// Info about the current instance's tier.
    ///
    /// It may not be known for a short moment but once it's known it's always known.
    this_tier: OnceCell<Tier>,

    /// FIXME: there's space for optimization here. We use `String` as key which
    /// requires additional memory allocation + cache misses. This would be
    /// improved if we used a `Uuid` type, but there's a problem with that,
    /// because we don't use `Uuid` type anywhere else which means we'll need to
    /// add conversions from String to Uuid, which cancels out some of the
    /// performance gain. We can't use `SmolStr` because it only stores up to
    /// 23 bytes in-place, while a stringified `Uuid` is 36 bytes. There's no
    /// good reason, why a small string can't store 36 bytes in-place but we'll
    /// have to implement our own. Thankfully it's going to be super fricken ez,
    /// we just have to do it..
    instances_by_name: HashMap<String, Instance>,
    instance_name_by_uuid: HashMap<String, String>,

    /// We store replicasets by uuid because in some places we need bucket_id
    /// based routing which only works via vshard at the moment, which only
    /// knows about replicaset uuid, so we must optimize the lookup via uuid.
    /// For instances there's no such need and we don't have any APIs which
    /// operate on instance uuids at the moment, so we optimize for lookup based
    /// on instance name.
    replicasets_by_uuid: HashMap<String, Replicaset>,
    replicaset_uuid_by_name: HashMap<String, String>,

    tiers_by_name: HashMap<String, Tier>,

    /// The meaning of the data is such:
    /// ```ignore
    /// HashMap<PluginName, HashMap<PluginVersion, HashMap<ServiceName, HashMap<InstanceName, IsPoisoned>>>>
    /// ```
    ///
    /// XXX: This monstrosity exists, because rust is such a great language.
    /// What we want is to have HashMap<(String, String, String, String), T>,
    /// but this would mean that when HashMap::get will have to take a
    /// &(String, String, String, String), which means we need to construct 4 strings
    /// every time we want to read the data. And passing a (&str, &str, &str, &str)
    /// will not compile. We need this code to be blazingly fast though, so we
    /// do this...
    #[allow(clippy::type_complexity)]
    service_routes: HashMap<String, HashMap<String, HashMap<String, HashMap<String, bool>>>>,
}

impl TopologyCacheMutable {
    pub fn load(storage: &Catalog, my_raft_id: RaftId) -> Result<Self> {
        let mut this_instance = None;
        let mut this_replicaset = None;
        let mut this_tier = None;

        let mut instances_by_name = HashMap::default();
        let mut instance_name_by_uuid = HashMap::default();
        let mut replicasets_by_uuid = HashMap::default();
        let mut replicaset_uuid_by_name = HashMap::default();
        let mut tiers_by_name = HashMap::default();
        let mut service_routes = HashMap::default();

        let instances = storage.instances.all_instances()?;
        for instance in instances {
            let instance_name = instance.name.to_string();

            if instance.raft_id == my_raft_id {
                this_instance = Some(instance.clone());
            }

            instance_name_by_uuid.insert(instance.uuid.clone(), instance_name.clone());
            instances_by_name.insert(instance_name, instance);
        }

        let replicasets = storage.replicasets.iter()?;
        for replicaset in replicasets {
            let replicaset_uuid = replicaset.uuid.clone();
            if let Some(instance) = &this_instance {
                if replicaset_uuid == instance.replicaset_uuid {
                    this_replicaset = Some(replicaset.clone());
                }
            }

            let replicaset_name = replicaset.name.to_string();
            replicaset_uuid_by_name.insert(replicaset_name, replicaset_uuid.clone());
            replicasets_by_uuid.insert(replicaset_uuid, replicaset);
        }

        let tiers = storage.tiers.iter()?;
        for tier in tiers {
            if let Some(instance) = &this_instance {
                if tier.name == instance.tier {
                    this_tier = Some(tier.clone());
                }
            }

            tiers_by_name.insert(tier.name.clone(), tier);
        }

        let items = storage.service_route_table.iter()?;
        for item in items {
            service_routes
                .entry(item.plugin_name)
                .or_insert_with(HashMap::default)
                .entry(item.plugin_version)
                .or_insert_with(HashMap::default)
                .entry(item.instance_name.into())
                .or_insert_with(HashMap::default)
                .insert(item.service_name, item.poison);
        }

        Ok(Self {
            my_raft_id,
            this_instance: once_cell_from_option(this_instance),
            this_replicaset: once_cell_from_option(this_replicaset),
            this_tier: once_cell_from_option(this_tier),
            instances_by_name,
            instance_name_by_uuid,
            replicasets_by_uuid,
            replicaset_uuid_by_name,
            tiers_by_name,
            service_routes,
        })
    }

    #[inline(always)]
    pub fn all_instances(&self) -> impl Iterator<Item = &Instance> {
        self.instances_by_name.values()
    }

    pub fn instance_by_name(&self, name: &str) -> Result<&Instance> {
        let this_instance = self
            .this_instance
            .get()
            .expect("should be known at this point");
        if this_instance.name == name {
            return Ok(this_instance);
        }

        let Some(instance) = self.instances_by_name.get(name) else {
            return Err(Error::NoSuchInstance(IdOfInstance::Name(name.into())));
        };

        Ok(instance)
    }

    pub fn instance_by_uuid(&self, uuid: &str) -> Result<&Instance> {
        let this_instance = self
            .this_instance
            .get()
            .expect("should be known at this point");
        if this_instance.uuid == uuid {
            return Ok(this_instance);
        }

        let Some(name) = self.instance_name_by_uuid.get(uuid) else {
            return Err(Error::NoSuchInstance(IdOfInstance::Uuid(uuid.into())));
        };

        self.instance_by_name(name)
    }

    #[inline(always)]
    pub fn all_replicasets(&self) -> impl Iterator<Item = &Replicaset> {
        self.replicasets_by_uuid.values()
    }

    pub fn replicaset_by_name(&self, name: &str) -> Result<&Replicaset> {
        if let Some(this_replicaset) = self.this_replicaset.get() {
            if this_replicaset.name == name {
                return Ok(this_replicaset);
            }
        }

        let Some(uuid) = self.replicaset_uuid_by_name.get(name) else {
            return Err(Error::NoSuchReplicaset {
                name: name.into(),
                id_is_uuid: false,
            });
        };

        self.replicaset_by_uuid(uuid)
    }

    pub fn replicaset_by_uuid(&self, uuid: &str) -> Result<&Replicaset> {
        if let Some(this_replicaset) = self.this_replicaset.get() {
            if this_replicaset.uuid == uuid {
                return Ok(this_replicaset);
            }
        }

        let Some(replicaset) = self.replicasets_by_uuid.get(uuid) else {
            return Err(Error::NoSuchReplicaset {
                name: uuid.into(),
                id_is_uuid: true,
            });
        };

        Ok(replicaset)
    }

    #[inline(always)]
    pub fn all_tiers(&self) -> impl Iterator<Item = &Tier> {
        self.tiers_by_name.values()
    }

    pub fn tier_by_name(&self, name: &str) -> Result<&Tier> {
        if let Some(this_tier) = self.this_tier.get() {
            if this_tier.name == name {
                return Ok(this_tier);
            }
        }

        let Some(tier) = self.tiers_by_name.get(name) else {
            return Err(Error::NoSuchTier(name.into()));
        };

        Ok(tier)
    }

    pub fn check_service_route(
        &self,
        plugin: &str,
        version: &str,
        service: &str,
        instance_name: &str,
    ) -> ServiceRouteCheck {
        let Some(is_poisoned) = self
            .service_routes
            .get(plugin)
            .and_then(|m| m.get(version))
            .and_then(|m| m.get(service))
            .and_then(|m| m.get(instance_name))
        else {
            return ServiceRouteCheck::ServiceNotEnabled;
        };

        if *is_poisoned {
            ServiceRouteCheck::RoutePoisoned
        } else {
            ServiceRouteCheck::Ok
        }
    }

    pub fn instances_running_service(
        &self,
        plugin: &str,
        version: &str,
        service: &str,
    ) -> impl Iterator<Item = &str> {
        // Is this code readable? Asking for a friend
        self.service_routes
            .get(plugin)
            .and_then(|m| m.get(version))
            .and_then(|m| m.get(service))
            .map(|m| m.iter())
            .into_iter()
            .flatten()
            .filter(|(_, &is_poisoned)| !is_poisoned)
            .map(|(i, _)| i.as_str())
    }

    fn update_instance(&mut self, old: Option<Instance>, new: Option<Instance>) {
        if let Some(new) = &new {
            if new.raft_id == self.my_raft_id {
                once_cell_replace(&mut self.this_instance, new.clone());
            }
        }

        match (old, new) {
            // Create new instance
            (None, Some(new)) => {
                let new_uuid = new.uuid.clone();
                let new_name = new.name.to_string();

                let old_cached_name = self
                    .instance_name_by_uuid
                    .insert(new_uuid, new_name.clone());
                debug_assert!(old_cached_name.is_none());

                let old_cached = self.instances_by_name.insert(new_name, new);
                debug_assert!(old_cached.is_none());
            }

            // Update instance
            (Some(old), Some(new)) => {
                // XXX: if the primary key changes, this logic must be updated
                debug_assert_eq!(old.name, new.name, "instance name is the primary key");

                if old != new {
                    let new_uuid = new.uuid.clone();
                    let new_name = new.name.to_string();

                    let same_uuid = (old.uuid == new_uuid);
                    if !same_uuid {
                        let old_cached_name = self
                            .instance_name_by_uuid
                            .insert(new_uuid, new_name.clone());
                        debug_assert!(old_cached_name.is_none());

                        // The new instance replaces the old one, so the old
                        // uuid -> name mapping should be removed
                        let old_cached = self.instance_name_by_uuid.remove(&old.uuid);
                        debug_assert!(old_cached.is_some());
                    }

                    let old_cached = self.instances_by_name.insert(new_name, new);
                    debug_assert_eq!(old_cached, Some(old));
                }
            }

            // Delete instance
            (Some(old), None) => {
                let old_cached_name = self.instance_name_by_uuid.remove(&old.uuid);
                debug_assert_eq!(old_cached_name.as_deref(), Some(&*old.name));

                let old_cached = self.instances_by_name.remove(&*old.name);
                debug_assert_eq!(old_cached, Some(old));
            }

            (None, None) => unreachable!(),
        }
    }

    fn update_replicaset(&mut self, old: Option<Replicaset>, new: Option<Replicaset>) {
        match (&new, self.this_instance.get()) {
            (Some(new), Some(this_instance)) if new.name == this_instance.replicaset_name => {
                once_cell_replace(&mut self.this_replicaset, new.clone());
            }
            _ => {}
        }

        match (old, new) {
            // Create new replicaset
            (None, Some(new)) => {
                let new_uuid = new.uuid.clone();
                let new_name = new.name.to_string();

                let old_cached_uuid = self
                    .replicaset_uuid_by_name
                    .insert(new_name, new_uuid.clone());
                debug_assert!(old_cached_uuid.is_none());

                let old_cached = self.replicasets_by_uuid.insert(new_uuid, new);
                debug_assert!(old_cached.is_none());
            }

            // Update replicaset
            (Some(old), Some(new)) => {
                // XXX: if the primary key changes, this logic must be updated
                debug_assert_eq!(old.name, new.name, "replicaset name is the primary key");

                if old != new {
                    let new_uuid = new.uuid.clone();
                    let new_name = new.name.to_string();

                    let same_uuid = (old.uuid == new_uuid);
                    if !same_uuid {
                        let old_cached_uuid = self
                            .replicaset_uuid_by_name
                            .insert(new_name, new_uuid.clone());
                        debug_assert_eq!(old_cached_uuid.as_ref(), Some(&old.uuid));

                        // The new replicaset replaces the old one, so that one
                        // should be removed. This is needed because our cache
                        // stores replicasets by `uuid` (primary key of sorts),
                        // while in storage the primary key is `name`. Maybe we
                        // should change the primary key in storage to `uuid` as well
                        let old_cached = self.replicasets_by_uuid.remove(&old.uuid);
                        debug_assert!(old_cached.is_some());
                    }

                    let old_cached = self.replicasets_by_uuid.insert(new_uuid, new);
                    debug_assert_eq!(old_cached, same_uuid.then_some(old));
                }
            }

            // Delete replicaset
            (Some(old), None) => {
                let old_cached_uuid = self.replicaset_uuid_by_name.remove(&*old.name);
                debug_assert_eq!(old_cached_uuid.as_ref(), Some(&old.uuid));

                let old_cached = self.replicasets_by_uuid.remove(&old.uuid);
                debug_assert_eq!(old_cached, Some(old));
            }

            (None, None) => unreachable!(),
        }
    }

    fn update_tier(&mut self, old: Option<Tier>, new: Option<Tier>) {
        if let Some(new) = new {
            let new_name = new.name.clone();

            if let Some(this_instance) = self.this_instance.get() {
                if new_name == this_instance.tier {
                    once_cell_replace(&mut self.this_tier, new.clone());
                }
            }

            // Create new tier or update old tier
            let old_cached = self.tiers_by_name.insert(new_name, new);
            debug_assert_eq!(old_cached, old);
        } else if let Some(old) = old {
            // Delete tier
            let old_cached = self.tiers_by_name.remove(&old.name);
            debug_assert_eq!(old_cached.as_ref(), Some(&old));
        } else {
            unreachable!()
        }
    }

    fn update_service_route(
        &mut self,
        old: Option<ServiceRouteItem>,
        new: Option<ServiceRouteItem>,
    ) {
        if let Some(new) = new {
            // Create service route record or update an old one
            self.service_routes
                .entry(new.plugin_name)
                .or_default()
                .entry(new.plugin_version)
                .or_default()
                .entry(new.service_name)
                .or_default()
                .insert(new.instance_name.into(), new.poison);
        } else if let Some(old) = old {
            // Delete a service route record
            let _: Option<bool> = self
                .service_routes
                .get_mut(&old.plugin_name)
                .and_then(|m| m.get_mut(&old.plugin_version))
                .and_then(|m| m.get_mut(&old.service_name))
                .and_then(|m| m.remove(&*old.instance_name));
        } else {
            unreachable!()
        }
    }
}

/// Value of this type is returned from [`TopologyCacheMutable::check_service_route`].
pub enum ServiceRouteCheck {
    Ok,
    RoutePoisoned,
    ServiceNotEnabled,
}

#[inline(always)]
fn once_cell_from_option<T>(v: Option<T>) -> OnceCell<T>
where
    T: std::fmt::Debug,
{
    let res = OnceCell::new();
    if let Some(v) = v {
        res.set(v).expect("was empty");
    }
    res
}

#[inline(always)]
fn once_cell_replace<T>(cell: &mut OnceCell<T>, new: T)
where
    T: std::fmt::Debug,
{
    if let Some(cell) = cell.get_mut() {
        *cell = new;
    } else {
        cell.set(new).expect("was empty");
    }
}
