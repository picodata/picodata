use serde::Serialize;
use smol_str::{format_smolstr, ToSmolStr};

use crate::errors::{Entity, SbroadError};

use super::{
    AlterUser, CreateRole, CreateUser, DropRole, DropUser, GrantPrivilege, NodeAligned,
    RevokePrivilege,
};

#[allow(clippy::module_name_repetitions)]
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub enum AclOwned {
    DropRole(DropRole),
    DropUser(DropUser),
    CreateRole(CreateRole),
    CreateUser(CreateUser),
    AlterUser(AlterUser),
    GrantPrivilege(GrantPrivilege),
    RevokePrivilege(RevokePrivilege),
}

impl AclOwned {
    /// Return ACL node timeout.
    ///
    /// # Errors
    /// - timeout parsing error
    pub fn timeout(&self) -> Result<f64, SbroadError> {
        match self {
            AclOwned::DropRole(DropRole { ref timeout, .. })
            | AclOwned::DropUser(DropUser { ref timeout, .. })
            | AclOwned::CreateRole(CreateRole { ref timeout, .. })
            | AclOwned::AlterUser(AlterUser { ref timeout, .. })
            | AclOwned::CreateUser(CreateUser { ref timeout, .. })
            | AclOwned::RevokePrivilege(RevokePrivilege { ref timeout, .. })
            | AclOwned::GrantPrivilege(GrantPrivilege { ref timeout, .. }) => timeout,
        }
        .to_smolstr()
        .parse()
        .map_err(|e| {
            SbroadError::Invalid(
                Entity::SpaceMetadata,
                Some(format_smolstr!("timeout parsing error {e:?}")),
            )
        })
    }
}

impl From<AclOwned> for NodeAligned {
    fn from(value: AclOwned) -> Self {
        match value {
            AclOwned::AlterUser(alter_user) => alter_user.into(),
            AclOwned::CreateRole(create_role) => create_role.into(),
            AclOwned::CreateUser(create_user) => create_user.into(),
            AclOwned::DropRole(drop_role) => drop_role.into(),
            AclOwned::DropUser(drop_user) => drop_user.into(),
            AclOwned::GrantPrivilege(grant_privilege) => grant_privilege.into(),
            AclOwned::RevokePrivilege(revoke_privilege) => revoke_privilege.into(),
        }
    }
}

#[allow(clippy::module_name_repetitions)]
#[derive(Debug, PartialEq, Eq, Serialize)]
pub enum MutAcl<'a> {
    DropRole(&'a mut DropRole),
    DropUser(&'a mut DropUser),
    CreateRole(&'a mut CreateRole),
    CreateUser(&'a mut CreateUser),
    AlterUser(&'a mut AlterUser),
    GrantPrivilege(&'a mut GrantPrivilege),
    RevokePrivilege(&'a mut RevokePrivilege),
}

#[allow(clippy::module_name_repetitions)]
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub enum Acl<'a> {
    DropRole(&'a DropRole),
    DropUser(&'a DropUser),
    CreateRole(&'a CreateRole),
    CreateUser(&'a CreateUser),
    AlterUser(&'a AlterUser),
    GrantPrivilege(&'a GrantPrivilege),
    RevokePrivilege(&'a RevokePrivilege),
}

impl Acl<'_> {
    /// Return ACL node timeout.
    ///
    /// # Errors
    /// - timeout parsing error
    pub fn timeout(&self) -> Result<f64, SbroadError> {
        match self {
            Acl::DropRole(DropRole { ref timeout, .. })
            | Acl::DropUser(DropUser { ref timeout, .. })
            | Acl::CreateRole(CreateRole { ref timeout, .. })
            | Acl::AlterUser(AlterUser { ref timeout, .. })
            | Acl::CreateUser(CreateUser { ref timeout, .. })
            | Acl::RevokePrivilege(RevokePrivilege { ref timeout, .. })
            | Acl::GrantPrivilege(GrantPrivilege { ref timeout, .. }) => timeout,
        }
        .to_smolstr()
        .parse()
        .map_err(|e| {
            SbroadError::Invalid(
                Entity::SpaceMetadata,
                Some(format_smolstr!("timeout parsing error {e:?}")),
            )
        })
    }

    #[must_use]
    pub fn get_acl_owned(&self) -> AclOwned {
        match self {
            Acl::AlterUser(alter_user) => AclOwned::AlterUser((*alter_user).clone()),
            Acl::CreateRole(create_role) => AclOwned::CreateRole((*create_role).clone()),
            Acl::CreateUser(create_user) => AclOwned::CreateUser((*create_user).clone()),
            Acl::DropRole(drop_role) => AclOwned::DropRole((*drop_role).clone()),
            Acl::DropUser(drop_user) => AclOwned::DropUser((*drop_user).clone()),
            Acl::GrantPrivilege(grant_privelege) => {
                AclOwned::GrantPrivilege((*grant_privelege).clone())
            }
            Acl::RevokePrivilege(revoke_privelege) => {
                AclOwned::RevokePrivilege((*revoke_privelege).clone())
            }
        }
    }
}
