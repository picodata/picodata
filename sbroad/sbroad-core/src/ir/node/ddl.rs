use serde::Serialize;
use smol_str::{format_smolstr, ToSmolStr};

use crate::errors::{Entity, SbroadError};

use super::{
    AlterSystem, CreateIndex, CreateProc, CreateTable, DropIndex, DropProc, DropTable, NodeAligned,
    RenameRoutine, SetParam, SetTransaction,
};

#[allow(clippy::module_name_repetitions)]
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub enum DdlOwned {
    CreateTable(CreateTable),
    DropTable(DropTable),
    CreateProc(CreateProc),
    DropProc(DropProc),
    RenameRoutine(RenameRoutine),
    AlterSystem(AlterSystem),
    CreateIndex(CreateIndex),
    DropIndex(DropIndex),
    SetParam(SetParam),
    SetTransaction(SetTransaction),
}

impl DdlOwned {
    /// Return DDL node timeout.
    ///
    /// # Errors
    /// - timeout parsing error
    pub fn timeout(&self) -> Result<f64, SbroadError> {
        match self {
            DdlOwned::CreateTable(CreateTable { ref timeout, .. })
            | DdlOwned::DropTable(DropTable { ref timeout, .. })
            | DdlOwned::CreateIndex(CreateIndex { ref timeout, .. })
            | DdlOwned::DropIndex(DropIndex { ref timeout, .. })
            | DdlOwned::SetParam(SetParam { ref timeout, .. })
            | DdlOwned::SetTransaction(SetTransaction { ref timeout, .. })
            | DdlOwned::AlterSystem(AlterSystem { ref timeout, .. })
            | DdlOwned::CreateProc(CreateProc { ref timeout, .. })
            | DdlOwned::DropProc(DropProc { ref timeout, .. })
            | DdlOwned::RenameRoutine(RenameRoutine { ref timeout, .. }) => timeout,
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

    pub fn wait_applied_globally(&self) -> bool {
        match self {
            DdlOwned::CreateTable(CreateTable {
                wait_applied_globally,
                ..
            })
            | DdlOwned::DropTable(DropTable {
                wait_applied_globally,
                ..
            })
            | DdlOwned::CreateIndex(CreateIndex {
                wait_applied_globally,
                ..
            })
            | DdlOwned::DropIndex(DropIndex {
                wait_applied_globally,
                ..
            })
            | DdlOwned::CreateProc(CreateProc {
                wait_applied_globally,
                ..
            })
            | DdlOwned::DropProc(DropProc {
                wait_applied_globally,
                ..
            })
            | DdlOwned::RenameRoutine(RenameRoutine {
                wait_applied_globally,
                ..
            }) => *wait_applied_globally,
            _ => false,
        }
    }
}

impl From<DdlOwned> for NodeAligned {
    fn from(value: DdlOwned) -> Self {
        match value {
            DdlOwned::CreateIndex(create_index) => create_index.into(),
            DdlOwned::CreateProc(create_proc) => create_proc.into(),
            DdlOwned::CreateTable(create_table) => create_table.into(),
            DdlOwned::DropIndex(drop_index) => drop_index.into(),
            DdlOwned::DropProc(drop_proc) => drop_proc.into(),
            DdlOwned::DropTable(drop_table) => drop_table.into(),
            DdlOwned::AlterSystem(alter_system) => alter_system.into(),
            DdlOwned::RenameRoutine(rename) => rename.into(),
            DdlOwned::SetParam(set_param) => set_param.into(),
            DdlOwned::SetTransaction(set_trans) => set_trans.into(),
        }
    }
}

#[allow(clippy::module_name_repetitions)]
#[derive(Debug, PartialEq, Eq, Serialize)]
pub enum MutDdl<'a> {
    CreateTable(&'a mut CreateTable),
    DropTable(&'a mut DropTable),
    CreateProc(&'a mut CreateProc),
    DropProc(&'a mut DropProc),
    RenameRoutine(&'a mut RenameRoutine),
    AlterSystem(&'a mut AlterSystem),
    CreateIndex(&'a mut CreateIndex),
    DropIndex(&'a mut DropIndex),
    SetParam(&'a mut SetParam),
    SetTransaction(&'a mut SetTransaction),
}

#[allow(clippy::module_name_repetitions)]
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub enum Ddl<'a> {
    CreateTable(&'a CreateTable),
    DropTable(&'a DropTable),
    CreateProc(&'a CreateProc),
    DropProc(&'a DropProc),
    RenameRoutine(&'a RenameRoutine),
    AlterSystem(&'a AlterSystem),
    CreateIndex(&'a CreateIndex),
    DropIndex(&'a DropIndex),
    SetParam(&'a SetParam),
    SetTransaction(&'a SetTransaction),
}

impl Ddl<'_> {
    /// Return DDL node timeout.
    ///
    /// # Errors
    /// - timeout parsing error
    pub fn timeout(&self) -> Result<f64, SbroadError> {
        match self {
            Ddl::CreateTable(CreateTable { ref timeout, .. })
            | Ddl::DropTable(DropTable { ref timeout, .. })
            | Ddl::CreateIndex(CreateIndex { ref timeout, .. })
            | Ddl::DropIndex(DropIndex { ref timeout, .. })
            | Ddl::SetParam(SetParam { ref timeout, .. })
            | Ddl::SetTransaction(SetTransaction { ref timeout, .. })
            | Ddl::AlterSystem(AlterSystem { ref timeout, .. })
            | Ddl::CreateProc(CreateProc { ref timeout, .. })
            | Ddl::DropProc(DropProc { ref timeout, .. })
            | Ddl::RenameRoutine(RenameRoutine { ref timeout, .. }) => timeout,
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

    pub fn wait_applied_globally(&self) -> bool {
        match self {
            Ddl::CreateTable(CreateTable {
                wait_applied_globally,
                ..
            })
            | Ddl::DropTable(DropTable {
                wait_applied_globally,
                ..
            })
            | Ddl::CreateIndex(CreateIndex {
                wait_applied_globally,
                ..
            })
            | Ddl::DropIndex(DropIndex {
                wait_applied_globally,
                ..
            })
            | Ddl::CreateProc(CreateProc {
                wait_applied_globally,
                ..
            })
            | Ddl::DropProc(DropProc {
                wait_applied_globally,
                ..
            })
            | Ddl::RenameRoutine(RenameRoutine {
                wait_applied_globally,
                ..
            }) => *wait_applied_globally,
            _ => false,
        }
    }

    #[must_use]
    pub fn get_ddl_owned(&self) -> DdlOwned {
        match self {
            Ddl::CreateIndex(create_index) => DdlOwned::CreateIndex((*create_index).clone()),
            Ddl::CreateProc(create_proc) => DdlOwned::CreateProc((*create_proc).clone()),
            Ddl::CreateTable(create_table) => DdlOwned::CreateTable((*create_table).clone()),
            Ddl::DropIndex(drop_index) => DdlOwned::DropIndex((*drop_index).clone()),
            Ddl::DropProc(drop_proc) => DdlOwned::DropProc((*drop_proc).clone()),
            Ddl::DropTable(drop_table) => DdlOwned::DropTable((*drop_table).clone()),
            Ddl::AlterSystem(alter_system) => DdlOwned::AlterSystem((*alter_system).clone()),
            Ddl::RenameRoutine(rename) => DdlOwned::RenameRoutine((*rename).clone()),
            Ddl::SetParam(set_param) => DdlOwned::SetParam((*set_param).clone()),
            Ddl::SetTransaction(set_trans) => DdlOwned::SetTransaction((*set_trans).clone()),
        }
    }
}
