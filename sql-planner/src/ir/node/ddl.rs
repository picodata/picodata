use super::{
    AlterSystem, AlterTable, Backup, CreateIndex, CreateProc, CreateTable, DropIndex, DropProc,
    DropTable, NodeAligned, RenameRoutine, SetParam, SetTransaction, TruncateTable,
};
use crate::errors::{Entity, SbroadError};
use crate::ir::Node32;
use serde::Serialize;
use smol_str::{format_smolstr, ToSmolStr};

#[allow(clippy::module_name_repetitions)]
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub enum DdlOwned {
    CreateTable(CreateTable),
    DropTable(DropTable),
    AlterTable(AlterTable),
    TruncateTable(TruncateTable),
    CreateProc(CreateProc),
    DropProc(DropProc),
    RenameRoutine(RenameRoutine),
    AlterSystem(AlterSystem),
    CreateIndex(CreateIndex),
    DropIndex(DropIndex),
    CreateSchema,
    DropSchema,
    SetParam(SetParam),
    SetTransaction(SetTransaction),
    Backup(Backup),
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
            | DdlOwned::TruncateTable(TruncateTable { ref timeout, .. })
            | DdlOwned::Backup(Backup { ref timeout, .. })
            | DdlOwned::AlterTable(AlterTable { ref timeout, .. })
            | DdlOwned::CreateIndex(CreateIndex { ref timeout, .. })
            | DdlOwned::DropIndex(DropIndex { ref timeout, .. })
            | DdlOwned::SetParam(SetParam { ref timeout, .. })
            | DdlOwned::SetTransaction(SetTransaction { ref timeout, .. })
            | DdlOwned::AlterSystem(AlterSystem { ref timeout, .. })
            | DdlOwned::CreateProc(CreateProc { ref timeout, .. })
            | DdlOwned::DropProc(DropProc { ref timeout, .. })
            | DdlOwned::RenameRoutine(RenameRoutine { ref timeout, .. }) => {
                timeout.to_smolstr().parse().map_err(|e| {
                    SbroadError::Invalid(
                        Entity::SpaceMetadata,
                        Some(format_smolstr!("timeout parsing error {e:?}")),
                    )
                })
            }
            DdlOwned::CreateSchema | DdlOwned::DropSchema => Ok(0.0),
        }
    }

    /// Drop operations removes existing object from schema,
    /// so it's safe to execute drop operation on each instance even
    /// in heterogeneous clusters.
    pub fn is_drop_operation(&self) -> bool {
        match self {
            DdlOwned::CreateTable(_)
            | DdlOwned::AlterTable(_)
            | DdlOwned::TruncateTable(_)
            | DdlOwned::CreateProc(_)
            | DdlOwned::RenameRoutine(_)
            | DdlOwned::AlterSystem(_)
            | DdlOwned::CreateIndex(_)
            | DdlOwned::CreateSchema
            | DdlOwned::SetParam(_)
            | DdlOwned::Backup(_)
            | DdlOwned::SetTransaction(_) => false,

            DdlOwned::DropTable(_)
            | DdlOwned::DropProc(_)
            | DdlOwned::DropIndex(_)
            | DdlOwned::DropSchema => true,
        }
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
            | DdlOwned::TruncateTable(TruncateTable {
                wait_applied_globally,
                ..
            })
            | DdlOwned::Backup(Backup {
                wait_applied_globally,
                ..
            })
            | DdlOwned::AlterTable(AlterTable {
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
            DdlOwned::CreateSchema => Self::Node32(Node32::CreateSchema),
            DdlOwned::DropIndex(drop_index) => drop_index.into(),
            DdlOwned::DropProc(drop_proc) => drop_proc.into(),
            DdlOwned::DropTable(drop_table) => drop_table.into(),
            DdlOwned::AlterTable(alter_table) => alter_table.into(),
            DdlOwned::TruncateTable(truncate_table) => truncate_table.into(),
            DdlOwned::DropSchema => Self::Node32(Node32::DropSchema),
            DdlOwned::AlterSystem(alter_system) => alter_system.into(),
            DdlOwned::RenameRoutine(rename) => rename.into(),
            DdlOwned::SetParam(set_param) => set_param.into(),
            DdlOwned::SetTransaction(set_trans) => set_trans.into(),
            DdlOwned::Backup(backup) => backup.into(),
        }
    }
}

#[allow(clippy::module_name_repetitions)]
#[derive(Debug, PartialEq, Eq, Serialize)]
pub enum MutDdl<'a> {
    CreateTable(&'a mut CreateTable),
    DropTable(&'a mut DropTable),
    TruncateTable(&'a mut TruncateTable),
    AlterTable(&'a mut AlterTable),
    CreateProc(&'a mut CreateProc),
    DropProc(&'a mut DropProc),
    RenameRoutine(&'a mut RenameRoutine),
    AlterSystem(&'a mut AlterSystem),
    CreateIndex(&'a mut CreateIndex),
    DropIndex(&'a mut DropIndex),
    CreateSchema,
    DropSchema,
    SetParam(&'a mut SetParam),
    SetTransaction(&'a mut SetTransaction),
    Backup(&'a mut Backup),
}

#[allow(clippy::module_name_repetitions)]
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub enum Ddl<'a> {
    CreateTable(&'a CreateTable),
    DropTable(&'a DropTable),
    TruncateTable(&'a TruncateTable),
    AlterTable(&'a AlterTable),
    CreateProc(&'a CreateProc),
    DropProc(&'a DropProc),
    RenameRoutine(&'a RenameRoutine),
    AlterSystem(&'a AlterSystem),
    CreateIndex(&'a CreateIndex),
    DropIndex(&'a DropIndex),
    CreateSchema,
    DropSchema,
    SetParam(&'a SetParam),
    SetTransaction(&'a SetTransaction),
    Backup(&'a Backup),
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
            | Ddl::TruncateTable(TruncateTable { ref timeout, .. })
            | Ddl::Backup(Backup { ref timeout, .. })
            | Ddl::AlterTable(AlterTable { ref timeout, .. })
            | Ddl::CreateIndex(CreateIndex { ref timeout, .. })
            | Ddl::DropIndex(DropIndex { ref timeout, .. })
            | Ddl::SetParam(SetParam { ref timeout, .. })
            | Ddl::SetTransaction(SetTransaction { ref timeout, .. })
            | Ddl::AlterSystem(AlterSystem { ref timeout, .. })
            | Ddl::CreateProc(CreateProc { ref timeout, .. })
            | Ddl::DropProc(DropProc { ref timeout, .. })
            | Ddl::RenameRoutine(RenameRoutine { ref timeout, .. }) => {
                timeout.to_smolstr().parse().map_err(|e| {
                    SbroadError::Invalid(
                        Entity::SpaceMetadata,
                        Some(format_smolstr!("timeout parsing error {e:?}")),
                    )
                })
            }
            Ddl::CreateSchema | Ddl::DropSchema => Ok(0.0),
        }
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
            | Ddl::TruncateTable(TruncateTable {
                wait_applied_globally,
                ..
            })
            | Ddl::Backup(Backup {
                wait_applied_globally,
                ..
            })
            | Ddl::AlterTable(AlterTable {
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
            Ddl::CreateSchema => DdlOwned::CreateSchema,
            Ddl::DropSchema => DdlOwned::DropSchema,
            Ddl::DropProc(drop_proc) => DdlOwned::DropProc((*drop_proc).clone()),
            Ddl::DropTable(drop_table) => DdlOwned::DropTable((*drop_table).clone()),
            Ddl::TruncateTable(truncate_table) => {
                DdlOwned::TruncateTable((*truncate_table).clone())
            }
            Ddl::Backup(backup) => DdlOwned::Backup((*backup).clone()),
            Ddl::AlterSystem(alter_system) => DdlOwned::AlterSystem((*alter_system).clone()),
            Ddl::RenameRoutine(rename) => DdlOwned::RenameRoutine((*rename).clone()),
            Ddl::SetParam(set_param) => DdlOwned::SetParam((*set_param).clone()),
            Ddl::SetTransaction(set_trans) => DdlOwned::SetTransaction((*set_trans).clone()),
            Ddl::AlterTable(alter_table) => DdlOwned::AlterTable((*alter_table).clone()),
        }
    }
}
