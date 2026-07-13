use super::{
    AlterSystemCluster, AlterSystemLocal, AlterTable, Backup, CreateIndex, CreateProc, CreateTable,
    DropIndex, DropProc, DropTable, NodeAligned, RenameIndex, RenameRoutine, SetParam,
    SetTransaction, TruncateTable,
};
use crate::ir::Node32;
use serde::Serialize;

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
    AlterSystemCluster(AlterSystemCluster),
    AlterSystemLocal(AlterSystemLocal),
    CreateIndex(CreateIndex),
    DropIndex(DropIndex),
    CreateSchema,
    DropSchema,
    SetParam(SetParam),
    SetTransaction(SetTransaction),
    Backup(Backup),
    RenameIndex(RenameIndex),
}

impl DdlOwned {
    /// Return DDL node timeout.
    pub fn timeout(&self) -> &crate::ir::options::Timeout {
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
            | DdlOwned::AlterSystemCluster(AlterSystemCluster { ref timeout, .. })
            | DdlOwned::CreateProc(CreateProc { ref timeout, .. })
            | DdlOwned::DropProc(DropProc { ref timeout, .. })
            | DdlOwned::RenameIndex(RenameIndex { ref timeout, .. })
            | DdlOwned::RenameRoutine(RenameRoutine { ref timeout, .. }) => timeout,

            // CREATE SCHEMA and DROP SCHEMA are stubs; they won't ever execute, so they don't need a timeout.
            // ALTER SYSTEM LOCAL is a fully local operation, so it won't have a timeout applied either.
            DdlOwned::CreateSchema | DdlOwned::DropSchema | DdlOwned::AlterSystemLocal(_) => {
                &crate::ir::options::Timeout::ZERO
            }
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
            | DdlOwned::RenameIndex(_)
            | DdlOwned::RenameRoutine(_)
            | DdlOwned::AlterSystemCluster(_)
            | DdlOwned::AlterSystemLocal(_)
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
            })
            | DdlOwned::AlterSystemCluster(AlterSystemCluster {
                wait_applied_globally,
                ..
            })
            | DdlOwned::RenameIndex(RenameIndex {
                wait_applied_globally,
                ..
            }) => *wait_applied_globally,
            DdlOwned::SetParam(_)
            | DdlOwned::SetTransaction(_)
            | DdlOwned::CreateSchema
            | DdlOwned::DropSchema
            | DdlOwned::AlterSystemLocal(_) => false,
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
            DdlOwned::AlterSystemCluster(alter_system) => alter_system.into(),
            DdlOwned::AlterSystemLocal(alter_system) => alter_system.into(),
            DdlOwned::RenameRoutine(rename) => rename.into(),
            DdlOwned::SetParam(set_param) => set_param.into(),
            DdlOwned::SetTransaction(set_trans) => set_trans.into(),
            DdlOwned::Backup(backup) => backup.into(),
            DdlOwned::RenameIndex(rename_index) => rename_index.into(),
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
    AlterSystemCluster(&'a mut AlterSystemCluster),
    AlterSystemLocal(&'a mut AlterSystemLocal),
    CreateIndex(&'a mut CreateIndex),
    DropIndex(&'a mut DropIndex),
    CreateSchema,
    DropSchema,
    SetParam(&'a mut SetParam),
    SetTransaction(&'a mut SetTransaction),
    Backup(&'a mut Backup),
    RenameIndex(&'a mut RenameIndex),
}

impl MutDdl<'_> {
    /// Return a mutable reference to the timeout, if present.
    pub fn timeout_mut(&mut self) -> Option<&mut crate::ir::options::Timeout> {
        match self {
            MutDdl::CreateTable(n) => Some(&mut n.timeout),
            MutDdl::DropTable(n) => Some(&mut n.timeout),
            MutDdl::TruncateTable(n) => Some(&mut n.timeout),
            MutDdl::AlterTable(n) => Some(&mut n.timeout),
            MutDdl::CreateProc(n) => Some(&mut n.timeout),
            MutDdl::DropProc(n) => Some(&mut n.timeout),
            MutDdl::RenameRoutine(n) => Some(&mut n.timeout),
            MutDdl::AlterSystemCluster(n) => Some(&mut n.timeout),
            MutDdl::CreateIndex(n) => Some(&mut n.timeout),
            MutDdl::DropIndex(n) => Some(&mut n.timeout),
            MutDdl::SetParam(n) => Some(&mut n.timeout),
            MutDdl::SetTransaction(n) => Some(&mut n.timeout),
            MutDdl::Backup(n) => Some(&mut n.timeout),
            MutDdl::RenameIndex(n) => Some(&mut n.timeout),
            MutDdl::CreateSchema | MutDdl::DropSchema | MutDdl::AlterSystemLocal(_) => None,
        }
    }
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
    AlterSystemCluster(&'a AlterSystemCluster),
    AlterSystemLocal(&'a AlterSystemLocal),
    CreateIndex(&'a CreateIndex),
    DropIndex(&'a DropIndex),
    CreateSchema,
    DropSchema,
    SetParam(&'a SetParam),
    SetTransaction(&'a SetTransaction),
    Backup(&'a Backup),
    RenameIndex(&'a RenameIndex),
}

impl Ddl<'_> {
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
            Ddl::AlterSystemCluster(alter_system) => {
                DdlOwned::AlterSystemCluster((*alter_system).clone())
            }
            Ddl::AlterSystemLocal(alter_system) => {
                DdlOwned::AlterSystemLocal((*alter_system).clone())
            }
            Ddl::RenameRoutine(rename) => DdlOwned::RenameRoutine((*rename).clone()),
            Ddl::SetParam(set_param) => DdlOwned::SetParam((*set_param).clone()),
            Ddl::SetTransaction(set_trans) => DdlOwned::SetTransaction((*set_trans).clone()),
            Ddl::AlterTable(alter_table) => DdlOwned::AlterTable((*alter_table).clone()),
            Ddl::RenameIndex(rename_index) => DdlOwned::RenameIndex((*rename_index).clone()),
        }
    }
}
