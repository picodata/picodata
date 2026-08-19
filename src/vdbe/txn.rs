use super::ffi::*;
use super::insert;
use super::{alloc_zeroed, reserve, reserve_one};
use ::sql::executor::engine::{BlockQuery, BlockRuntimeHook, VersionMap};
use ::sql::executor::vdbe::{SqlError, SqlStmt};
use ::sql::ir::node::{BlockEntries, BlockEntriesMut, BlockEntryKind, BlockStatement};
use ::sql::ir::operator::ConflictUpdateValue;
use smol_str::SmolStr;
use std::collections::HashMap;
use std::pin::Pin;

/// A parsed SQL parameter name from a compiled VDBE's OP_Variable opcode.
///
/// All parameter names start with `:`. The character after the colon
/// determines the kind:
/// - `:N`    (digit)  — positional query parameter
/// - `:name` (letter) — LET variable reference
#[derive(Debug)]
enum ParamName<'a> {
    /// `:N` — positional query parameter. Holds the 1-based index N.
    Positional(i32),
    /// `:name` — LET variable reference. Holds the full `":name"` string
    /// (colon included) for direct use as a HashMap key.
    Let(&'a str),
}

impl<'a> ParamName<'a> {
    fn parse(s: &'a str) -> Self {
        let rest = s.strip_prefix(':').expect("param name must start with ':'");
        if rest.starts_with(|c: char| c.is_ascii_digit()) {
            Self::Positional(rest.parse().expect("invalid :N param index"))
        } else {
            Self::Let(s)
        }
    }
}

fn hook_max_positional_parameter_index(hook: &BlockRuntimeHook) -> i32 {
    match hook {
        BlockRuntimeHook::IdxInsertOnConflictDoUpdate { update, .. } => update
            .items
            .iter()
            .filter_map(|item| match item.value {
                ConflictUpdateValue::Param { index, .. } => Some(i32::from(index)),
                ConflictUpdateValue::Const(_) | ConflictUpdateValue::LetVar { .. } => None,
            })
            .max()
            .unwrap_or(0),
    }
}

/// A compiled subprogram ready for merging into a block VDBE program.
struct CompiledSubprogram {
    subprogram: Box<SubProgram>,
    n_res_column: u16,
    hooks: Vec<BlockRuntimeHook>,
    // OP_Variable ops in `subprogram.aOp` reference parameter name strings
    // stored in this VList via P4_STATIC p4.z pointers. We hold on to it
    // until those ops are patched (which nulls p4.z), then free it in Drop.
    p_v_list: *mut VList,
}

impl Drop for CompiledSubprogram {
    fn drop(&mut self) {
        if !self.p_v_list.is_null() {
            unsafe { sql_xfree(self.p_v_list.cast()) };
        }
    }
}

impl CompiledSubprogram {
    /// Compile a block query into a subprogram.
    fn compile(query: &BlockQuery) -> Result<Self, String> {
        let stmt = SqlStmt::compile(&query.pattern).map_err(|e| e.to_string())?;
        let vdbe = unsafe { &mut *stmt.as_ptr().cast::<Vdbe>() };

        let mut subprogram = unsafe { alloc_zeroed::<SubProgram>() };
        subprogram.aOp = vdbe.aOp;
        subprogram.nOp = vdbe.nOp;
        subprogram.nMem = vdbe.nMem;
        subprogram.nCsr = vdbe.nCursor;

        let n_res_column = vdbe.nResColumn;

        // Ownership of aOp now belongs to the subprogram; null it out on the
        // source VDBE so its Drop doesn't free the opcode array a second time.
        vdbe.aOp = std::ptr::null_mut();
        vdbe.nOp = 0;
        // aOp's OP_Variable opcodes point into pVList via P4_STATIC p4.z, so
        // we must keep pVList alive past the stmt's Drop. Move it out here
        // and release it later in `CompiledSubprogram::drop`.
        let p_v_list = std::mem::replace(&mut vdbe.pVList, std::ptr::null_mut());
        // sqlVdbeMakeReady leaves the VDBE in MAGIC_INIT, and in that state
        // sqlVdbeClearObject skips freeing pFree and aVar (the allocation
        // backing aMem and aVar). Switching to MAGIC_RESET lets the standard
        // finalize path release them.
        vdbe.magic = VDBE_MAGIC_RESET as _;

        Ok(Self {
            subprogram,
            n_res_column,
            hooks: query.hooks.clone(),
            p_v_list,
        })
    }

    fn ops(&self) -> &[VdbeOp] {
        // SAFETY: `compile` moves the prepared VDBE opcode array into this
        // subprogram, and `nOp` is the number of initialized entries in it.
        unsafe { std::slice::from_raw_parts(self.subprogram.aOp, self.subprogram.nOp as usize) }
    }

    fn ops_mut(&mut self) -> &mut [VdbeOp] {
        // SAFETY: same as `ops`; `&mut self` guarantees exclusive access to the
        // opcode array while the mutable slice is alive.
        unsafe { std::slice::from_raw_parts_mut(self.subprogram.aOp, self.subprogram.nOp as usize) }
    }

    fn attach_vdbe_hooks(
        &mut self,
        defined_let_vars: &HashMap<SmolStr, i32>,
        query_param_offset: i32,
        table_versions: &VersionMap,
    ) -> Result<Vec<Pin<Box<insert::VdbeIdxInsertHookHandle>>>, SqlError> {
        let mut hooks = Vec::new();
        for hook in std::mem::take(&mut self.hooks) {
            hooks.push(insert::attach_idx_insert_hook(
                self.ops_mut(),
                &hook,
                defined_let_vars,
                query_param_offset,
                table_versions,
            )?);
        }
        Ok(hooks)
    }

    /// Patch this subprogram's OP_Variable slots at `cumulative_offset`,
    /// advance the offset by the max local `:N` index, and attach its runtime
    /// hooks — the per-statement step of the block patch loop.
    fn patch_and_attach(
        &mut self,
        cumulative_offset: &mut i32,
        hooks: &mut Vec<Pin<Box<insert::VdbeIdxInsertHookHandle>>>,
        all_let_vars: &HashMap<SmolStr, i32>,
        defined_let_vars: &HashMap<SmolStr, i32>,
        table_versions: &VersionMap,
    ) -> Result<(), SqlError> {
        let query_param_offset = *cumulative_offset;
        *cumulative_offset +=
            self.patch_variable_slots(query_param_offset, all_let_vars, defined_let_vars)?;
        hooks.append(&mut self.attach_vdbe_hooks(
            defined_let_vars,
            query_param_offset,
            table_versions,
        )?);
        Ok(())
    }

    /// Consume the struct and return the owned subprogram. `Drop` runs after
    /// this returns and releases the parameter-name VList, so callers must
    /// only invoke this after `patch_variable_slots` has nulled every
    /// OP_Variable `p4.z` that points into it.
    fn into_subprogram(mut self) -> Box<SubProgram> {
        // Swap out the Box and let Drop free `p_v_list`. The dummy has no
        // destructor (SubProgram is a plain C struct), so dropping it is a
        // bare deallocation.
        std::mem::replace(&mut self.subprogram, unsafe { alloc_zeroed() })
    }

    /// Max positional (`:N`) parameter index referenced by attached hooks.
    fn hooks_max_param_index(&self) -> i32 {
        self.hooks
            .iter()
            .map(hook_max_positional_parameter_index)
            .max()
            .unwrap_or(0)
    }

    /// Count the number of positional parameters in the subprogram.
    // FIXME: this looks more like max_positional_parameter_index
    fn count_positional_parameters(&self) -> i32 {
        let mut count = 0;
        for op in self.ops() {
            if op.opcode as i32 == OP_Variable {
                let name = unsafe { Self::op_var_name(op) }.expect("OP_Variable without p4.z name");
                if let ParamName::Positional(idx) = ParamName::parse(name) {
                    count = std::cmp::max(count, idx);
                }
            }
        }
        count.max(self.hooks_max_param_index())
    }

    /// Patch all OP_Variable opcodes, resolving each to its global aVar slot.
    ///
    /// - `Positional(N)` → global slot `query_param_offset + N`
    /// - `Let(":name")`  → slot from `defined_let_vars[":name"]`
    ///
    /// Returns `Err` if a LET variable reference is unknown or used before
    /// definition. Returns the max `:N` index seen (used as offset increment).
    fn patch_variable_slots(
        &mut self,
        query_param_offset: i32,
        all_let_vars: &HashMap<SmolStr, i32>,
        defined_let_vars: &HashMap<SmolStr, i32>,
    ) -> Result<i32, String> {
        let mut max_local_n = 0i32;

        for op in self.ops_mut() {
            if op.opcode as i32 == OP_Variable {
                let name = unsafe { Self::op_var_name(op) }.expect("OP_Variable without p4.z name");
                op.p1 = match ParamName::parse(name) {
                    ParamName::Positional(n) => {
                        max_local_n = max_local_n.max(n);
                        query_param_offset + n
                    }
                    ParamName::Let(name) => {
                        if !all_let_vars.contains_key(name) {
                            return Err(format!("unknown variable: {name}"));
                        }
                        match defined_let_vars.get(name) {
                            Some(&slot) => slot,
                            None => {
                                return Err(format!("use of variable {name} before its assignment"))
                            }
                        }
                    }
                };
                op.p4type = P4_NOTUSED as _;
                op.p4.z = std::ptr::null_mut();
            }
        }
        Ok(max_local_n.max(self.hooks_max_param_index()))
    }

    /// Patch OP_ResultRow to write the scalar result to `aVar[var_slot - 1]`
    /// instead of outputting to the port.
    ///
    /// TODO: enforce that LET/IF-cond statements produce at most 1 row.
    fn patch_let_result_row(&mut self, var_slot: i32) {
        for op in self.ops_mut() {
            if op.opcode as i32 == OP_ResultRow {
                op.p3 = var_slot;
            }
        }
    }

    /// Returns the name stored in `op.p4.z`, or `None` if null.
    ///
    /// # Safety
    /// p4.z must be either null or a valid C string pointer.
    unsafe fn op_var_name(op: &VdbeOp) -> Option<&str> {
        if op.p4.z.is_null() {
            None
        } else {
            std::ffi::CStr::from_ptr(op.p4.z).to_str().ok()
        }
    }
}

/// Assign aVar slots to LET variables and IF condition results.
///
/// One slot per LET variable *name*, so a reassignment reuses its slot -- and
/// so do two variables that merely happen to share a name because their scopes
/// do not overlap (sibling IF bodies, say). The frontend only ever resolves a
/// reference to a binding assigned earlier in this same execution order, with
/// no other assignment to the name in between, so a shared slot always holds
/// the value the reference was resolved against.
///
/// Each IF gets one anonymous slot for its condition boolean.
///
/// Slots are handed out in execution order, at any nesting depth, so the
/// consumers below can re-walk the block with the same iterator and line their
/// statements up with these slots one-to-one.
///
/// Returns `(let_vars, if_cond_slots, total_var_slots)`.
fn build_var_slots<S>(
    stmts: &[BlockStatement<S>],
    base_slot: i32,
) -> (HashMap<SmolStr, i32>, Vec<i32>, i32) {
    let mut let_vars: HashMap<SmolStr, i32> = HashMap::new();
    let mut if_cond_slots: Vec<i32> = Vec::new();
    let mut next = base_slot + 1;

    for entry in BlockEntries::new(stmts) {
        match entry.location.kind {
            BlockEntryKind::Let { var, .. } => {
                let_vars.entry(var).or_insert_with(|| {
                    let slot = next;
                    next += 1;
                    slot
                });
            }
            BlockEntryKind::IfCondition => {
                if_cond_slots.push(next);
                next += 1;
            }
            BlockEntryKind::ReturnQuery | BlockEntryKind::Query => {}
        }
    }

    (let_vars, if_cond_slots, next - 1)
}

/// Emit one statement list into `vdbe`, recursing into IF bodies.
///
/// For plain items: one OP_Program each.
/// For `If` items: condition OP_Program, OP_Variable + OP_IfNot in root,
/// then the body (which may open IFs of its own), then the OP_IfNot jump
/// target is patched past everything the body emitted. An ELSE body follows
/// that same layout: the THEN body ends with an OP_Goto over it, and OP_IfNot
/// lands on its first opcode instead of past the IF.
///
/// `if_cond_slots` is drained in step with the traversal -- [`build_var_slots`]
/// filled it walking the very same block in the very same order.
///
/// # Safety
/// Calls tarantool VDBE C APIs.
unsafe fn emit_block_stmts(
    vdbe: &mut Vdbe,
    parser: &mut Parse,
    stmts: Vec<BlockStatement<CompiledSubprogram>>,
    if_cond_slots: &mut impl Iterator<Item = i32>,
) {
    // Link the subprogram to the root VDBE (for cleanup) and call it.
    unsafe fn emit_program(vdbe: &mut Vdbe, parser: &mut Parse, cs: CompiledSubprogram) {
        let first_arg_cell = 0;
        let exception_jump_addr = 0;

        let state_cell = reserve_one(&mut parser.nMem);
        let sp_ptr = Box::into_raw(cs.into_subprogram());
        sqlVdbeLinkSubProgram(vdbe, sp_ptr);
        sqlVdbeAddOp!(
            vdbe, OP_Program,
            first_arg_cell, exception_jump_addr, state_cell,
            sp_ptr.cast() => P4_SUBPROGRAM,
        );
    }

    for stmt in stmts {
        match stmt {
            BlockStatement::Let { query: cs, .. }
            | BlockStatement::ReturnQuery(cs)
            | BlockStatement::Query(cs) => emit_program(vdbe, parser, cs),
            BlockStatement::If {
                cond,
                body,
                else_body,
            } => {
                let cond_slot = if_cond_slots
                    .next()
                    .expect("build_var_slots reserves a slot per IF condition");

                // Condition subprogram writes boolean to aVar[cond_slot - 1].
                emit_program(vdbe, parser, cond);

                let cond_reg = reserve_one(&mut parser.nMem);
                sqlVdbeAddOp!(vdbe, OP_Variable, cond_slot, cond_reg, 1);

                // Skip body if condition is false or NULL (p3=1 → jump on null).
                let ifnot_addr = sqlVdbeAddOp!(vdbe, OP_IfNot, cond_reg, 0, 1);

                emit_block_stmts(vdbe, parser, body, if_cond_slots);

                if else_body.is_empty() {
                    // Patch OP_IfNot to jump to the first opcode past the body.
                    (*vdbe.aOp.add(ifnot_addr as usize)).p2 = vdbe.nOp;
                } else {
                    // Having run the THEN body, jump over the ELSE one.
                    let goto_addr = sqlVdbeAddOp!(vdbe, OP_Goto, 0, 0);

                    // A false condition lands here, on the ELSE body.
                    (*vdbe.aOp.add(ifnot_addr as usize)).p2 = vdbe.nOp;

                    emit_block_stmts(vdbe, parser, else_body, if_cond_slots);

                    // Patch OP_Goto to jump past everything the ELSE emitted.
                    (*vdbe.aOp.add(goto_addr as usize)).p2 = vdbe.nOp;
                }
            }
        }
    }
}

/// Assemble compiled statements into a single root VDBE program.
///
/// # Safety
/// Calls tarantool VDBE C APIs.
unsafe fn assemble_block_vdbe(
    compiled: Vec<BlockStatement<CompiledSubprogram>>,
    if_cond_slots: &[i32],
    n_res_column: u16,
    n_mem: i32,
    n_var: i32,
) -> *mut Vdbe {
    with_parser(|parser| {
        let vdbe = &mut *sqlGetVdbe(parser);
        // Allocates `vdbe.metadata` of length `n_res_column` and stores
        // `nResColumn` on the VDBE. `metadata` is read by `sql_stmt_est_size`
        // and other introspection paths, so it must be a real allocation
        // even when we don't fill in column names.
        sqlVdbeSetNumCols(vdbe, n_res_column as i32);
        vdbe.nVar = n_var;
        parser.nVar = n_var;
        reserve(&mut parser.nMem, n_mem);

        emit_block_stmts(vdbe, parser, compiled, &mut if_cond_slots.iter().copied());

        sqlVdbeAddOp!(vdbe, OP_Halt);
        // `sql_stmt_est_size` and `sql_stmt_query_str` both read `zSql`, so it
        // must be non-null. The contents are only used for diagnostics in our
        // case — assembled blocks recompile from stored block statements.
        let placeholder = c"<assembled block>";
        sqlVdbeSetSql(vdbe, placeholder.as_ptr(), placeholder.count_bytes() as i32);
        sqlVdbeMakeReady(vdbe, parser);
        vdbe as *mut Vdbe
    })
}

/// Compile, patch, and assemble a block of statements into a VDBE program.
///
/// TODO: enforce that each statement (LET, RETURN, IF cond) produces at most 1 row.
pub(crate) fn compile_transactional_block(
    stmts: &[BlockStatement<BlockQuery>],
    table_versions: &VersionMap,
) -> Result<SqlStmt, SqlError> {
    fn compile_stmt(
        stmt: &BlockStatement<BlockQuery>,
    ) -> Result<BlockStatement<CompiledSubprogram>, String> {
        Ok(match stmt {
            BlockStatement::Let {
                var,
                query,
                is_used,
            } => BlockStatement::Let {
                var: var.clone(),
                query: CompiledSubprogram::compile(query)?,
                is_used: *is_used,
            },
            BlockStatement::ReturnQuery(query) => {
                BlockStatement::ReturnQuery(CompiledSubprogram::compile(query)?)
            }
            BlockStatement::Query(query) => {
                BlockStatement::Query(CompiledSubprogram::compile(query)?)
            }
            BlockStatement::If {
                cond,
                body,
                else_body,
            } => BlockStatement::If {
                cond: CompiledSubprogram::compile(cond)?,
                body: body
                    .iter()
                    .map(compile_stmt)
                    .collect::<Result<Vec<_>, String>>()?,
                else_body: else_body
                    .iter()
                    .map(compile_stmt)
                    .collect::<Result<Vec<_>, String>>()?,
            },
        })
    }

    let mut hooks = Vec::new();
    let mut compiled = stmts
        .iter()
        .map(compile_stmt)
        .collect::<Result<Vec<_>, String>>()?;

    let total_number_of_positional_params: i32 = BlockEntries::new(&compiled)
        .map(|entry| entry.query.count_positional_parameters())
        .sum();

    // Allocate aVar slots for LET variables and IF condition results.
    let (let_var_slots, if_cond_slots, total_var_slots) =
        build_var_slots(&compiled, total_number_of_positional_params);

    // Patch OP_ResultRow: LET -> named slot, IF cond -> anonymous slot.
    let mut if_cond_slots_iter = if_cond_slots.iter().copied();
    for entry in BlockEntriesMut::new(&mut compiled) {
        match entry.location.kind {
            BlockEntryKind::Let { var, .. } => {
                let slot = let_var_slots[var.as_str()];
                entry.query.patch_let_result_row(slot);
            }
            BlockEntryKind::IfCondition => {
                let slot = if_cond_slots_iter.next().unwrap();
                entry.query.patch_let_result_row(slot);
            }
            _ => {}
        }
    }

    // Patch OP_Variable slots in execution order, tracking defined LET vars
    // to catch forward references and unknown variable names.
    //
    // `defined_let_vars` only grows: a LET inside an IF body stays "defined"
    // past `END IF`, and nothing here notices that a false condition leaves
    // its slot NULL. So this is a check against a name that no statement
    // assigns before this one *anywhere* in the block, not a scope check --
    // IF scoping is settled in the frontend, which rejects a reference to a
    // variable whose body has ended before it ever reaches us.
    let mut cumulative_offset = 0i32;
    let mut defined_let_vars: HashMap<SmolStr, i32> = HashMap::new();
    for entry in BlockEntriesMut::new(&mut compiled) {
        let declared_var = match &entry.location.kind {
            BlockEntryKind::Let { var, .. } => Some(var.clone()),
            _ => None,
        };
        entry.query.patch_and_attach(
            &mut cumulative_offset,
            &mut hooks,
            &let_var_slots,
            &defined_let_vars,
            table_versions,
        )?;
        // A LET only becomes visible to the statements that follow it.
        if let Some(var) = declared_var {
            let slot = let_var_slots[var.as_str()];
            defined_let_vars.insert(var, slot);
        }
    }

    // All `RETURN QUERY` statements must agree on the number of result columns,
    // however deeply they are nested. `LET` and `IF cond` write to aVar; plain
    // `Query` is DML.
    let n_res_column = {
        let mut n: Option<u16> = None;
        let return_queries = BlockEntries::new(&compiled)
            .filter(|entry| entry.location.kind == BlockEntryKind::ReturnQuery);
        for entry in return_queries {
            let n_res_column = entry.query.n_res_column;
            match n {
                None => n = Some(n_res_column),
                Some(expected) if expected != n_res_column => {
                    return Err(format!(
                        "all result statements must return the same number of columns \
                         (expected {expected}, got {n_res_column})"
                    )
                    .into())
                }
                Some(_) => {}
            }
        }
        n.unwrap_or(1)
    };

    let max_n_mem = BlockEntries::new(&compiled)
        .map(|entry| entry.query.subprogram.nMem)
        .max()
        .unwrap_or(1);

    let vdbe = unsafe {
        assemble_block_vdbe(
            compiled,
            &if_cond_slots,
            n_res_column,
            max_n_mem,
            total_var_slots,
        )
    };
    // SAFETY: `assemble_block_vdbe` produces a freshly-prepared VDBE owned by us.
    // The statements and this routine let the VDBE re-assemble itself when its
    // schema version goes stale (any DDL bumps the global SQL schema version).
    let mut stmt = unsafe {
        SqlStmt::new_transactional(
            vdbe.cast(),
            stmts.to_vec(),
            table_versions.clone(),
            compile_transactional_block,
        )
    };
    if !hooks.is_empty() {
        stmt.add_owned_payload(Box::new(insert::VdbeIdxInsertHookHandles::new(hooks)));
    }
    Ok(stmt)
}
