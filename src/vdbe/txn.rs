use super::ffi::*;
use super::{alloc_zeroed, reserve, reserve_one};
use ::sql::backend::sql::ir::PatternWithParams;
use ::sql::executor::vdbe::SqlStmt;
use ::sql::ir::node::BlockStatement;
use ::sql::ir::value::Value;
use std::collections::HashMap;
use tarantool::ffi::sql::PortC;

/// Compile and execute a block of statements into the given port.
pub fn execute_block_into_port(
    stmts: &[BlockStatement<PatternWithParams>],
    vdbe_max_steps: u64,
    port: &mut PortC,
) -> Result<(), String> {
    let params = collect_params(stmts);
    let vdbe = compile_and_assemble(stmts)?;
    // SAFETY: `compile_and_assemble` produces a freshly-prepared VDBE owned
    // by us; wrapping it in `SqlStmt` transfers ownership so it gets
    // finalized on drop.
    let mut stmt = unsafe { SqlStmt::from_raw(vdbe.cast()) };
    stmt.execute_once(&params, vdbe_max_steps, port)
        .map_err(|e| e.to_string())
}

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

/// A compiled subprogram ready for merging into a block VDBE program.
struct CompiledSubprogram {
    subprogram: Box<SubProgram>,
    n_res_column: u16,
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
    /// Compile a SQL query into a subprogram.
    fn compile(query: &str) -> Result<Self, String> {
        let stmt = SqlStmt::compile(query).map_err(|e| e.to_string())?;
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
            p_v_list,
        })
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

    /// Count the number of positional parameters in the subprogram.
    // FIXME: this looks more like max_positional_parameter_index
    fn count_positional_parameters(&self) -> i32 {
        let ops = unsafe {
            std::slice::from_raw_parts(self.subprogram.aOp, self.subprogram.nOp as usize)
        };
        let mut count = 0;
        for op in ops {
            if op.opcode as i32 == OP_Variable {
                let name = unsafe { Self::op_var_name(op) }.expect("OP_Variable without p4.z name");
                if let ParamName::Positional(idx) = ParamName::parse(name) {
                    count = std::cmp::max(count, idx);
                }
            }
        }

        count
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
        all_let_vars: &HashMap<String, i32>,
        defined_let_vars: &HashMap<String, i32>,
    ) -> Result<i32, String> {
        let ops = unsafe {
            std::slice::from_raw_parts_mut(self.subprogram.aOp, self.subprogram.nOp as usize)
        };
        let mut max_local_n = 0i32;

        for op in ops.iter_mut() {
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

        Ok(max_local_n)
    }

    /// Patch OP_ResultRow to write the scalar result to `aVar[var_slot - 1]`
    /// instead of outputting to the port.
    ///
    /// TODO: enforce that LET/IF-cond statements produce at most 1 row.
    fn patch_let_result_row(&mut self, var_slot: i32) {
        let ops = unsafe {
            std::slice::from_raw_parts_mut(self.subprogram.aOp, self.subprogram.nOp as usize)
        };
        for op in ops.iter_mut() {
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
/// LET variable slots are shared across reassignments (same name → same slot).
/// Each IF gets one anonymous slot for its condition boolean.
///
/// Returns `(let_vars, if_cond_slots, total_var_slots)`.
fn build_var_slots<S>(
    stmts: &[BlockStatement<S>],
    base_slot: i32,
) -> (HashMap<String, i32>, Vec<i32>, i32) {
    let mut let_vars: HashMap<String, i32> = HashMap::new();
    let mut if_cond_slots: Vec<i32> = Vec::new();
    let mut next = base_slot + 1;

    for stmt in stmts {
        match stmt {
            BlockStatement::Let { var, .. } => {
                let_vars.entry(var.clone()).or_insert_with(|| {
                    let slot = next;
                    next += 1;
                    slot
                });
            }
            BlockStatement::If { .. } => {
                if_cond_slots.push(next);
                next += 1;
            }
            BlockStatement::ReturnQuery(_) | BlockStatement::Query(_) => {}
        }
    }

    (let_vars, if_cond_slots, next - 1)
}

/// Assemble compiled statements into a single root VDBE program.
///
/// For `Subprogram` items: one OP_Program each.
/// For `If` items: condition OP_Program, OP_Variable + OP_IfNot in root,
/// body OP_Programs, then the OP_IfNot jump target is patched.
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

        let first_arg_cell = 0;
        let exception_jump_addr = 0;
        let mut if_idx = 0usize;

        for item in compiled {
            match item {
                BlockStatement::Let { query: cs, .. }
                | BlockStatement::ReturnQuery(cs)
                | BlockStatement::Query(cs) => {
                    let state_cell = reserve_one(&mut parser.nMem);
                    let sp_ptr = Box::into_raw(cs.into_subprogram());
                    sqlVdbeLinkSubProgram(vdbe, sp_ptr);
                    sqlVdbeAddOp!(
                        vdbe, OP_Program,
                        first_arg_cell, exception_jump_addr, state_cell,
                        sp_ptr.cast() => P4_SUBPROGRAM,
                    );
                }
                BlockStatement::If { cond, body } => {
                    let cond_slot = if_cond_slots[if_idx];
                    if_idx += 1;

                    // Condition subprogram writes boolean to aVar[cond_slot - 1].
                    let cond_state_cell = reserve_one(&mut parser.nMem);
                    let cond_ptr = Box::into_raw(cond.into_subprogram());
                    sqlVdbeLinkSubProgram(vdbe, cond_ptr);
                    sqlVdbeAddOp!(
                        vdbe, OP_Program,
                        first_arg_cell, exception_jump_addr, cond_state_cell,
                        cond_ptr.cast() => P4_SUBPROGRAM,
                    );

                    // Load condition result from aVar into a register.
                    let cond_reg = reserve_one(&mut parser.nMem);
                    sqlVdbeAddOp!(vdbe, OP_Variable, cond_slot, cond_reg, 1);

                    // Skip body if condition is false or NULL (p3=1 → jump on null).
                    let ifnot_addr = sqlVdbeAddOp!(vdbe, OP_IfNot, cond_reg, 0, 1);

                    // Body subprograms — all linked to the root VDBE for cleanup.
                    for body_cs in body {
                        let state_cell = reserve_one(&mut parser.nMem);
                        let body_ptr = Box::into_raw(body_cs.into_subprogram());
                        sqlVdbeLinkSubProgram(vdbe, body_ptr);
                        sqlVdbeAddOp!(
                            vdbe, OP_Program,
                            first_arg_cell, exception_jump_addr, state_cell,
                            body_ptr.cast() => P4_SUBPROGRAM,
                        );
                    }

                    // Patch OP_IfNot to jump to the first opcode past the body.
                    (*vdbe.aOp.add(ifnot_addr as usize)).p2 = vdbe.nOp;
                }
            }
        }

        sqlVdbeAddOp!(vdbe, OP_Halt);
        // `sql_stmt_est_size` and `sql_stmt_query_str` both read `zSql`, so it
        // must be non-null. The contents are only used for diagnostics in our
        // case — assembled blocks don't go through stale-schema recompile.
        let placeholder = c"<assembled block>";
        sqlVdbeSetSql(vdbe, placeholder.as_ptr(), placeholder.count_bytes() as i32);
        sqlVdbeMakeReady(vdbe, parser);
        vdbe as *mut Vdbe
    })
}

///Compile, patch, and assemble a block of statements into a VDBE program.
///
/// TODO: enforce that each statement (LET, RETURN, IF cond) produces at most 1 row.
fn compile_and_assemble(
    // FIXME: Consider passing stmts as Vec<BlockStatement<String>.
    // One way to to this is to return a vec of statements and a merged list of params
    // (Vec<BlockStatement<String>, Vec<Params>) from generate_pattern_with_params_for_block.
    stmts: &[BlockStatement<PatternWithParams>],
) -> Result<*mut Vdbe, String> {
    // Compile all subprograms.
    let mut compiled: Vec<BlockStatement<CompiledSubprogram>> = stmts
        .iter()
        .map(
            |stmt| -> Result<BlockStatement<CompiledSubprogram>, String> {
                match stmt {
                    BlockStatement::Let { var, query } => Ok(BlockStatement::Let {
                        var: var.clone(),
                        query: CompiledSubprogram::compile(&query.pattern)?,
                    }),
                    BlockStatement::ReturnQuery(query) => Ok(BlockStatement::ReturnQuery(
                        CompiledSubprogram::compile(&query.pattern)?,
                    )),
                    BlockStatement::Query(query) => Ok(BlockStatement::Query(
                        CompiledSubprogram::compile(&query.pattern)?,
                    )),
                    BlockStatement::If { cond, body } => Ok(BlockStatement::If {
                        cond: CompiledSubprogram::compile(&cond.pattern)?,
                        body: body
                            .iter()
                            .map(|bq| CompiledSubprogram::compile(&bq.pattern))
                            .collect::<Result<_, _>>()?,
                    }),
                }
            },
        )
        .collect::<Result<Vec<_>, _>>()?;

    let total_number_of_positional_params: i32 = compiled
        .iter()
        .map(|stmt| match stmt {
            BlockStatement::Let { query, .. }
            | BlockStatement::ReturnQuery(query)
            | BlockStatement::Query(query) => query.count_positional_parameters(),
            BlockStatement::If { cond, body } => {
                cond.count_positional_parameters()
                    + body
                        .iter()
                        .map(|b| b.count_positional_parameters())
                        .sum::<i32>()
            }
        })
        .sum();

    let total_number_of_passsed_params = stmts
        .iter()
        .map(|stmt| match stmt {
            BlockStatement::Let { query, .. }
            | BlockStatement::ReturnQuery(query)
            | BlockStatement::Query(query) => query.params.len(),
            BlockStatement::If { cond, body } => {
                cond.params.len() + body.iter().map(|b| b.params.len()).sum::<usize>()
            }
        })
        .sum::<usize>();

    // Esnure all parameters will be bound.
    debug_assert_eq!(
        total_number_of_passsed_params, total_number_of_positional_params as usize,
        "the number of passed parameters in program doesn't match the actuall number",
    );

    // Allocate aVar slots for LET variables and IF condition results.
    let (all_let_vars, if_cond_slots, total_var_slots) =
        build_var_slots(&compiled, total_number_of_positional_params);

    // Patch OP_ResultRow: LET → named slot, IF cond → anonymous slot.
    let mut if_idx = 0usize;
    for cs in &mut compiled {
        match cs {
            BlockStatement::Let { var, query: sp } => {
                sp.patch_let_result_row(all_let_vars[var.as_str()]);
            }
            BlockStatement::If { cond, .. } => {
                cond.patch_let_result_row(if_cond_slots[if_idx]);
                if_idx += 1;
            }
            BlockStatement::ReturnQuery(_) | BlockStatement::Query(_) => {}
        }
    }

    // Patch OP_Variable slots in execution order, tracking defined LET vars
    // to catch forward references and unknown variable names.
    let mut cumulative_offset = 0i32;
    let mut defined_let_vars: HashMap<String, i32> = HashMap::new();
    for cs in &mut compiled {
        match cs {
            BlockStatement::Let { var, query: sp } => {
                let n =
                    sp.patch_variable_slots(cumulative_offset, &all_let_vars, &defined_let_vars)?;
                cumulative_offset += n;
                defined_let_vars.insert(var.clone(), all_let_vars[var.as_str()]);
            }
            BlockStatement::ReturnQuery(sp) | BlockStatement::Query(sp) => {
                let n =
                    sp.patch_variable_slots(cumulative_offset, &all_let_vars, &defined_let_vars)?;
                cumulative_offset += n;
            }
            BlockStatement::If { cond, body } => {
                // Condition can see LET vars defined before this IF.
                let n =
                    cond.patch_variable_slots(cumulative_offset, &all_let_vars, &defined_let_vars)?;
                cumulative_offset += n;
                // Body queries can also see LET vars defined before this IF.
                for body_sp in body.iter_mut() {
                    let n = body_sp.patch_variable_slots(
                        cumulative_offset,
                        &all_let_vars,
                        &defined_let_vars,
                    )?;
                    cumulative_offset += n;
                }
            }
        }
    }

    // All result-producing subprograms (ReturnQuery and IF body) must have the
    // same number of columns; LET, Query, and IF cond write to aVar or are DML.
    let n_res_column = {
        let mut n: Option<u16> = None;
        let mut check = |sp: &CompiledSubprogram| -> Result<(), String> {
            match n {
                None => {
                    n = Some(sp.n_res_column);
                    Ok(())
                }
                Some(expected) if expected != sp.n_res_column => Err(format!(
                    "all result statements must return the same number of columns \
                     (expected {expected}, got {})",
                    sp.n_res_column
                )),
                Some(_) => Ok(()),
            }
        };
        for cs in &compiled {
            match cs {
                BlockStatement::ReturnQuery(sp) => check(sp)?,
                BlockStatement::If { body, .. } => {
                    for sp in body {
                        check(sp)?;
                    }
                }
                BlockStatement::Let { .. } | BlockStatement::Query(_) => {}
            }
        }
        n.unwrap_or(1)
    };

    let max_n_mem = compiled
        .iter()
        .map(|cs| match cs {
            BlockStatement::Let { query, .. }
            | BlockStatement::ReturnQuery(query)
            | BlockStatement::Query(query) => query.subprogram.nMem,
            BlockStatement::If { cond, body } => body
                .iter()
                .map(|b| b.subprogram.nMem)
                .max()
                .unwrap_or(0)
                .max(cond.subprogram.nMem),
        })
        .max()
        .unwrap_or(1);

    Ok(unsafe {
        assemble_block_vdbe(
            compiled,
            &if_cond_slots,
            n_res_column,
            max_n_mem,
            total_var_slots,
        )
    })
}

/// Collect the merged parameter list for a block of statements.
///
/// Parameters appear in execution order: for IF, condition params come before
/// body params.
fn collect_params(stmts: &[BlockStatement<PatternWithParams>]) -> Vec<&Value> {
    let mut result = Vec::new();
    for stmt in stmts {
        match stmt {
            BlockStatement::Let { query, .. }
            | BlockStatement::ReturnQuery(query)
            | BlockStatement::Query(query) => {
                result.extend(query.params.iter());
            }
            BlockStatement::If { cond, body } => {
                result.extend(cond.params.iter());
                for bq in body {
                    result.extend(bq.params.iter());
                }
            }
        }
    }
    result
}
