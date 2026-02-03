use crate::{
    errors::SbroadError,
    ir::{
        node::{
            expression::Expression, relational::Relational, Cast, Motion, NodeId, Parameter,
            Values, ValuesRow,
        },
        transformation::redistribution::{MotionPolicy, Target},
        types::UnrestrictedType,
        Plan,
    },
};

impl Plan {
    fn try_get_param_info(
        &self,
        id: NodeId,
    ) -> Result<Option<(u16, UnrestrictedType)>, SbroadError> {
        match self.get_expression_node(id)? {
            Expression::Cast(Cast { child, to }) => {
                if let Expression::Parameter(Parameter {
                    index, param_type, ..
                }) = self.get_expression_node(*child)?
                {
                    let param_type = param_type.get().expect("parameter type must be known");
                    let cast_type = UnrestrictedType::from(*to);
                    if param_type == cast_type {
                        Ok(Some((*index, param_type)))
                    } else {
                        Ok(None)
                    }
                } else {
                    Ok(None)
                }
            }
            Expression::Parameter(Parameter {
                index, param_type, ..
            }) => Ok(Some((
                *index,
                param_type.get().expect("parameter type must be known"),
            ))),
            _ => Ok(None),
        }
    }

    fn collect_insert_metadata(&self) -> Result<Vec<(u16, UnrestrictedType)>, SbroadError> {
        let insert_id = self.get_top()?;
        let child_id = self.dml_child_id(insert_id)?;
        let Relational::Motion(Motion { child, policy, .. }) = self.get_relation_node(child_id)?
        else {
            return Ok(Vec::new());
        };

        let MotionPolicy::Segment(motion_key) = policy else {
            return Ok(Vec::new());
        };

        let Relational::Values(Values { children, .. }) =
            self.get_relation_node(child.expect("MOTION must contain child"))?
        else {
            return Ok(Vec::new());
        };

        if children.len() > 1 {
            return Ok(Vec::new());
        }

        let mut param_idx = Vec::new();
        let row_node = self.get_relation_node(children[0])?;
        let Relational::ValuesRow(ValuesRow { data, .. }) = row_node else {
            panic!("Expected ValuesRow under Values. Got {row_node:?}.")
        };
        let row_list = self.get_row_list(*data)?;

        for target in motion_key.targets.iter() {
            if let Target::Reference(pos) = target {
                let column_id = row_list
                    .get(*pos)
                    .expect("Reference position should point to column");
                match self.try_get_param_info(*column_id)? {
                    Some(idx) => {
                        param_idx.push(idx);
                    }
                    None => {
                        return Ok(Vec::new());
                    }
                }
            } else {
                return Ok(Vec::new());
            }
        }

        Ok(param_idx)
    }

    /// Discover indexes of params that are used
    /// for bucket id calculation.
    ///
    /// # Errors
    /// - Node with id is not present in plan.
    #[allow(clippy::too_many_lines)]
    pub fn discover_sharding_key_info(&self) -> Result<Vec<(u16, UnrestrictedType)>, SbroadError> {
        // INSERT INTO T VALUES ($1, $2)
        // We need to handle this case separately
        // since it possible to determine indexes
        // even though plan contains segment motion.
        if self.is_insert()? {
            self.collect_insert_metadata()
        } else {
            Ok(Vec::new())
        }
    }
}
