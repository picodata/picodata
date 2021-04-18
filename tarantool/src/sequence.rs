//! Box: sequences
use crate::error::{Error, TarantoolError};
use crate::ffi::tarantool as ffi;
use crate::schema;
use crate::space::{Space, SystemSpace};
use crate::tuple::AsTuple;

/// A sequence is a generator of ordered integer values.
pub struct Sequence {
    seq_id: u32,
}

impl Sequence {
    /// Find sequence by name.
    pub fn find(name: &str) -> Result<Option<Self>, Error> {
        #[derive(Serialize, Deserialize)]
        struct Row {
            seq_id: u32,
        }

        impl AsTuple for Row {}

        let space: Space = SystemSpace::Sequence.into();
        let name_idx = space.index("name").unwrap();

        Ok(match name_idx.get(&(name,))? {
            None => None,
            Some(row_tuple) => Some(Sequence {
                seq_id: row_tuple.as_struct::<Row>()?.seq_id,
            }),
        })
    }

    /// Find sequence by id.
    pub fn find_by_id(id: u32) -> Result<Option<Self>, Error> {
        let sys_vsequence: Space = SystemSpace::VSequence.into();
        let index_name = sys_vsequence.index("name").unwrap();
        match index_name.get(&(id,))? {
            None => Ok(None),
            Some(_) => Ok(Some(Sequence { seq_id: id })),
        }
    }

    /// Drop sequence.
    pub fn drop(&self) -> Result<(), Error> {
        schema::revoke_object_privileges("sequence", self.seq_id)?;

        let mut sys_sequence: Space = SystemSpace::Sequence.into();
        sys_sequence.delete(&(self.seq_id,))?;

        let mut sys_sequence_data: Space = SystemSpace::SequenceData.into();
        sys_sequence_data.delete(&(self.seq_id,))?;

        Ok(())
    }

    /// Generate the next value and return it.
    ///
    /// The generation algorithm is simple:
    /// - If this is the first time, then return the `start` value.
    /// - If the previous value plus the `increment` value is less than the `minimum` value or greater than the
    /// `maximum` value, that is "overflow", so either raise an error (if `cycle = false`) or return the `maximum` value
    /// (if `cycle = true` and `step < 0`) or return the `minimum` value (if `cycle = true` and `step > 0`).
    ///
    /// If there was no error, then save the returned result, it is now the "previous value".
    pub fn next(&mut self) -> Result<i64, Error> {
        let mut result: i64 = 0;
        if unsafe { ffi::box_sequence_next(self.seq_id, &mut result) } < 0 {
            Err(TarantoolError::last().into())
        } else {
            Ok(result)
        }
    }

    /// Set the "previous value" to `new_value`.
    ///
    /// This function requires a "write" privilege on the sequence.
    pub fn set(&mut self, new_value: i64) -> Result<(), Error> {
        if unsafe { ffi::box_sequence_set(self.seq_id, new_value) } < 0 {
            Err(TarantoolError::last().into())
        } else {
            Ok(())
        }
    }

    /// Set the sequence back to its original state.
    ///
    /// The effect is that a subsequent [next](#method.next) will return the start value.
    /// This function requires a "write" privilege on the sequence.
    pub fn reset(&mut self) -> Result<(), Error> {
        if unsafe { ffi::box_sequence_reset(self.seq_id) } < 0 {
            Err(TarantoolError::last().into())
        } else {
            Ok(())
        }
    }
}
