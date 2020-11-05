use crate::error::{Error, TarantoolError};
use crate::space::{Space, SystemSpace};
use crate::tuple::AsTuple;

pub struct Sequence {
    seq_id: u32,
}

impl Sequence {
    pub fn find(name: &str) -> Result<Option<Self>, Error> {
        #[derive(Serialize, Deserialize)]
        struct Row {
            seq_id: u32,
        }

        impl AsTuple for Row {}

        let name_idx = Space::system_space(SystemSpace::Sequence)
            .index("name")
            .unwrap();

        Ok(match name_idx.get(&(name,))? {
            None => None,
            Some(row_tuple) => Some(Sequence {
                seq_id: row_tuple.into_struct::<Row>()?.seq_id,
            }),
        })
    }

    /// Advance a sequence.
    pub fn next(&mut self) -> Result<i64, Error> {
        let mut result: i64 = 0;
        if unsafe { ffi::box_sequence_next(self.seq_id, &mut result) } < 0 {
            Err(TarantoolError::last().into())
        } else {
            Ok(result)
        }
    }

    /// Set a sequence value.
    pub fn set(&mut self, value: i64) -> Result<(), Error> {
        if unsafe { ffi::box_sequence_set(self.seq_id, value) } < 0 {
            Err(TarantoolError::last().into())
        } else {
            Ok(())
        }
    }

    /// Reset a sequence.
    pub fn reset(&mut self) -> Result<(), Error> {
        if unsafe { ffi::box_sequence_reset(self.seq_id) } < 0 {
            Err(TarantoolError::last().into())
        } else {
            Ok(())
        }
    }
}

pub mod ffi {
    use std::os::raw::c_int;

    extern "C" {
        pub fn box_sequence_next(seq_id: u32, result: *mut i64) -> c_int;
        pub fn box_sequence_set(seq_id: u32, value: i64) -> c_int;
        pub fn box_sequence_reset(seq_id: u32) -> c_int;
    }
}
