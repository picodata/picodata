use crate::ffi::tarantool as ffi;
use crate::index::IndexId;
use crate::space::SpaceId;
use std::mem::align_of;
use std::mem::size_of;
use std::mem::MaybeUninit;
use std::ptr::NonNull;

////////////////////////////////////////////////////////////////////////////////
// read view
////////////////////////////////////////////////////////////////////////////////

/// An object which guards a read view on the selected set of spaces and
/// indexes. Provides access to the frozen contents of the selected indexes at
/// the moment of the read view's creation.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ReadView {
    inner: NonNull<ffi::box_read_view_t>,
    space_indexes: Vec<(SpaceId, IndexId)>,
}

impl ReadView {
    /// Open a read view on the given space indexes.
    #[inline]
    #[track_caller]
    pub fn for_space_indexes(space_indexes: Vec<(SpaceId, IndexId)>) -> crate::Result<Self> {
        const _: () = {
            assert!(size_of::<(SpaceId, IndexId)>() == size_of::<ffi::space_index_id>());
            assert!(align_of::<(SpaceId, IndexId)>() == align_of::<ffi::space_index_id>());
        };

        let loc = std::panic::Location::caller();
        let name = format!("<rust@{}:{}>\0", loc.file(), loc.line());
        let rv = unsafe {
            ffi::box_read_view_open_for_given_spaces(
                name.as_ptr() as _,
                space_indexes.as_ptr() as _,
                space_indexes.len() as _,
                0,
            )
        };

        let Some(rv) = NonNull::new(rv) else {
            return Err(crate::error::TarantoolError::last().into());
        };

        Ok(Self {
            inner: rv,
            space_indexes,
        })
    }

    /// Get the list of spaces and idexes for which the read view was opened, if
    /// it's available.
    #[inline(always)]
    pub fn space_indexes(&self) -> Option<&[(SpaceId, IndexId)]> {
        // NOTE: Currently the data is always available but in the future we
        // may add other ways of specifying spaces for the read view (e.g. all
        // spaces, etc.). In that case the slice of space & index ids would not
        // be available and this function would return None.
        Some(&self.space_indexes)
    }

    /// Get an iterator over all of the tuples in the given index read view.
    /// The tuples are returned as raw byte slices.
    #[inline]
    pub fn iter_all(
        &self,
        space: SpaceId,
        index: IndexId,
    ) -> crate::Result<Option<ReadViewIterator>> {
        unsafe {
            let mut iter = MaybeUninit::uninit();
            let rc = ffi::box_read_view_iterator_all(
                self.inner.as_ptr(),
                space,
                index,
                iter.as_mut_ptr(),
            );
            if rc != 0 {
                return Err(crate::error::TarantoolError::last().into());
            }
            let iter = iter.assume_init();
            Ok(NonNull::new(iter).map(|inner| ReadViewIterator {
                inner,
                _marker: std::marker::PhantomData,
            }))
        }
    }
}

impl Drop for ReadView {
    #[inline(always)]
    fn drop(&mut self) {
        unsafe { ffi::box_read_view_close(self.inner.as_ptr()) }
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ReadViewIterator<'a> {
    inner: NonNull<ffi::box_read_view_iterator_t>,
    _marker: std::marker::PhantomData<&'a ()>,
}

impl<'a> Iterator for ReadViewIterator<'a> {
    type Item = &'a [u8];

    /// Get next tuple data.
    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        let mut data = MaybeUninit::uninit();
        let mut size = MaybeUninit::uninit();
        unsafe {
            let rc = ffi::box_read_view_iterator_next_raw(
                self.inner.as_ptr(),
                data.as_mut_ptr(),
                size.as_mut_ptr(),
            );
            if rc != 0 {
                return None;
            }
            let data = data.assume_init();
            let size = size.assume_init();
            if data.is_null() {
                return None;
            }
            Some(std::slice::from_raw_parts(data, size as _))
        }
    }
}

impl<'a> Drop for ReadViewIterator<'a> {
    #[inline(always)]
    fn drop(&mut self) {
        unsafe { ffi::box_read_view_iterator_free(self.inner.as_ptr()) }
    }
}

#[cfg(feature = "internal_test")]
mod tests {
    use super::*;
    use crate::space::Space;
    use crate::space::SystemSpace;
    use crate::temp_space_name;

    #[crate::test(tarantool = "crate")]
    fn read_view() {
        let s = Space::builder(&temp_space_name!()).create().unwrap();
        s.index_builder("pk").create().unwrap();
        s.insert(&(1, 2, 3)).unwrap();
        s.insert(&(2, "hello")).unwrap();

        let rv = ReadView::for_space_indexes(vec![(s.id(), 0)]).unwrap();

        // Space is not in the read view.
        assert_eq!(rv.iter_all(SystemSpace::Space as _, 0).unwrap(), None);

        let mut iter = rv.iter_all(s.id(), 0).unwrap().unwrap();
        assert_eq!(iter.next(), Some(&b"\x93\x01\x02\x03"[..]));
        assert_eq!(iter.next(), Some(&b"\x92\x02\xa5hello"[..]));
        assert_eq!(iter.next(), None);
        assert_eq!(iter.next(), None);
    }
}
