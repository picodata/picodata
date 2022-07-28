use crate::{AsLua, LuaState, LuaRead, Push, PushInto, PushOneInto};
use crate::object::{FromObject, Object};
use crate::lua_functions::LuaFunction;
use std::os::raw::{c_char, c_void};
use std::cell::UnsafeCell;
use std::num::NonZeroI32;
use std::convert::TryFrom;
use crate::ffi;

////////////////////////////////////////////////////////////////////////////////
/// CDataOnStack
////////////////////////////////////////////////////////////////////////////////

/// Represents a reference to the underlying cdata value corresponding to a
/// given cdata object.
#[derive(Debug, Clone, Copy)]
enum CDataRef<'l> {
    Ptr(*mut c_void),
    Slice(&'l [u8]),
}

/// A cdata value stored on lua stack. Can be used to check type of cdata,
/// access the raw bytes of the data, downcast it to a rust type or passed as
/// an argument into a lua function.
/// # Examples:
/// ```no_run
/// use tlua::{CDataOnStack, Lua, ffi};
/// let lua = Lua::new();
/// let cdata: CDataOnStack<_> = lua.eval("
///     ffi = require 'ffi';
///     return ffi.new('uint8_t', 69)
/// ").unwrap();
///
/// // check CTypeID
/// assert_eq!(cdata.ctypeid(), ffi::CTID_UINT8);

/// // check raw bytes
/// assert_eq!(cdata.data(), [69]);
///
/// // pass to a lua function
/// let n: u8 = lua.eval_with("return ... + 1", &cdata).unwrap();
/// assert_eq!(n, 70);
/// ```
#[derive(Debug)]
pub struct CDataOnStack<'l, L> {
    inner: Object<L>,
    data: UnsafeCell<CDataRef<'l>>,
    ctypeid: ffi::CTypeID,
}

impl<L> CDataOnStack<'_, L>
where
    L: AsLua,
{
    /// Return pointer to data. Maybe use [`CDataOnStack::data`] instead.
    #[inline(always)]
    pub fn as_ptr(&self) -> *const c_void {
        match unsafe { *self.data.get() } {
            CDataRef::Ptr(ptr) => ptr,
            CDataRef::Slice(slice) => slice.as_ptr().cast(),
        }
    }

    /// Updates the CDataRef inplace replacing the pointer with a slice of known
    /// size. This function is only executed once because it performs an
    /// expensnive call to luajit runtime to figure out the size of the data.
    fn update_data(&self, ptr: *const c_void) -> &[u8] {
        let f = LuaFunction::load(self, "return require('ffi').sizeof(...)").unwrap();
        let size: usize = f.into_call_with_args(self).unwrap();
        unsafe {
            let slice = std::slice::from_raw_parts(ptr.cast(), size);
            std::ptr::write(self.data.get(), CDataRef::Slice(slice));
            slice
        }
    }

    /// Return a slice of bytes covering the data if the data's size was already
    /// retrieved before. Otherwise return `None`.
    ///
    /// See also [`CDataOnStack::data`].
    pub fn try_as_bytes(&self) -> Option<&[u8]> {
        match unsafe { *self.data.get() } {
            CDataRef::Slice(slice) => Some(slice),
            CDataRef::Ptr(_) => None,
        }
    }

    /// Return a mutable slice of bytes covering the data if the data's size was
    /// already retrieved before. Otherwise return `None`.
    ///
    /// See also [`CDataOnStack::data_mut`].
    pub fn try_as_bytes_mut(&mut self) -> Option<&mut [u8]> {
        match unsafe { *self.data.get() } {
            CDataRef::Slice(slice) => unsafe {
                Some(std::slice::from_raw_parts_mut(slice.as_ptr() as *mut _, slice.len()))
            },
            CDataRef::Ptr(_) => None,
        }
    }

    /// Return a slice of bytes covering the data. Calling this function the
    /// first time around will perform an expensive operation of retrieving the
    /// data's size. So if for some reason you just need the pointer to the
    /// data, use the [`CDataOnStack::as_ptr`]. But if you actually need the
    /// bytes, use this function.
    #[inline(always)]
    pub fn data(&self) -> &[u8] {
        match unsafe { *self.data.get() } {
            CDataRef::Ptr(ptr) => self.update_data(ptr),
            CDataRef::Slice(slice) => slice,
        }
    }

    /// Return a mutable slice of bytes covering the data. Calling this function the
    /// first time around will perform an expensive operation of retrieving the
    /// data's size.
    #[inline(always)]
    pub fn data_mut(&mut self) -> &mut [u8] {
        let data = self.data();
        unsafe {
            std::slice::from_raw_parts_mut(data.as_ptr() as *mut _, data.len())
        }
    }

    /// Return the ctypeid of the cdata.
    #[inline(always)]
    pub fn ctypeid(&self) -> ffi::CTypeID {
        self.ctypeid
    }

    /// Return a reference to the underlying value if
    /// `self.`[`ctypeid`]`() ==
    /// <T as `[`AsCData`]`>::`[`ctypeid`](AsCData::ctypeid)`()`,
    /// otherwise return `None`.
    ///
    /// **This function may panic, if
    /// `self.`[`data`]`().len() != std::mem::size_of::<T>()`**
    ///
    /// [`data`]: CDataOnStack::data
    /// [`ctypeid`]: CDataOnStack::ctypeid
    #[inline(always)]
    pub fn try_downcast<T>(&self) -> Option<&T>
    where
        T: AsCData,
    {
        self.check_ctypeid::<T>()
            .then(|| unsafe { &*self.as_ptr().cast::<T>() })
    }

    /// Return a mutable reference to the underlying value if
    /// `self.`[`ctypeid`]`() ==
    /// <T as `[`AsCData`]`>::`[`ctypeid`](AsCData::ctypeid)`()`,
    /// otherwise return `None`.
    ///
    /// **This function may panic, if
    /// `self.`[`data`]`().len() != std::mem::size_of::<T>()`**
    ///
    /// [`data`]: CDataOnStack::data
    /// [`ctypeid`]: CDataOnStack::ctypeid
    #[inline(always)]
    pub fn try_downcast_mut<T>(&self) -> Option<&mut T>
    where
        T: AsCData,
    {
        self.check_ctypeid::<T>()
            .then(|| unsafe { &mut *(self.as_ptr().cast::<T>() as *mut _) })
    }

    /// Return the underlying value consuming `self` if
    /// `self.`[`ctypeid`]`() ==
    /// <T as `[`AsCData`]`>::`[`ctypeid`](AsCData::ctypeid)`()`,
    /// otherwise return `Err(self)`.
    ///
    /// **This function may panic, if
    /// `self.`[`data`]`().len() != std::mem::size_of::<T>()`**
    ///
    /// [`data`]: CDataOnStack::data
    /// [`ctypeid`]: CDataOnStack::ctypeid
    #[inline(always)]
    pub fn try_downcast_into<T>(self) -> Result<T, Self>
    where
        T: AsCData,
    {
        self.check_ctypeid::<T>()
            .then(|| unsafe { std::ptr::read(self.as_ptr().cast::<T>()) })
            .ok_or(self)
    }

    #[inline(always)]
    #[allow(clippy::nonminimal_bool)]
    fn check_ctypeid<T: AsCData>(&self) -> bool {
        self.ctypeid == T::ctypeid() && {
            if cfg!(debug_assertions) {
                assert_eq!(self.data().len(), std::mem::size_of::<T>());
            }
            true
        }
    }
}

impl<L> FromObject<L> for CDataOnStack<'_, L>
where
    L: AsLua,
{
    unsafe fn check(lua: impl AsLua, index: NonZeroI32) -> bool {
        ffi::lua_type(lua.as_lua(), index.into()) == ffi::LUA_TCDATA
    }

    unsafe fn from_obj(inner: Object<L>) -> Self {
        let mut ctypeid = 0;
        let cdata = ffi::luaL_checkcdata(
            inner.as_lua(),
            inner.index().into(),
            &mut ctypeid,
        );
        Self {
            inner,
            data: UnsafeCell::new(CDataRef::Ptr(cdata)),
            ctypeid,
        }
    }
}

impl<L> TryFrom<Object<L>> for CDataOnStack<'_, L>
where
    L: AsLua,
{
    type Error = Object<L>;

    #[inline(always)]
    fn try_from(o: Object<L>) -> Result<Self, Self::Error> {
        Self::try_from_obj(o)
    }
}

impl<'a, L> From<CDataOnStack<'a, L>> for Object<L> {
    fn from(ud: CDataOnStack<'a, L>) -> Self {
        ud.inner
    }
}

impl<L> LuaRead<L> for CDataOnStack<'_, L>
where
    L: AsLua,
{
    #[inline]
    fn lua_read_at_position(lua: L, index: NonZeroI32) -> Result<Self, L> {
        Self::try_from_obj(Object::new(lua, index))
            .map_err(Object::into_guard)
    }
}

impl<L, O> Push<L> for CDataOnStack<'_, O>
where
    L: AsLua,
{
    type Err = crate::Void;
    #[inline]
    fn push_to_lua(&self, lua: L) -> Result<crate::PushGuard<L>, (Self::Err, L)> {
        unsafe {
            crate::ffi::lua_pushvalue(lua.as_lua(), self.inner.index().into());
            Ok(crate::PushGuard::new(lua, 1))
        }
    }
}

impl<L> AsLua for CDataOnStack<'_, L>
where
    L: AsLua,
{
    #[inline]
    fn as_lua(&self) -> LuaState {
        self.inner.as_lua()
    }
}

////////////////////////////////////////////////////////////////////////////////
/// AsCData
////////////////////////////////////////////////////////////////////////////////

/// Types implementing this trait can be represented as luajit's cdata.
///
/// # Safety
/// The implementor must make sure that the type can actually be used as cdata,
/// for example the CTypeID must be correct and the size of the type must be the
/// same as `ffi.sizeof(<typeid>)`
pub unsafe trait AsCData: Sized + 'static {
    /// This function will be called every time the ctypeid is needed, so
    /// implement your own caching if it's required.
    fn ctypeid() -> ffi::CTypeID;
}

macro_rules! impl_builtin_as_cdata {
    ($($t:ty: $ctid:expr),* $(,)?) => {
        $(
            unsafe impl AsCData for $t {
                fn ctypeid() -> ffi::CTypeID {
                    $ctid
                }
            }
        )*
    };
}

impl_builtin_as_cdata! {
    i8 : ffi::CTID_INT8,
    i16: ffi::CTID_INT16,
    i32: ffi::CTID_INT32,
    i64: ffi::CTID_INT64,
    u8 : ffi::CTID_UINT8,
    u16: ffi::CTID_UINT16,
    u32: ffi::CTID_UINT32,
    u64: ffi::CTID_UINT64,
    f32: ffi::CTID_FLOAT,
    f64: ffi::CTID_DOUBLE,

    bool: ffi::CTID_BOOL,

    *mut   c_void: ffi::CTID_P_VOID,
    *const c_void: ffi::CTID_P_CVOID,
    *const c_char: ffi::CTID_P_CCHAR,

    isize:
        match std::mem::size_of::<isize>() {
            4 => ffi::CTID_INT32,
            8 => ffi::CTID_INT64,
            _ => unimplemented!("only 32 & 64 bit pointers are supported"),
        },
    usize:
        match std::mem::size_of::<usize>() {
            4 => ffi::CTID_UINT32,
            8 => ffi::CTID_UINT64,
            _ => unimplemented!("only 32 & 64 bit pointers are supported"),
        },
}

////////////////////////////////////////////////////////////////////////////////
/// CData
////////////////////////////////////////////////////////////////////////////////

/// A wrapper type for reading/writing rust values as luajit cdata.
/// ```no_run
/// use tlua::{Lua, CData};
/// let lua = Lua::new();
/// lua.set("cdata", CData(1337_i16));
/// let ty: String = lua.eval("return require('ffi').typeof(cdata)").unwrap();
/// assert_eq!(ty, "ctype<short>");
///
/// let CData(num): CData<i16> = lua.get("cdata").unwrap();
/// assert_eq!(num, 1337);
/// ```
///
/// For this to work the type must implement [`AsCData`] which is true for
/// builtin numbers and some pointers but can also be implemented for user
/// defined types:
/// ```no_run
/// use tlua::{AsCData, CData};
/// use tlua::{Lua, AsLua, ffi, c_ptr};
/// # let lua = Lua::new();
///
/// #[repr(C)]
/// #[derive(Debug, PartialEq, Clone, Copy)]
/// struct S { i: i32, f: f32 }
///
/// // let luajit know about our struct
/// lua.exec("ffi.cdef[[ struct S { int i; float f; }; ]]").unwrap();
///
/// // save the CTypeID of our struct
/// static mut CTID_STRUCT_S: Option<ffi::CTypeID> = None;
/// let ctid = unsafe { ffi::luaL_ctypeid(lua.as_lua(), c_ptr!("struct S")) };
/// unsafe { CTID_STRUCT_S = Some(ctid) }
///
/// // implement AsCData for our struct so that it can be wrapped with CData
/// unsafe impl AsCData for S {
///     fn ctypeid() -> ffi::CTypeID {
///         unsafe { CTID_STRUCT_S.unwrap() }
///     }
/// }
///
/// // wirte our struct into a lua variable as cdata
/// lua.set("tmp", CData(S { i: 69, f: 420.0 }));
///
/// // check it's type
/// let ty: String = lua.eval("return type(tmp)").unwrap();
/// assert_eq!(ty, "cdata");
///
/// // read the value back
/// let CData(res): CData<S> = lua.get("tmp").unwrap();
/// assert_eq!(res, S { i: 69, f: 420.0 });
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CData<T>(pub T)
where
    T: AsCData;

impl<L, T> PushInto<L> for CData<T>
where
    L: AsLua,
    T: AsCData,
{
    type Err = crate::Void;
    fn push_into_lua(self, lua: L) -> Result<crate::PushGuard<L>, (Self::Err, L)> {
        let Self(value) = self;
        unsafe {
            let ptr = ffi::luaL_pushcdata(lua.as_lua(), T::ctypeid());
            std::ptr::write(ptr.cast::<T>(), value);
            Ok(crate::PushGuard::new(lua, 1))
        }
    }
}
impl<L, T> PushOneInto<L> for CData<T>
where
    L: AsLua,
    T: AsCData,
    T: Copy,
{
}

impl<L, T> LuaRead<L> for CData<T>
where
    L: AsLua,
    T: AsCData,
{
    fn lua_read_at_position(lua: L, index: NonZeroI32) -> Result<Self, L> {
        CDataOnStack::lua_read_at_position(lua, index)
            .and_then(|data| {
                match data.try_downcast_into() {
                    Ok(value) => Ok(CData(value)),
                    Err(data) => Err(data.inner.into_guard()),
                }
            })
    }
}
