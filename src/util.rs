use crate::traft::error::Error;
use nix::sys::termios::{tcgetattr, tcsetattr, LocalFlags, SetArg::TCSADRAIN};
use std::any::{Any, TypeId};
use std::cell::Cell;
use std::io::BufRead as _;
use std::io::BufReader;
use std::io::Write as _;
use std::mem::replace;
use std::os::fd::AsRawFd;
use std::panic::Location;
use std::path::Path;
use std::time::Duration;
use tarantool::session::{self, UserId};
pub use Either::{Left, Right};

pub const INFINITY: Duration = Duration::from_secs(30 * 365 * 24 * 60 * 60);

/// Converts `secs` to `Duration`. If `secs` is negative, it's clamped to zero.
/// If `secs` overflows the `Duration` it's clamped to [`INFINITY`].
///
/// Panics if `secs` is NaN.
#[inline(always)]
pub fn duration_from_secs_f64_clamped(secs: f64) -> Duration {
    if secs <= 0.0 {
        Duration::ZERO
    } else if secs.is_nan() {
        panic!("attempt to construct a Duration from NaN of seconds");
    } else if let Ok(d) = Duration::try_from_secs_f64(secs) {
        d
    } else {
        INFINITY
    }
}

////////////////////////////////////////////////////////////////////////////////
/// A generic enum that contains exactly one of two possible types. Equivalent
/// to `std::result::Result`, but is more intuitive in some cases.
#[derive(Debug, PartialEq, Eq, Hash)]
pub enum Either<L, R> {
    Left(L),
    Right(R),
}

impl<L, R> Either<L, R> {
    #[inline(always)]
    pub fn map_left<F, T>(self, f: F) -> Either<T, R>
    where
        F: FnOnce(L) -> T,
    {
        match self {
            Left(l) => Left(f(l)),
            Right(r) => Right(r),
        }
    }

    #[inline(always)]
    pub fn left(self) -> Option<L> {
        match self {
            Left(l) => Some(l),
            _ => None,
        }
    }

    #[inline(always)]
    pub fn unwrap_left_or(self, default: L) -> L {
        match self {
            Left(l) => l,
            _ => default,
        }
    }

    #[inline(always)]
    pub fn unwrap_left_or_else(self, f: impl FnOnce(R) -> L) -> L {
        match self {
            Left(l) => l,
            Right(r) => f(r),
        }
    }

    #[inline(always)]
    pub fn map_right<F, T>(self, f: F) -> Either<L, T>
    where
        F: FnOnce(R) -> T,
    {
        match self {
            Left(l) => Left(l),
            Right(r) => Right(f(r)),
        }
    }

    #[inline(always)]
    pub fn right(self) -> Option<R> {
        match self {
            Right(r) => Some(r),
            _ => None,
        }
    }

    #[inline(always)]
    pub fn unwrap_right_or(self, default: R) -> R {
        match self {
            Right(r) => r,
            _ => default,
        }
    }

    #[inline(always)]
    pub fn unwrap_right_or_else(self, f: impl FnOnce(L) -> R) -> R {
        match self {
            Left(l) => f(l),
            Right(r) => r,
        }
    }

    #[inline(always)]
    pub fn as_ref(&self) -> Either<&L, &R> {
        match self {
            Left(l) => Left(l),
            Right(r) => Right(r),
        }
    }
}

impl<L, R> From<Result<L, R>> for Either<L, R> {
    fn from(r: Result<L, R>) -> Self {
        match r {
            Ok(l) => Left(l),
            Err(r) => Right(r),
        }
    }
}

impl<L, R> From<Either<L, R>> for Result<L, R> {
    fn from(e: Either<L, R>) -> Self {
        match e {
            Left(l) => Ok(l),
            Right(r) => Err(r),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// macros
////////////////////////////////////////////////////////////////////////////////

#[macro_export]
macro_rules! unwrap_some_or {
    ($o:expr, $($else:tt)+) => {
        match $o {
            Some(v) => v,
            None => $($else)+,
        }
    }
}

#[macro_export]
macro_rules! unwrap_ok_or {
    ($o:expr, $err:pat => $($else:tt)+) => {
        match $o {
            Ok(v) => v,
            $err => $($else)+,
        }
    }
}

#[macro_export]
macro_rules! warn_or_panic {
    ($($arg:tt)*) => {{
        $crate::tlog!(Warning, $($arg)*);
        if cfg!(debug_assertions) {
            panic!($($arg)*);
        }
    }};
}

#[macro_export]
macro_rules! stringify_debug {
    ($t:ty) => {{
        fn _check_debug<T: std::fmt::Debug>() {}
        _check_debug::<$t>();
        ::std::stringify!($t)
    }};
}

#[macro_export]
macro_rules! define_string_newtype {
    (
        $(#[$meta:meta])*
        pub struct $type:ident ( pub String );
    ) => {
        #[derive(
            Default,
            Debug,
            Eq,
            Clone,
            Hash,
            Ord,
            ::tarantool::tlua::LuaRead,
            ::tarantool::tlua::Push,
            ::tarantool::tlua::PushInto,
            serde::Serialize,
            serde::Deserialize,
        )]
        pub struct $type(pub String);

        impl ::std::fmt::Display for $type {
            fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
                ::std::fmt::Display::fmt(&self.0, f)
            }
        }

        impl From<String> for $type {
            fn from(s: String) -> Self {
                Self(s)
            }
        }

        impl From<&str> for $type {
            fn from(s: &str) -> Self {
                Self(s.into())
            }
        }

        impl From<$type> for String {
            fn from(i: $type) -> Self {
                i.0
            }
        }

        impl AsRef<str> for $type {
            fn as_ref(&self) -> &str {
                &self.0
            }
        }

        impl ::std::borrow::Borrow<str> for $type {
            fn borrow(&self) -> &str {
                &self.0
            }
        }

        impl ::std::ops::Deref for $type {
            type Target = str;
            fn deref(&self) -> &str {
                &self.0
            }
        }

        impl<T> ::std::cmp::PartialEq<T> for $type
        where
            T: ?Sized,
            T: AsRef<str>,
        {
            fn eq(&self, rhs: &T) -> bool {
                self.0 == rhs.as_ref()
            }
        }

        impl<T> ::std::cmp::PartialOrd<T> for $type
        where
            T: ?Sized,
            T: AsRef<str>,
        {
            fn partial_cmp(&self, rhs: &T) -> Option<::std::cmp::Ordering> {
                (*self.0).partial_cmp(rhs.as_ref())
            }
        }

        impl ::std::str::FromStr for $type {
            type Err = ::std::convert::Infallible;

            fn from_str(s: &str) -> ::std::result::Result<Self, ::std::convert::Infallible> {
                Ok(Self(s.into()))
            }
        }
    };
}

////////////////////////////////////////////////////////////////////////////////
/// A wrapper around `String` that garantees the string is uppercase by
/// converting it to uppercase (if needed) on construction.
#[derive(Default, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, serde::Serialize)]
pub struct Uppercase(String);

impl<'de> serde::Deserialize<'de> for Uppercase {
    fn deserialize<D>(de: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        Ok(String::deserialize(de)?.into())
    }
}

impl<L: ::tarantool::tlua::AsLua> ::tarantool::tlua::Push<L> for Uppercase {
    type Err = ::tarantool::tlua::Void;

    fn push_to_lua(&self, lua: L) -> Result<tarantool::tlua::PushGuard<L>, (Self::Err, L)> {
        self.0.push_to_lua(lua)
    }
}

impl<L: ::tarantool::tlua::AsLua> ::tarantool::tlua::PushOne<L> for Uppercase {}

impl<L: ::tarantool::tlua::AsLua> ::tarantool::tlua::PushInto<L> for Uppercase {
    type Err = ::tarantool::tlua::Void;

    fn push_into_lua(self, lua: L) -> Result<tarantool::tlua::PushGuard<L>, (Self::Err, L)> {
        self.0.push_into_lua(lua)
    }
}

impl<L: ::tarantool::tlua::AsLua> ::tarantool::tlua::PushOneInto<L> for Uppercase {}

impl<L: ::tarantool::tlua::AsLua> ::tarantool::tlua::LuaRead<L> for Uppercase {
    fn lua_read_at_position(
        lua: L,
        index: std::num::NonZeroI32,
    ) -> ::tarantool::tlua::ReadResult<Self, L> {
        Ok(String::lua_read_at_position(lua, index)?.into())
    }
}

impl From<String> for Uppercase {
    fn from(s: String) -> Self {
        if s.chars().all(char::is_uppercase) {
            Self(s)
        } else {
            Self(s.to_uppercase())
        }
    }
}

impl From<&str> for Uppercase {
    fn from(s: &str) -> Self {
        Self(s.to_uppercase())
    }
}

impl From<Uppercase> for String {
    fn from(u: Uppercase) -> Self {
        u.0
    }
}

impl std::ops::Deref for Uppercase {
    type Target = String;

    fn deref(&self) -> &String {
        &self.0
    }
}

impl std::fmt::Display for Uppercase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

impl std::borrow::Borrow<str> for Uppercase {
    fn borrow(&self) -> &str {
        &self.0
    }
}

////////////////////////////////////////////////////////////////////////////////
/// Compare string literals at compile time.

#[allow(dead_code)] // suppress the warning since it's only used at compile time
pub const fn str_eq(lhs: &str, rhs: &str) -> bool {
    let lhs = lhs.as_bytes();
    let rhs = rhs.as_bytes();
    if lhs.len() != rhs.len() {
        return false;
    }
    let mut i = 0;
    loop {
        if i == lhs.len() {
            return true;
        }
        if lhs[i] != rhs[i] {
            return false;
        }
        i += 1;
    }
}

////////////////////////////////////////////////////////////////////////////////
/// Return terminal screen size in rows, columns.
pub fn screen_size() -> (i32, i32) {
    let mut rows = std::mem::MaybeUninit::uninit();
    let mut cols = std::mem::MaybeUninit::uninit();
    unsafe {
        rl_get_screen_size(rows.as_mut_ptr(), cols.as_mut_ptr());
        return (rows.assume_init() as _, cols.assume_init() as _);
    }

    use std::os::raw::c_int;
    extern "C" {
        pub fn rl_get_screen_size(rows: *mut c_int, cols: *mut c_int);
    }
}

////////////////////////////////////////////////////////////////////////////////
/// An extention for [`std::any::Any`] that includes a `type_name` method for
/// getting the type name from a `dyn AnyWithTypeName`.
pub trait AnyWithTypeName: Any {
    fn type_name(&self) -> &'static str;

    fn into_box_dyn_any(self) -> Box<dyn AnyWithTypeName>
    where
        Self: Sized,
    {
        Box::new(self)
    }
}

impl<T: Any> AnyWithTypeName for T {
    #[inline]
    fn type_name(&self) -> &'static str {
        std::any::type_name::<T>()
    }
}

#[inline]
pub fn downcast<T: 'static>(any: Box<dyn AnyWithTypeName>) -> Result<T, Error> {
    if TypeId::of::<T>() != (*any).type_id() {
        return Err(Error::DowncastError {
            expected: std::any::type_name::<T>(),
            actual: (*any).type_name(),
        });
    }

    unsafe {
        let raw: *mut dyn AnyWithTypeName = Box::into_raw(any);
        Ok(*Box::from_raw(raw as *mut T))
    }
}

////////////////////////////////////////////////////////////////////////////////
/// A helper struct for displaying transitions between 2 values.
pub struct Transition<T, U> {
    pub from: T,
    pub to: U,
}

impl<T, U> std::fmt::Display for Transition<T, U>
where
    T: std::fmt::Display,
    U: std::fmt::Display,
    T: PartialEq<U>,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.from == self.to {
            write!(f, "{}", self.to)
        } else {
            write!(f, "{} -> {}", self.from, self.to)
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
/// Prompts a password from a terminal.
///
/// This function bypasses stdin redirection (like `cat script.lua |
/// picodata connect`) and always prompts a password from a TTY.
pub fn prompt_password(prompt: &str) -> Result<String, std::io::Error> {
    // See also: https://man7.org/linux/man-pages/man3/termios.3.html
    let mut tty = std::fs::File::options()
        .read(true)
        .write(true)
        .open("/dev/tty")?;
    let tty_fd = tty.as_raw_fd();
    let tcattr_old = tcgetattr(tty_fd)?;

    // Restore old terminal settings when `_guard` is dropped
    let _guard = on_scope_exit(|| tcsetattr(tty_fd, TCSADRAIN, &tcattr_old).unwrap_or(()));

    // Disable echo while prompting a password
    let mut tcattr_new = tcattr_old.clone();
    tcattr_new.local_flags.set(LocalFlags::ECHO, false);
    tcattr_new.local_flags.set(LocalFlags::ECHONL, true);
    tcsetattr(tty_fd, TCSADRAIN, &tcattr_new)?;

    // Print the prompt
    tty.write_all(prompt.as_bytes())?;
    tty.flush()?;

    // Read the password
    let mut password = String::new();
    BufReader::new(&tty).read_line(&mut password)?;

    if !password.ends_with('\n') {
        // Preliminary EOF, a user didn't hit enter
        return Err(std::io::Error::from(std::io::ErrorKind::Interrupted));
    }

    let crlf = |c| matches!(c, '\r' | '\n');
    Ok(password.trim_end_matches(crlf).to_owned())
}

////////////////////////////////////////////////////////////////////////////////
/// Returns a unix socket uri from the given file path.
///
/// Non-absolute paths are prepended with `./`.
///
/// Returns and error in case validation using lua `uri` module fails.
pub fn validate_and_complete_unix_socket_path(
    socket_path: impl AsRef<Path>,
) -> Result<String, Error> {
    let l = ::tarantool::lua_state();
    let path = socket_path.as_ref();
    let path_str = path.to_str().ok_or(Error::other(format!(
        "socket_path {} is not encoded in UTF-8",
        socket_path.as_ref().to_string_lossy()
    )))?;
    let path_str = if path.is_absolute() {
        format!("unix/:{path_str}")
    } else {
        format!("unix/:./{path_str}")
    };

    // Check that Lua can correctly parse the unix socket path
    l.exec_with(
        "local u = require('uri').parse(...); assert(u and u.unix)",
        &path_str,
    )
    .map_err(|_| Error::other(format!("invalid socket path: {}", path.display())))?;

    Ok(path_str)
}

////////////////////////////////////////////////////////////////////////////////
/// IsSameType

pub trait IsSameType<L, R> {
    type Void;
}

impl<T> IsSameType<T, T> for T {
    type Void = ();
}

#[allow(unused)]
pub type CheckIsSameType<L, R> = <L as IsSameType<L, R>>::Void;

////////////////////////////////////////////////////////////////////////////////
// no yields check
////////////////////////////////////////////////////////////////////////////////

/// A helper struct to enforce that a function must not yield. Will cause a
/// panic if fiber yields are detected when drop is called for it.
pub struct NoYieldsGuard {
    message: &'static str,
    csw: u64,
}

#[allow(clippy::new_without_default)]
impl NoYieldsGuard {
    #[inline(always)]
    pub fn new() -> Self {
        Self {
            message: "fiber yielded when it wasn't supposed to",
            csw: tarantool::fiber::csw(),
        }
    }

    #[inline(always)]
    pub fn with_message(message: &'static str) -> Self {
        Self {
            message,
            csw: tarantool::fiber::csw(),
        }
    }

    #[inline(always)]
    pub fn has_yielded(&self) -> bool {
        tarantool::fiber::csw() != self.csw
    }
}

impl Drop for NoYieldsGuard {
    #[inline(always)]
    fn drop(&mut self) {
        if self.has_yielded() {
            panic!("NoYieldsGuard: {}", self.message);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
// NoYieldsRefCell
////////////////////////////////////////////////////////////////////////////////

/// A `RefCell` wrapper which also enforces that the wrapped value is never
/// borrowed across fiber yields.
#[derive(Debug)]
pub struct NoYieldsRefCell<T> {
    inner: std::cell::RefCell<T>,
    loc: Cell<&'static Location<'static>>,
}

impl<T> Default for NoYieldsRefCell<T>
where
    T: Default,
{
    #[inline(always)]
    #[track_caller]
    fn default() -> Self {
        Self {
            inner: Default::default(),
            loc: Cell::new(Location::caller()),
        }
    }
}

impl<T> NoYieldsRefCell<T> {
    #[inline(always)]
    #[track_caller]
    pub fn new(inner: T) -> Self {
        Self {
            inner: std::cell::RefCell::new(inner),
            loc: Cell::new(Location::caller()),
        }
    }

    #[inline(always)]
    #[track_caller]
    pub fn borrow(&self) -> NoYieldsRef<'_, T> {
        self.loc.set(Location::caller());
        let inner = self.inner.borrow();
        let guard =
            NoYieldsGuard::with_message("yield detected while NoYieldsRefCell was borrowed");
        NoYieldsRef { inner, guard }
    }

    #[inline(always)]
    #[track_caller]
    pub fn borrow_mut(&self) -> NoYieldsRefMut<'_, T> {
        let Ok(inner) = self.inner.try_borrow_mut() else {
            panic!("already borrowed at {}", self.loc.get());
        };
        self.loc.set(Location::caller());
        let guard =
            NoYieldsGuard::with_message("yield detected while NoYieldsRefCell was borrowed");
        NoYieldsRefMut { inner, guard }
    }
}

pub struct NoYieldsRef<'a, T> {
    inner: std::cell::Ref<'a, T>,
    /// This is only needed for it's `Drop` implementation.
    #[allow(unused)]
    guard: NoYieldsGuard,
}

impl<T> std::ops::Deref for NoYieldsRef<'_, T> {
    type Target = T;

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

pub struct NoYieldsRefMut<'a, T> {
    inner: std::cell::RefMut<'a, T>,
    /// This is only needed for it's `Drop` implementation.
    #[allow(unused)]
    guard: NoYieldsGuard,
}

impl<T> std::ops::Deref for NoYieldsRefMut<'_, T> {
    type Target = T;

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<T> std::ops::DerefMut for NoYieldsRefMut<'_, T> {
    #[inline(always)]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

////////////////////////////////////////////////////////////////////////////////
// ScopeGuard
////////////////////////////////////////////////////////////////////////////////
// TODO: this one is copied from tarantool-module, it should instead be export from there

#[derive(Debug)]
#[must_use = "The callback is invoked when the `ScopeGuard` is dropped"]
pub struct ScopeGuard<F>
where
    F: FnOnce(),
{
    cb: Option<F>,
}

impl<F> Drop for ScopeGuard<F>
where
    F: FnOnce(),
{
    #[inline(always)]
    fn drop(&mut self) {
        if let Some(cb) = self.cb.take() {
            cb()
        }
    }
}

#[inline(always)]
pub fn on_scope_exit<F>(cb: F) -> ScopeGuard<F>
where
    F: FnOnce(),
{
    ScopeGuard { cb: Some(cb) }
}

////////////////////////////////////////////////////////////////////////////////
// Lexer
////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Default, PartialEq, Eq)]
pub enum QuoteEscapingStyle {
    /// "quote\"in string"for 'single\'quote'
    #[default]
    Backslash,
    /// 'this is a '' single quote'
    DoubleSingleQuote,
}

pub struct Lexer<'a> {
    input: &'a str,
    utf8_stream: std::iter::Peekable<std::str::CharIndices<'a>>,
    last_token: Option<TokenInfo<'a>>,
    last_token_was_peeked: bool,
    quote_escaping_style: QuoteEscapingStyle,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TokenInfo<'a> {
    pub text: &'a str,
    pub start: usize,
    pub end: usize,
    pub utf8_count: usize,
}

impl<'a> Lexer<'a> {
    #[inline(always)]
    pub fn new(input: &'a str) -> Self {
        Self {
            utf8_stream: input.char_indices().peekable(),
            input,
            last_token: None,
            last_token_was_peeked: false,
            quote_escaping_style: QuoteEscapingStyle::Backslash,
        }
    }

    #[inline(always)]
    pub fn set_quote_escaping_style(&mut self, quote_escaping_style: QuoteEscapingStyle) {
        self.quote_escaping_style = quote_escaping_style;
    }

    #[inline(always)]
    pub fn peek_token(&mut self) -> Option<&TokenInfo<'a>> {
        if !self.last_token_was_peeked {
            self.next_token();
            self.last_token_was_peeked = true;
        }
        self.last_token.as_ref()
    }

    pub fn next_token(&mut self) -> Option<&TokenInfo<'a>> {
        if self.last_token_was_peeked {
            self.last_token_was_peeked = false;
            return self.last_token.as_ref();
        }

        // skip leading whitespace
        while let Some(&(_, c)) = self.utf8_stream.peek() {
            if !c.is_whitespace() {
                break;
            }
            _ = self.utf8_stream.next();
        }

        let Some((i, c)) = self.utf8_stream.next() else {
            self.last_token = None;
            return None;
        };

        let start = i;
        let mut utf8_count = 1;

        match c {
            c if is_alphanumeric_or_underscore(c) => {
                while let Some(&(i, c)) = self.utf8_stream.peek() {
                    if !is_alphanumeric_or_underscore(c) {
                        return Some(self.update_last_token(start, i, utf8_count));
                    }
                    utf8_count += 1;
                    _ = self.utf8_stream.next().expect("peek returned Some");
                }
                // in case stream ended, fall through to handled it at the end
            }
            '"' | '\'' if self.quote_escaping_style == QuoteEscapingStyle::Backslash => {
                let openning_quote = c;
                while let Some((i, c)) = self.utf8_stream.next() {
                    utf8_count += 1;
                    if c == '\\' {
                        // next character is escaped, so always added to the token
                        if self.utf8_stream.next().is_some() {
                            utf8_count += 1;
                        } else {
                            // end of input
                        }
                    } else if c == openning_quote {
                        let end = i + c.len_utf8();
                        return Some(self.update_last_token(start, end, utf8_count));
                    }
                }
                // in case stream ended, fall through to handled it at the end
            }
            '\'' if self.quote_escaping_style == QuoteEscapingStyle::DoubleSingleQuote => {
                while let Some((i, c)) = self.utf8_stream.next() {
                    utf8_count += 1;
                    if c == '\'' {
                        if let Some((_, '\'')) = self.utf8_stream.peek() {
                            // an escaped single quote character
                            utf8_count += 1;
                            _ = self.utf8_stream.next().expect("peek returned Some");
                        } else {
                            // not a quote or input ended => string literal ended
                            let end = i + c.len_utf8();
                            return Some(self.update_last_token(start, end, utf8_count));
                        }
                    }
                }
                // in case stream ended, fall through to handled it at the end
            }
            _ => { /* a single character token, handled bellow */ }
        }

        let end = if let Some(&(i, _)) = self.utf8_stream.peek() {
            i
        } else {
            self.input.len()
        };
        return Some(self.update_last_token(start, end, utf8_count));
    }

    #[inline(always)]
    fn update_last_token(&mut self, start: usize, end: usize, utf8_count: usize) -> &TokenInfo<'a> {
        self.last_token.insert(TokenInfo {
            text: &self.input[start..end],
            start,
            end,
            utf8_count,
        })
    }
}

#[inline(always)]
fn is_alphanumeric_or_underscore(c: char) -> bool {
    c == '_' || c.is_alphanumeric()
}

////////////////////////////////////////////////////////////////////////////////
// ...
////////////////////////////////////////////////////////////////////////////////

#[inline(always)]
pub fn file_exists(path: impl AsRef<Path>) -> bool {
    std::fs::metadata(path).is_ok()
}

#[inline]
pub(crate) fn effective_user_id() -> UserId {
    session::euid().expect("infallible in picodata")
}

#[cfg(test)]
use tarantool::space::Field;
#[cfg(test)]
#[track_caller]
pub fn check_tuple_matches_format(tuple: &[u8], format: &[Field], what_to_fix: &str) {
    use tarantool::space::FieldType;
    use tarantool::tuple::Decode;

    let value = rmpv::Value::decode(tuple).unwrap();
    let fields = value.as_array().unwrap();
    assert_eq!(
        fields.len(),
        format.len(),
        "don't forget to update {what_to_fix}!"
    );

    for i in 0..fields.len() {
        let field = &fields[i];
        let field_type = format[i].field_type;
        let field_name = &format[i].name;
        let ok = match field_type {
            FieldType::Any => true,
            FieldType::Unsigned => field.is_u64(),
            FieldType::String => field.is_str(),
            FieldType::Number => field.is_number(),
            FieldType::Double => field.is_f32() || field.is_f64(),
            FieldType::Integer => field.is_i64(),
            FieldType::Boolean => field.is_bool(),
            FieldType::Varbinary => todo!(),
            FieldType::Scalar => todo!(),
            FieldType::Decimal | FieldType::Uuid | FieldType::Datetime | FieldType::Interval => {
                field.is_ext()
            }
            FieldType::Array => field.is_array(),
            FieldType::Map => field.is_map(),
        };
        if !ok {
            panic!("expected field '{field_name}' to be {field_type:?}, but got {field:?}");
        }
    }
}

/// Returns the number of character edit operations needed to convert `lhs` to `rhs`.
///
/// By operations we mean
/// - insert character
/// - remove character
/// - replace one character with another
///
/// # Examples
/// ```rust
/// # use picodata::util::edit_distance;
/// assert_eq!(edit_distance("instance-id", "instance_id"), 1);
/// assert_eq!(edit_distance("foo", "bar"), 3);
/// assert_eq!(edit_distance("care", "scar"), 2);
/// ```
pub fn edit_distance(lhs: &str, rhs: &str) -> usize {
    let mut l_size = lhs.chars().count();
    let mut r_size = rhs.chars().count();

    if l_size == 0 {
        return r_size;
    } else if r_size == 0 {
        return l_size;
    }

    // Make rhs always be the shorter string, to minimize memory allocation.
    let (lhs, rhs) = if l_size < r_size {
        std::mem::swap(&mut l_size, &mut r_size);
        (rhs, lhs)
    } else {
        (lhs, rhs)
    };

    let n = r_size + 1;
    // In the regular Damerau-Levenshtein algorithm we need to compute the
    // matrix of distances between all the string prefixes of `lhs` and `rhs`.
    // Our particular implementation has an optimization where we only store at
    // most 1 row of that distance_matrix.
    // For full algorithm definition see <https://en.wikipedia.org/wiki/Damerau-Levenshtein_distance>
    let mut current_row = vec![0; n];
    // This warning bellow in this case is the stupidest suggestion by clippy yet
    #[allow(clippy::needless_range_loop)]
    for i in 0..n {
        current_row[i] = i;
    }

    for (l_char, i) in lhs.chars().zip(1..) {
        // On the previous iteration of this loop (if any) `current_row` was
        // assigned the values for the previous row of "distance_matrix",
        // so this value is equivalent to distance_matrix[i - 1][0]
        let mut previous_row = replace(&mut current_row[0], i);

        for (r_char, j) in rhs.chars().zip(1..) {
            let d = if l_char != r_char { 1 } else { 0 };

            // Equivalent to distance_matrix[i - 1][j - 1]
            let previous_diagonal = previous_row;
            let substitute_cost = previous_diagonal + d;

            // Equivalent to distance_matrix[i - 1][j]
            previous_row = current_row[j];
            let delete_cost = previous_row + 1;

            // Equivalent to distance_matrix[i][j - 1]
            let previous_column = current_row[j - 1];
            let insert_cost = previous_column + 1;

            let distance = substitute_cost.min(delete_cost).min(insert_cost);
            // Equivalent to distance_matrix[i][j]
            current_row[j] = distance;
        }
    }

    current_row[current_row.len() - 1]
}

////////////////////////////////////////////////////////////////////////////////
/// tests
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uppercase() {
        use super::Uppercase;
        assert_eq!(&*Uppercase::from(""), "");
        assert_eq!(&*Uppercase::from("hello"), "HELLO");
        assert_eq!(&*Uppercase::from("HELLO"), "HELLO");
        assert_eq!(&*Uppercase::from("123-?!"), "123-?!");
        assert_eq!(&*Uppercase::from(String::from("hello")), "HELLO");
        assert_eq!(&*Uppercase::from(String::from("HELLO")), "HELLO");
    }

    #[test]
    fn str_eq() {
        use super::str_eq;
        assert!(str_eq("", ""));
        assert!(str_eq("a", "a"));
        assert!(str_eq("\0b", "\0b"));
        assert!(str_eq("foobar", concat!("foo", "bar")));

        assert!(!str_eq("", "x"));
        assert!(!str_eq("x", ""));
        assert!(!str_eq("x", "y"));
        assert!(!str_eq("ы", "Ы"));
        assert!(!str_eq("\0x", "\0y"));
        assert!(!str_eq("foo1", "bar1"));
        assert!(!str_eq("foo1", "foo2"));
    }

    #[test]
    fn downcast() {
        assert_eq!(super::downcast::<u8>(Box::new(13_u8)).unwrap(), 13);
        let err = super::downcast::<i8>(Box::new(13_u8)).unwrap_err();
        assert_eq!(
            err.to_string(),
            r#"downcast error: expected "i8", actual: "u8""#
        );
    }

    #[test]
    fn check_edit_distance() {
        assert_eq!(edit_distance("", ""), 0);
        assert_eq!(edit_distance("", "a"), 1);
        assert_eq!(edit_distance("aba", ""), 3);
        assert_eq!(edit_distance("abba", "baba"), 2);
        assert_eq!(edit_distance("instance-id", "instance_id"), 1);
        assert_eq!(edit_distance("буква-w", "буква-ю"), 1);
        assert_eq!(edit_distance("thouroughness", "abandonment"), 11);
        assert_eq!(edit_distance("lonesome", "somebody"), 5);
    }

    #[test]
    fn lexer() {
        //
        //
        //

        let mut lexer = Lexer::new(
            r##"foo bar1   3baz " \" ' "  'single\'quotes' 'double"in singles',,) "unfinished"##,
        );

        let mut tokens = vec![];
        while let Some(token) = lexer.next_token().copied() {
            assert_eq!(token.text, &lexer.input[token.start..token.end]);
            assert_eq!(token.utf8_count, dbg!(token.text).chars().count());
            tokens.push(token.text);
        }

        assert_eq!(
            tokens,
            [
                "foo",
                "bar1",
                "3baz",
                "\" \\\" ' \"",
                "'single\\'quotes'",
                "'double\"in singles'",
                ",",
                ",",
                ")",
                "\"unfinished"
            ]
        );

        //
        //
        //
        let mut lexer = Lexer::new("   alphanumeric");

        let mut tokens = vec![];
        while let Some(token) = lexer.next_token().copied() {
            assert_eq!(token.text, &lexer.input[token.start..token.end]);
            assert_eq!(token.utf8_count, dbg!(token.text).chars().count());
            tokens.push(token.text);
        }

        assert_eq!(tokens, ["alphanumeric"]);

        //
        //
        //
        let mut lexer = Lexer::new("quotes_at_the_end''");

        let mut tokens = vec![];
        while let Some(token) = lexer.next_token().copied() {
            assert_eq!(token.text, &lexer.input[token.start..token.end]);
            assert_eq!(token.utf8_count, dbg!(token.text).chars().count());
            tokens.push(token.text);
        }

        assert_eq!(tokens, ["quotes_at_the_end", "''"]);

        //
        //
        //
        let mut lexer = Lexer::new("backslash_at_the_end'\\");

        let mut tokens = vec![];
        while let Some(token) = lexer.next_token().copied() {
            assert_eq!(token.text, &lexer.input[token.start..token.end]);
            assert_eq!(token.utf8_count, dbg!(token.text).chars().count());
            tokens.push(token.text);
        }

        assert_eq!(tokens, ["backslash_at_the_end", "'\\"]);

        //
        //
        //
        let mut lexer = Lexer::new("foo bar");
        assert_eq!(lexer.next_token().unwrap().text, "foo");
        assert_eq!(lexer.peek_token().unwrap().text, "bar");
        assert_eq!(lexer.peek_token().unwrap().text, "bar");
        assert_eq!(lexer.next_token().unwrap().text, "bar");

        assert!(lexer.peek_token().is_none());
        assert!(lexer.peek_token().is_none());
        assert!(lexer.next_token().is_none());

        //
        //
        //
        let mut lexer =
            Lexer::new("single_quote '''' double_single_quote '''''' apostrophe 'parsn''t' end ''");
        lexer.set_quote_escaping_style(QuoteEscapingStyle::DoubleSingleQuote);

        let mut tokens = vec![];
        while let Some(token) = lexer.next_token().copied() {
            assert_eq!(token.text, &lexer.input[token.start..token.end]);
            assert_eq!(token.utf8_count, dbg!(token.text).chars().count());
            tokens.push(token.text);
        }

        assert_eq!(
            tokens,
            [
                "single_quote",
                "''''",
                "double_single_quote",
                "''''''",
                "apostrophe",
                "'parsn''t'",
                "end",
                "''",
            ]
        );
    }
}
