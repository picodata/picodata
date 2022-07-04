#[derive(Debug, PartialEq, Eq, Hash)]
pub enum Either<L, R> {
    Left(L),
    Right(R),
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
    ($($arg:tt)*) => {
        $crate::tlog!(Warning, $($arg)*);
        if cfg!(debug_assertions) {
            panic!($($arg)*);
        }
    };
}
