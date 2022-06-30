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
