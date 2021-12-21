use std::{
    convert::TryFrom,
};

use tarantool::{
    decimal,
    decimal::Decimal,
    tlua,
    tuple::Tuple,
};

pub fn from_lua() {
    let lua = tarantool::global_lua();
    let d: Decimal = lua.eval("return require('decimal').new('-8.11')").unwrap();
    assert_eq!(d.to_string(), "-8.11");
}

pub fn to_lua() {
    let lua = tarantool::global_lua();
    let tostring: tlua::LuaFunction<_> = lua.eval("return tostring").unwrap();
    let d: Decimal = "-8.11".parse().unwrap();
    let s: String = tostring.call_with_args(d).unwrap();
    assert_eq!(s, "-8.11");
}

pub fn from_string() {
    let d: Decimal = "-81.1e-1".parse().unwrap();
    assert_eq!(d.to_string(), "-8.11");
    assert_eq!(decimal!(-81.1e-1).to_string(), "-8.11");

    assert_eq!("foobar".parse::<Decimal>().ok(), None::<Decimal>);
    assert_eq!("".parse::<Decimal>().ok(), None::<Decimal>);

    // tarantool decimals don't support infinity or NaN
    assert_eq!("inf".parse::<Decimal>().ok(), None::<Decimal>);
    assert_eq!("infinity".parse::<Decimal>().ok(), None::<Decimal>);
    assert_eq!("NaN".parse::<Decimal>().ok(), None::<Decimal>);
}

pub fn from_tuple() {
    let lua = tarantool::global_lua();
    let t: Tuple = lua.eval("return box.tuple.new(require('decimal').new('-8.11'))").unwrap();
    let (d,): (Decimal,) = t.as_struct().unwrap();
    assert_eq!(d.to_string(), "-8.11");
}

pub fn to_tuple() {
    let d = decimal!(-8.11);
    let t = Tuple::from_struct(&(d,)).unwrap();
    let lua = tarantool::global_lua();
    let f: tlua::LuaFunction<_> = lua.eval("return box.tuple.unpack").unwrap();
    let d: Decimal = f.call_with_args(t).unwrap();
    assert_eq!(d.to_string(), "-8.11");
}

pub fn from_num() {
    assert_eq!(Decimal::from(0i8), Decimal::zero());
    assert_eq!(Decimal::from(42i8).to_string(), "42");
    assert_eq!(Decimal::from(i8::MAX).to_string(), "127");
    assert_eq!(Decimal::from(i8::MIN).to_string(), "-128");
    assert_eq!(Decimal::from(0i16), Decimal::zero());
    assert_eq!(Decimal::from(42i16).to_string(), "42");
    assert_eq!(Decimal::from(i16::MAX).to_string(), "32767");
    assert_eq!(Decimal::from(i16::MIN).to_string(), "-32768");
    assert_eq!(Decimal::from(0i32), Decimal::zero());
    assert_eq!(Decimal::from(42i32).to_string(), "42");
    assert_eq!(Decimal::from(i32::MAX).to_string(), "2147483647");
    assert_eq!(Decimal::from(i32::MIN).to_string(), "-2147483648");
    assert_eq!(Decimal::from(0i64), Decimal::zero());
    assert_eq!(Decimal::from(42i64).to_string(), "42");
    assert_eq!(Decimal::from(i64::MAX).to_string(), "9223372036854775807");
    assert_eq!(Decimal::from(i64::MIN).to_string(), "-9223372036854775808");
    assert_eq!(Decimal::from(0isize), Decimal::zero());
    assert_eq!(Decimal::from(42isize).to_string(), "42");
    assert_eq!(Decimal::from(isize::MAX).to_string(), "9223372036854775807");
    assert_eq!(Decimal::from(isize::MIN).to_string(), "-9223372036854775808");

    assert_eq!(Decimal::from(0u8), Decimal::zero());
    assert_eq!(Decimal::from(42u8).to_string(), "42");
    assert_eq!(Decimal::from(u8::MAX).to_string(), "255");
    assert_eq!(Decimal::from(0u16), Decimal::zero());
    assert_eq!(Decimal::from(42u16).to_string(), "42");
    assert_eq!(Decimal::from(u16::MAX).to_string(), "65535");
    assert_eq!(Decimal::from(0u32), Decimal::zero());
    assert_eq!(Decimal::from(42u32).to_string(), "42");
    assert_eq!(Decimal::from(u32::MAX).to_string(), "4294967295");
    assert_eq!(Decimal::from(0u64), Decimal::zero());
    assert_eq!(Decimal::from(42u64).to_string(), "42");
    assert_eq!(Decimal::from(u64::MAX).to_string(), "18446744073709551615");
    assert_eq!(Decimal::from(0usize), Decimal::zero());
    assert_eq!(Decimal::from(42usize).to_string(), "42");
    assert_eq!(Decimal::from(usize::MAX).to_string(), "18446744073709551615");

    assert_eq!(Decimal::try_from(0f32).unwrap(), Decimal::zero());
    assert_eq!(Decimal::try_from(-8.11f32).unwrap().to_string(), "-8.10999965667725");
    assert_eq!(Decimal::try_from(f32::INFINITY).unwrap_err().to_string(), "float is infinite");
    assert_eq!(Decimal::try_from(f32::NEG_INFINITY).unwrap_err().to_string(), "float is infinite");
    assert_eq!(Decimal::try_from(f32::NAN).unwrap_err().to_string(), "float is NaN");
    assert_eq!(Decimal::try_from(f32::EPSILON).unwrap().to_string(), "0.000000119209289550781");
    assert_eq!(Decimal::try_from(f32::MIN).unwrap_err().to_string(),
        "float `-340282350000000000000000000000000000000` cannot be represented using 38 digits"
    );
    assert_eq!(Decimal::try_from(f32::MAX).unwrap_err().to_string(),
        "float `340282350000000000000000000000000000000` cannot be represented using 38 digits"
    );
    assert_eq!(Decimal::try_from(1.0e-40_f32).unwrap(), Decimal::zero());
    assert_eq!(Decimal::try_from(1e38_f32).unwrap().to_string(),
        "99999996802856900000000000000000000000"
    );

    assert_eq!(Decimal::try_from(0f64).unwrap(), Decimal::zero());
    assert_eq!(Decimal::try_from(-8.11f64).unwrap().to_string(), "-8.11");
    assert_eq!(Decimal::try_from(f64::INFINITY).unwrap_err().to_string(), "float is infinite");
    assert_eq!(Decimal::try_from(f64::NEG_INFINITY).unwrap_err().to_string(), "float is infinite");
    assert_eq!(Decimal::try_from(f64::NAN).unwrap_err().to_string(), "float is NaN");
    assert_eq!(Decimal::try_from(f64::EPSILON).unwrap().to_string(), "0.000000000000000222044604925031");
    assert_eq!(Decimal::try_from(f64::MIN).unwrap_err().to_string(),
        "float `-179769313486231570000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000` cannot be represented using 38 digits"
    );
    assert_eq!(Decimal::try_from(f64::MAX).unwrap_err().to_string(),
        "float `179769313486231570000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000` cannot be represented using 38 digits"
    );
    assert_eq!(Decimal::try_from(1.0e-40_f64).unwrap(), Decimal::zero());
    assert_eq!(Decimal::try_from(1e38_f64).unwrap_err().to_string(),
        "float `100000000000000000000000000000000000000` cannot be represented using 38 digits"
    );
}

pub fn to_num() {
    assert_eq!(i64::try_from(decimal!(420)).unwrap(), 420);
    assert_eq!(i64::try_from(decimal!(9223372036854775807)).unwrap(), i64::MAX);
    assert_eq!(i64::try_from(decimal!(9223372036854775808)).unwrap_err().to_string(),
        "decimal is out of range"
    );
    assert_eq!(i64::try_from(decimal!(-9223372036854775808)).unwrap(), i64::MIN);
    assert_eq!(i64::try_from(decimal!(-9223372036854775809)).unwrap_err().to_string(),
        "decimal is out of range"
    );
    assert_eq!(i64::try_from(decimal!(3.14)).unwrap_err().to_string(),
        "decimal is not an integer"
    );

    assert_eq!(isize::try_from(decimal!(420)).unwrap(), 420);
    assert_eq!(isize::try_from(decimal!(9223372036854775807)).unwrap(), isize::MAX);
    assert_eq!(isize::try_from(decimal!(9223372036854775808)).unwrap_err().to_string(),
        "decimal is out of range"
    );
    assert_eq!(isize::try_from(decimal!(-9223372036854775808)).unwrap(), isize::MIN);
    assert_eq!(isize::try_from(decimal!(-9223372036854775809)).unwrap_err().to_string(),
        "decimal is out of range"
    );
    assert_eq!(isize::try_from(decimal!(3.14)).unwrap_err().to_string(),
        "decimal is not an integer"
    );

    assert_eq!(u64::try_from(decimal!(420)).unwrap(), 420);
    assert_eq!(u64::try_from(decimal!(18446744073709551615)).unwrap(), u64::MAX);
    assert_eq!(u64::try_from(decimal!(18446744073709551616)).unwrap_err().to_string(),
        "decimal is out of range"
    );
    assert_eq!(u64::try_from(decimal!(-1)).unwrap_err().to_string(),
        "decimal is out of range"
    );
    assert_eq!(u64::try_from(decimal!(3.14)).unwrap_err().to_string(),
        "decimal is not an integer"
    );

    assert_eq!(usize::try_from(decimal!(420)).unwrap(), 420);
    assert_eq!(usize::try_from(decimal!(18446744073709551615)).unwrap(), usize::MAX);
    assert_eq!(usize::try_from(decimal!(18446744073709551616)).unwrap_err().to_string(),
        "decimal is out of range"
    );
    assert_eq!(usize::try_from(decimal!(-1)).unwrap_err().to_string(),
        "decimal is out of range"
    );
    assert_eq!(usize::try_from(decimal!(3.14)).unwrap_err().to_string(),
        "decimal is not an integer"
    );

}

pub fn cmp() {
    assert!(decimal!(.1) < decimal!(.2));
    assert!(decimal!(.1) <= decimal!(.2));
    assert!(decimal!(.2) > decimal!(.1));
    assert!(decimal!(.2) >= decimal!(.1));

    assert_eq!(decimal!(0), 0);
    assert_eq!(decimal!(1), 1);
    assert_eq!(decimal!(-3), -3);
    assert_ne!(decimal!(-8.11), -8);
}

pub fn ops() {
    let a = decimal!(.1);
    let b = decimal!(.2);
    let c = decimal!(.3);
    assert_eq!(a + b, c);
    assert_eq!(c - b, a);
    assert_eq!(c - a, b);
    assert_eq!(b * c, decimal!(.06));
    assert_eq!(c / b, decimal!(1.5));

    let mut x = decimal!(.5);
    x += 1;
    assert_eq!(x, decimal!(1.5));
    x -= 2;
    assert_eq!(x, decimal!(-.5));
    x *= 3;
    assert_eq!(x, decimal!(-1.5));
    x /= 4;
    assert_eq!(x, decimal!(-.375));
    x %= 5;
    assert_eq!(x, decimal!(-.375));
    x += 12;
    assert_eq!(x, decimal!(11.625));
    assert_eq!(x % 5, decimal!(1.625));

    let x: Decimal = decimal!(99999999999999999999999999999999999999);
    let y: Decimal = 1.into();
    assert_eq!(x.checked_add(y), None::<Decimal>);

    let x: Decimal = decimal!(10000000000000000000000000000000000000);
    let y: Decimal = 10.into();
    assert_eq!(x.checked_mul(y), None::<Decimal>);

    let x = decimal!(-8.11);
    let y = x.abs();
    assert_eq!(y, -x);
    assert_eq!(y, decimal!(8.11));

    let x = decimal!(1.000);
    assert_eq!(x.to_string(), "1.000");
    assert_eq!(x.precision(), 4);
    assert_eq!(x.scale(), 3);
    let x = x.trim();
    assert_eq!(x.to_string(), "1");
    assert_eq!(x.precision(), 1);
    assert_eq!(x.scale(), 0);
    let x = x.rescale(3).unwrap();
    assert_eq!(x.to_string(), "1.000");
    assert_eq!(x.precision(), 4);
    assert_eq!(x.scale(), 3);

    assert_eq!(decimal!(100).log10(), decimal!(2));
    assert_eq!(decimal!(.01).log10(), decimal!(-2));

    let e = decimal!(1).exp().unwrap();
    assert_eq!(e, decimal!(2.7182818284590452353602874713526624978));
    assert_eq!(decimal!(1000).exp(), None::<Decimal>);

    assert_eq!(e.precision(), 38);
    assert_eq!(e.scale(), 37);
    assert_eq!(e.is_int(), false);
    assert_eq!(e.round().precision(), 1);
    assert_eq!(e.round().scale(), 0);
    assert_eq!(e.round().is_int(), true);

    assert_eq!(Decimal::from(usize::MAX).precision(), 20);

    assert_eq!(e.round_to(4), Some(decimal!(2.7183)));
    assert_eq!(e.floor_to(4), Some(decimal!(2.7182)));
    assert_eq!(e.round_to(40), None::<Decimal>);
    assert_eq!(e.floor_to(40), None::<Decimal>);

    assert_eq!(decimal!(1).ln(), decimal!(0));
    assert_eq!(e.ln(), decimal!(1));

    assert_eq!(decimal!(4).sqrt(), Some(decimal!(2)));
    assert_eq!(decimal!(-1).sqrt(), None::<Decimal>);

    assert_eq!(decimal!(2).pow(64), Some(decimal!(18446744073709551616)));
    assert_eq!(decimal!(2).pow(-2), Some(decimal!(.25)));
    assert_eq!(decimal!(10).pow(39), None::<Decimal>);
}

