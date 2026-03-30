#[macro_export]
macro_rules! define_str_enum_or_smol_str {
    (
        $(#[$enum_meta:meta])*
        $vis:vis enum $enum:ident {
            $(
                $(#[$variant_meta:meta])*
                $variant:ident = $display:literal,
            )+

            @unknown

            $(#[$variant_unknown_meta:meta])*
            $unknown:ident ( $SmolStr:ty ) $(,)?
        }
    ) => {
        const _ONLY_SMOLSTR_SUPPORTED: $crate::util::CheckIsSameType<$SmolStr, ::smol_str::SmolStr> = ();

        $(#[$enum_meta])*
        #[derive(Debug, Clone, PartialEq, Eq, Hash)]
        $vis enum $enum {
            $(
                $(#[$variant_meta])*
                $variant,
            )+
            $(#[$variant_unknown_meta])*
            $unknown($SmolStr),
        }

        impl $enum {
            pub fn as_str(&self) -> &str {
                match self {
                    $( Self::$variant => $display, )+
                    Self::$unknown(u) => u,
                }
            }
        }

        impl ::std::fmt::Display for $enum {
            fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
                f.write_str(self.as_str())
            }
        }

        impl From<$enum> for $SmolStr {
            fn from(v: $enum) -> Self {
                match v {
                    $( $enum::$variant => <$SmolStr>::new_static($display), )+
                    $enum::$unknown(s) => s,
                }
            }
        }

        impl ::std::str::FromStr for $enum {
            type Err = ::std::convert::Infallible;

            fn from_str(s: &str) -> ::std::result::Result<Self, Self::Err> {
                let res = match s {
                    $( $display => Self::$variant, )+
                    unknown => Self::$unknown(unknown.into()),
                };
                Ok(res)
            }
        }

        impl ::serde::Serialize for $enum {
            #[inline]
            fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                serializer.serialize_str(self.as_str())
            }
        }

        impl<'de> ::serde::Deserialize<'de> for $enum {
            #[inline]
            fn deserialize<D>(deserializer: D) -> ::std::result::Result<Self, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                deserializer.deserialize_str(::tarantool::define_str_enum::FromStrVisitor::<Self>::default())
            }
        }
    }

}
