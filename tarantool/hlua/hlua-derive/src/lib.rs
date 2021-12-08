use proc_macro2::{TokenStream, Span};
use quote::quote;
use syn::{parse_macro_input, DeriveInput, Ident};

#[proc_macro_derive(Push)]
pub fn proc_macro_derive_push(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    // TODO(gmoshkin): add an attribute to specify path to hlua module (see serde)
    // TODO(gmoshkin): add support for types with generic type parameters
    let name = &input.ident;
    let push_code = Info::new(&input).push();

    let expanded = quote! {
        impl<L> hlua::Push<L> for #name
        where
            L: hlua::AsLua,
        {
            type Err = hlua::Void;

            fn push_to_lua(&self, __lua: L)
                -> ::std::result::Result<hlua::PushGuard<L>, (Self::Err, L)>
            {
                Ok(#push_code)
            }
        }

        impl<L> hlua::PushOne<L> for #name
        where
            L: hlua::AsLua,
        {
        }
    };

    expanded.into()
}

#[proc_macro_derive(LuaRead)]
pub fn proc_macro_derive_lua_read(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;
    let info = Info::new(&input);
    let read_at_code = info.read();
    let maybe_n_values_expected = info.n_values();
    let maybe_lua_read = info.read_top();

    let expanded = quote! {
        impl<L> hlua::LuaRead<L> for #name
        where
            L: hlua::AsLua,
        {
            #maybe_n_values_expected

            #maybe_lua_read

            fn lua_read_at_position(__lua: L, __index: ::std::num::NonZeroI32)
                -> ::std::result::Result<Self, L>
            {
                #read_at_code
            }
        }
    };

    expanded.into()
}

enum Info<'a> {
    Struct(FieldsInfo<'a>),
    Enum(VariantsInfo<'a>),
}

impl<'a> Info<'a> {
    fn new(input: &'a DeriveInput) -> Self {
        match input.data {
            syn::Data::Struct(ref s) => {
                if let Some(fields) = FieldsInfo::new(&s.fields) {
                    Self::Struct(fields)
                } else {
                    unimplemented!("standalone unit structs aren't supproted yet")
                }
            }
            syn::Data::Enum(ref e) => Self::Enum(VariantsInfo::new(e)),
            syn::Data::Union(_) => unimplemented!("unions will never be supported"),
        }
    }

    fn push(&self) -> TokenStream {
        match self {
            Self::Struct(f) => {
                if matches!(f, FieldsInfo::Unnamed { .. }) {
                    unimplemented!("tuple structs are not supported")
                }
                let fields = f.pattern();
                let push_fields = f.push();
                quote! {
                    match self {
                        Self #fields => #push_fields,
                    }
                }
            }
            Self::Enum(v) => {
                let push_variants = v.variants.iter()
                    .map(VariantInfo::push)
                    .collect::<Vec<_>>();
                quote! {
                    match self {
                        #( #push_variants )*
                    }
                }
            }
        }
    }

    fn read(&self) -> TokenStream {
        match self {
            Self::Struct(f) => {
                if matches!(f, FieldsInfo::Unnamed { .. }) {
                    unimplemented!("tuple structs are not supported")
                }
                f.read_as(quote! { Self })
            }
            Self::Enum(v) => {
                let read_and_maybe_return_variant = v.variants.iter()
                    .map(VariantInfo::read_and_maybe_return)
                    .collect::<Vec<_>>();
                quote! {
                    #(
                        let __lua = #read_and_maybe_return_variant;
                    )*
                    Err(__lua)
                }
            }
        }
    }

    fn read_top(&self) -> TokenStream {
        match self {
            Self::Struct(_) => quote!{},
            Self::Enum(v) => {
                let mut n_vals = vec![];
                let mut read_and_maybe_return = vec![];
                for variant in &v.variants {
                    n_vals.push(
                        if let Some(ref fields) = variant.info {
                            fields.n_values()
                        } else {
                            quote! { 1 }
                        }
                    );
                    read_and_maybe_return.push(variant.read_and_maybe_return());
                }
                quote! {
                    fn lua_read(__lua: L) -> ::std::result::Result<Self, L> {
                        let top = unsafe { hlua::ffi::lua_gettop(__lua.as_lua()) };
                        #(
                            let n_vals = #n_vals;
                            let __lua = if top >= n_vals {
                                let __index = unsafe {
                                    ::std::num::NonZeroI32::new_unchecked(top - n_vals + 1)
                                };
                                #read_and_maybe_return
                            } else {
                                __lua
                            };
                        )*
                        Err(__lua)
                    }
                }
            }
        }
    }

    fn n_values(&self) -> TokenStream {
        match self {
            Self::Struct(fields) => {
                let n_values = fields.n_values();
                quote! {
                    #[inline(always)]
                    fn n_values_expected() -> i32 {
                        #n_values
                    }
                }
            }
            Self::Enum(_) => {
                quote! {}
            }
        }
    }
}

enum FieldsInfo<'a> {
    Named {
        n_rec: i32,
        field_names: Vec<String>,
        field_idents: Vec<&'a Ident>,
    },
    Unnamed {
        field_idents: Vec<Ident>,
        field_types: Vec<&'a syn::Type>,
    },
}

impl<'a> FieldsInfo<'a> {
    fn new(fields: &'a syn::Fields) -> Option<Self> {
        match &fields {
            syn::Fields::Named(ref fields) => {
                let n_fields = fields.named.len();
                let mut field_names = Vec::with_capacity(n_fields);
                let mut field_idents = Vec::with_capacity(n_fields);
                for ident in fields.named.iter().filter_map(|f| f.ident.as_ref()) {
                    field_names.push(ident.to_string().trim_start_matches("r#").into());
                    field_idents.push(ident);
                }

                Some(Self::Named {
                    field_names,
                    field_idents,
                    n_rec: n_fields as _,
                })
            }
            syn::Fields::Unnamed(ref fields) => {
                let mut field_idents = Vec::with_capacity(fields.unnamed.len());
                let mut field_types = Vec::with_capacity(fields.unnamed.len());
                for (field, i) in fields.unnamed.iter().zip(0..) {
                    field_idents.push(
                        Ident::new(&format!("field_{}", i), Span::call_site())
                    );
                    field_types.push(&field.ty);
                }

                Some(Self::Unnamed {
                    field_idents,
                    field_types,
                })
            }
            // TODO(gmoshkin): add attributes for changing string value, case
            // sensitivity etc. (see serde)
            syn::Fields::Unit => None,
        }
    }

    fn push(&self) -> TokenStream {
        match self {
            Self::Named { field_names, field_idents, n_rec, .. } => {
                quote! {
                    unsafe {
                        hlua::ffi::lua_createtable(__lua.as_lua(), 0, #n_rec);
                        #(
                            hlua::AsLua::push_one(__lua.as_lua(), #field_idents)
                                .assert_one_and_forget();
                            hlua::ffi::lua_setfield(
                                __lua.as_lua(), -2, hlua::c_ptr!(#field_names)
                            );
                        )*
                        hlua::PushGuard::new(__lua, 1)
                    }
                }
            }
            Self::Unnamed { field_idents, .. } => {
                match field_idents.len() {
                    0 => unimplemented!("unit structs are not supported yet"),
                    1 => {
                        let ref field_ident = field_idents[0];
                        quote! {
                            hlua::AsLua::push(__lua, #field_ident)
                        }
                    }
                    _ => {
                        quote! {
                            hlua::AsLua::push(__lua, ( #( #field_idents, )* ))
                        }
                    }
                }
            }
        }
    }

    fn read_as(&self, name: TokenStream) -> TokenStream {
        match self {
            FieldsInfo::Named { field_idents, field_names, .. } => {
                quote! {
                    let t: hlua::LuaTable<_> = hlua::AsLua::read_at_nz(__lua, __index)?;
                    Ok(
                        #name {
                            #(
                                #field_idents: match t.get(#field_names) {
                                    Some(v) => v,
                                    None => return Err(t.into_inner()),
                                },
                            )*
                        }
                    )
                }
            }
            FieldsInfo::Unnamed { field_idents, .. } => {
                quote! {
                    let (#(#field_idents,)*) = hlua::AsLua::read_at_nz(__lua, __index)?;
                    Ok(
                        #name(#(#field_idents,)*)
                    )
                }
            }
        }
    }

    fn pattern(&self) -> TokenStream {
        match self {
            Self::Named { field_idents, .. } => {
                quote! {
                    { #( #field_idents, )* }
                }
            }
            Self::Unnamed { field_idents, .. } => {
                quote! {
                    ( #( #field_idents, )* )
                }
            }
        }
    }

    fn n_values(&self) -> TokenStream {
        match self {
            Self::Named { .. } => {
                // Corresponds to a single lua table
                quote! { 1 }
            }
            Self::Unnamed { field_types, .. } => {
                // Corresponds to multiple values on the stack (same as tuple)
                quote! {
                    #(
                        <#field_types as hlua::LuaRead<L>>::n_values_expected()
                    )+*
                }
            }
        }
    }
}

struct VariantsInfo<'a> {
    variants: Vec<VariantInfo<'a>>,
}

struct VariantInfo<'a> {
    name: &'a Ident,
    info: Option<FieldsInfo<'a>>,
}

impl<'a> VariantsInfo<'a> {
    fn new(data: &'a syn::DataEnum) -> Self {
        let variants = data.variants.iter()
            .map(|syn::Variant { ref ident, ref fields, .. }|
                VariantInfo {
                    name: ident,
                    info: FieldsInfo::new(fields),
                }
            )
            .collect();

        Self { variants }
    }
}

impl<'a> VariantInfo<'a> {
    fn push(&self) -> TokenStream {
        let Self { name, info } = self;
        if let Some(info) = info {
            let fields = info.pattern();
            let push_fields = info.push();
            quote! {
                Self::#name #fields => #push_fields,
            }
        } else {
            let value = name.to_string().to_lowercase();
            quote! {
                Self::#name => {
                    hlua::AsLua::push_one(__lua.as_lua(), #value)
                        .assert_one_and_forget();
                    unsafe { hlua::PushGuard::new(__lua, 1) }
                }
            }
        }
    }

    fn read_and_maybe_return(&self) -> TokenStream {
        let read_variant = self.read();
        let pattern = self.pattern();
        let constructor = self.constructor();
        let (guard, catch_all) = self.optional_match();
        quote! {
            match #read_variant {
                ::std::result::Result::Ok(#pattern) #guard
                    => return ::std::result::Result::Ok(#constructor),
                #catch_all
                ::std::result::Result::Err(__lua) => __lua,
            }
        }
    }

    fn read(&self) -> TokenStream {
        let Self { name, info } = self;
        match info {
            Some(s @ FieldsInfo::Named { .. }) => {
                let read_struct = s.read_as(quote! { Self::#name });
                quote! {
                    (|| { #read_struct })()
                }
            }
            Some(FieldsInfo::Unnamed { .. }) => {
                quote! {
                    hlua::AsLua::read_at_nz(__lua, __index)
                }
            }
            None => {
                quote! {
                    hlua::AsLua::read_at_nz::<hlua::StringInLua<_>>(__lua, __index)
                }
            }
        }
    }

    fn pattern(&self) -> TokenStream {
        let Self { info, .. } = self;
        match info {
            Some(FieldsInfo::Named { .. }) | None => quote! { v },
            Some(FieldsInfo::Unnamed { field_idents, .. }) => {
                match field_idents.len() {
                    0 => unimplemented!("unit structs aren't supported yet"),
                    1 => quote! { v },
                    _ => quote! { ( #(#field_idents,)* ) },
                }
            }
        }
    }

    fn constructor(&self) -> TokenStream {
        let Self { name, info } = self;
        match info {
            Some(FieldsInfo::Named { .. }) => quote! { v },
            Some(FieldsInfo::Unnamed { field_idents, .. }) => {
                match field_idents.len() {
                    0 => quote! { Self::#name },
                    1 => quote! { Self::#name(v) },
                    _ => quote! { Self::#name(#(#field_idents,)*) },
                }
            }
            None => quote! { Self::#name }
        }
    }

    fn optional_match(&self) -> (TokenStream, TokenStream) {
        let Self { name, info } = self;
        let value = name.to_string().to_lowercase();
        if info.is_none() {
            (
                quote! {
                    if {
                        let mut v_count = 0;
                        v.chars()
                            .flat_map(char::to_lowercase)
                            .zip(
                                #value.chars()
                                    .map(::std::option::Option::Some)
                                    .chain(::std::iter::repeat(::std::option::Option::None))
                            )
                            .all(|(l, r)| {
                                v_count += 1;
                                r.map(|r| l == r).unwrap_or(false)
                            }) && v_count == #value.len()
                    }
                },
                quote! {
                    ::std::result::Result::Ok(v) => v.into_inner(),
                }
            )
        } else {
            (quote! {}, quote! {})
        }
    }
}
