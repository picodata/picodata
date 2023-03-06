use std::io::Write;

use proc_macro::TokenStream as TokenStream1;
use proc_macro2::{Span, TokenStream};
use quote::quote;
use syn::{parse_macro_input, DeriveInput, Ident, Lifetime, Type};

#[proc_macro_attribute]
pub fn test(_attr: TokenStream1, item: TokenStream1) -> TokenStream1 {
    let fn_item = parse_macro_input!(item as syn::ItemFn);
    let fn_name = fn_item.sig.ident;
    let test_name = fn_name.to_string();
    let test_name_ident = syn::Ident::new(&test_name.to_uppercase(), fn_name.span());
    let test_body = fn_item.block;
    let unsafe_token = fn_item.sig.unsafety;
    quote! {
        #[::linkme::distributed_slice(crate::test::TLUA_TESTS)]
        static #test_name_ident: crate::test::TestCase = crate::test::TestCase {
            name: ::std::concat!(::std::module_path!(), "::", #test_name),
            f: || #unsafe_token #test_body,
        };
    }
    .into()
}

fn proc_macro_derive_push_impl(
    input: proc_macro::TokenStream,
    is_push_into: bool,
) -> proc_macro::TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    // TODO(gmoshkin): add an attribute to specify path to tlua module (see serde)
    // TODO(gmoshkin): add support for custom type param bounds
    let name = &input.ident;
    let info = Info::new(&input);
    let ctx = Context::with_generics(&input.generics).set_is_push_into(is_push_into);
    let (lifetimes, types, consts) = split_generics(&input.generics);
    let (_, generics, where_clause) = input.generics.split_for_impl();
    let type_bounds = where_clause.map(|w| &w.predicates);
    let as_lua_bounds = info.push_bounds(&ctx);
    let push_code = info.push();
    let PushVariant {
        push_fn,
        push,
        push_one,
    } = ctx.push_variant();
    let l = ctx.as_lua_type_param;

    let expanded = quote! {
        #[automatically_derived]
        impl<#(#lifetimes,)* #(#types,)* #l, #(#consts,)*> tlua::#push<#l> for #name #generics
        where
            #l: tlua::AsLua,
            #as_lua_bounds
            #type_bounds
        {
            type Err = tlua::Void;

            fn #push_fn -> ::std::result::Result<tlua::PushGuard<#l>, (Self::Err, #l)> {
                Ok(#push_code)
            }
        }

        impl<#(#lifetimes,)* #(#types,)* #l, #(#consts,)*> tlua::#push_one<#l> for #name #generics
        where
            #l: tlua::AsLua,
            #as_lua_bounds
            #type_bounds
        {
        }
    };

    expanded.into()
}

#[proc_macro_derive(Push)]
pub fn proc_macro_derive_push(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    proc_macro_derive_push_impl(input, false)
}

#[proc_macro_derive(PushInto)]
pub fn proc_macro_derive_push_into(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    proc_macro_derive_push_impl(input, true)
}

#[proc_macro_derive(LuaRead)]
pub fn proc_macro_derive_lua_read(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;
    let info = Info::new(&input);
    let ctx = Context::with_generics(&input.generics);
    let (lifetimes, types, consts) = split_generics(&input.generics);
    let (_, generics, where_clause) = input.generics.split_for_impl();
    let type_bounds = where_clause.map(|w| &w.predicates);
    let as_lua_bounds = info.read_bounds(&ctx);
    let read_at_code = info.read(&ctx);
    let maybe_n_values_expected = info.n_values(&ctx);
    let maybe_lua_read = info.read_top(&ctx);

    let l = ctx.as_lua_type_param;

    let expanded = quote! {
        #[automatically_derived]
        impl<#(#lifetimes,)* #(#types,)* #l, #(#consts,)*> tlua::LuaRead<#l> for #name #generics
        where
            #l: tlua::AsLua,
            #as_lua_bounds
            #type_bounds
        {
            #maybe_n_values_expected

            #maybe_lua_read

            #[inline(always)]
            fn lua_read_at_position(__lua: #l, __index: ::std::num::NonZeroI32)
                -> tlua::ReadResult<Self, #l>
            {
                Self::lua_read_at_maybe_zero_position(__lua, __index.into())
            }

            fn lua_read_at_maybe_zero_position(__lua: #l, __index: i32)
                -> tlua::ReadResult<Self, #l>
            {
                #read_at_code
            }
        }
    };

    expanded.into()
}

macro_rules! ident {
    ($str:literal) => {
        Ident::new($str, Span::call_site())
    };
    ($($args:tt)*) => {
        Ident::new(&format!($($args)*), Span::call_site())
    };
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
                let fields = f.pattern();
                let push_fields = f.push();
                quote! {
                    match self {
                        Self #fields => #push_fields,
                    }
                }
            }
            Self::Enum(v) => {
                let push_variants = v.variants.iter().map(VariantInfo::push).collect::<Vec<_>>();
                quote! {
                    match self {
                        #( #push_variants )*
                    }
                }
            }
        }
    }

    fn push_bounds(&self, ctx: &Context) -> TokenStream {
        let l = &ctx.as_lua_type_param;
        let PushVariant { push, push_one, .. } = ctx.push_variant();

        let field_bounds = |info: &FieldsInfo| match info {
            FieldsInfo::Named {
                field_types: ty, ..
            } => {
                let ty = ty.iter().filter(|ty| ctx.is_generic(ty));
                quote! {
                    #(
                        #ty: tlua::#push_one<tlua::LuaState>,
                        tlua::Void: ::std::convert::From<<#ty as tlua::#push<tlua::LuaState>>::Err>,
                    )*
                }
            }
            FieldsInfo::Unnamed {
                field_types: ty, ..
            } if ty.iter().any(|ty| ctx.is_generic(ty)) => {
                quote! {
                    (#(#ty),*): tlua::#push<#l>,
                    tlua::Void: ::std::convert::From<<(#(#ty),*) as tlua::#push<#l>>::Err>,
                }
            }
            FieldsInfo::Unnamed { .. } => {
                quote! {}
            }
        };
        match self {
            Self::Struct(f) => field_bounds(f),
            Self::Enum(v) => {
                let bound = v.variants.iter().flat_map(|v| &v.info).map(field_bounds);
                quote! {
                    #(#bound)*
                }
            }
        }
    }

    fn read(&self, ctx: &Context) -> TokenStream {
        match self {
            Self::Struct(f) => f.read_as(None),
            Self::Enum(v) => {
                let mut n_vals = vec![];
                let mut read_and_maybe_return_variant = vec![];
                for variant in &v.variants {
                    n_vals.push(if let Some(ref fields) = variant.info {
                        fields.n_values(ctx)
                    } else {
                        quote! { 1 }
                    });
                    read_and_maybe_return_variant.push(variant.read_and_maybe_return());
                }
                quote! {
                    let mut errors: ::std::collections::LinkedList<tlua::WrongType> = Default::default();
                    #(
                        let n_vals = #n_vals;
                        let __lua = #read_and_maybe_return_variant;
                    )*
                    let e = tlua::WrongType::info("reading any of the variants")
                        .expected_type::<Self>()
                        .actual("something else")
                        .subtypes(errors);
                    Err((__lua, e))
                }
            }
        }
    }

    fn read_bounds(&self, ctx: &Context) -> TokenStream {
        let l = &ctx.as_lua_type_param;
        let lt = &ctx.as_lua_lifetime_param;

        let field_bounds = |info: &FieldsInfo| {
            match info {
                FieldsInfo::Named {
                    field_types: ty, ..
                } => {
                    // Structs fields are read as values from the lua tables and
                    // this is how `LuaTable::get` bounds it's return values
                    let ty = ty.iter().filter(|ty| ctx.is_generic(ty));
                    quote! {
                        #( #ty: for<#lt> tlua::LuaRead<tlua::PushGuard<&#lt #l>>, )*
                    }
                }
                FieldsInfo::Unnamed {
                    field_types: ty, ..
                } if ty.iter().any(|ty| ctx.is_generic(ty)) => {
                    // Tuple structs are read as tuples, so we bound they're
                    // fields as if they were a tuple
                    quote! {
                        (#(#ty),*): tlua::LuaRead<#l>,
                    }
                }
                FieldsInfo::Unnamed { .. } => {
                    // Unit structs (as vairants of enums) are read as strings
                    // so no need for type bounds
                    quote! {}
                }
            }
        };
        match self {
            Self::Struct(f) => field_bounds(f),
            Self::Enum(v) => {
                let bound = v.variants.iter().flat_map(|v| &v.info).map(field_bounds);
                quote! {
                    #(#bound)*
                }
            }
        }
    }

    fn read_top(&self, ctx: &Context) -> TokenStream {
        match self {
            Self::Struct(_) => quote! {},
            Self::Enum(v) => {
                let mut n_vals = vec![];
                let mut read_and_maybe_return = vec![];
                for variant in &v.variants {
                    n_vals.push(if let Some(ref fields) = variant.info {
                        fields.n_values(ctx)
                    } else {
                        quote! { 1 }
                    });
                    read_and_maybe_return.push(variant.read_and_maybe_return());
                }
                let l = &ctx.as_lua_type_param;
                quote! {
                    fn lua_read(__lua: #l) -> tlua::ReadResult<Self, #l> {
                        let top = unsafe { tlua::ffi::lua_gettop(__lua.as_lua()) };
                        let mut errors: ::std::collections::LinkedList<tlua::WrongType> = Default::default();
                        #(
                            let n_vals = #n_vals;
                            let __lua = if top >= n_vals {
                                let __index = top - n_vals + 1;
                                #read_and_maybe_return
                            } else {
                                __lua
                            };
                        )*
                        let e = tlua::WrongType::info("reading any of the variants")
                            .expected_type::<Self>()
                            .actual("something else")
                            .subtypes(errors);
                        Err((__lua, e))
                    }
                }
            }
        }
    }

    fn n_values(&self, ctx: &Context) -> TokenStream {
        match self {
            Self::Struct(fields) => {
                let n_values = fields.n_values(ctx);
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
        field_types: Vec<&'a Type>,
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
                let mut field_types = Vec::with_capacity(n_fields);
                for field in fields.named.iter() {
                    let ident = field.ident.as_ref().unwrap();
                    field_names.push(ident.to_string().trim_start_matches("r#").into());
                    field_idents.push(ident);
                    field_types.push(&field.ty);
                }

                Some(Self::Named {
                    field_names,
                    field_idents,
                    field_types,
                    n_rec: n_fields as _,
                })
            }
            syn::Fields::Unnamed(ref fields) => {
                let mut field_idents = Vec::with_capacity(fields.unnamed.len());
                let mut field_types = Vec::with_capacity(fields.unnamed.len());
                for (field, i) in fields.unnamed.iter().zip(0..) {
                    field_idents.push(ident!("field_{}", i));
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
            Self::Named {
                field_names,
                field_idents,
                n_rec,
                ..
            } => {
                quote! {
                    unsafe {
                        tlua::ffi::lua_createtable(__lua.as_lua(), 0, #n_rec);
                        #(
                            tlua::AsLua::push_one(__lua.as_lua(), #field_idents)
                                .assert_one_and_forget();
                            tlua::ffi::lua_setfield(
                                __lua.as_lua(), -2, ::std::concat!(#field_names, "\0").as_ptr() as _
                            );
                        )*
                        tlua::PushGuard::new(__lua, 1)
                    }
                }
            }
            Self::Unnamed { field_idents, .. } => match field_idents.len() {
                0 => unimplemented!("unit structs are not supported yet"),
                1 => {
                    let field_ident = &field_idents[0];
                    quote! {
                        tlua::AsLua::push(__lua, #field_ident)
                    }
                }
                _ => {
                    quote! {
                        tlua::AsLua::push(__lua, ( #( #field_idents, )* ))
                    }
                }
            },
        }
    }

    fn read_as(&self, name: Option<TokenStream>) -> TokenStream {
        let is_variant = name.is_some();
        let name = name.unwrap_or_else(|| quote! { Self });
        match self {
            FieldsInfo::Named {
                field_idents,
                field_names,
                field_types,
                ..
            } => {
                let expected = if is_variant {
                    let mut msg = vec![];
                    write!(&mut msg, "struct with fields {{").unwrap();
                    if let Some((n, t)) = field_names.iter().zip(field_types).next() {
                        write!(&mut msg, " {}: {}", n, quote! { #t }).unwrap();
                    }
                    for (n, t) in field_names.iter().zip(field_types).skip(1) {
                        write!(&mut msg, ", {}: {}", n, quote! { #t }).unwrap();
                    }
                    write!(&mut msg, " }}").unwrap();
                    let msg = String::from_utf8(msg).unwrap();
                    quote! { .expected(#msg) }
                } else {
                    quote! { .expected_type::<Self>() }
                };
                quote! {
                    let t: tlua::LuaTable<_> = tlua::AsLua::read_at(__lua, __index)
                        .map_err(|(lua, err)| {
                            let err = err.when("converting Lua value to struct")
                                .expected("Lua table");
                            (lua, err)
                        })?;
                    Ok(
                        #name {
                            #(
                                #field_idents: match tlua::Index::try_get(&t, #field_names) {
                                    Ok(v) => v,
                                    Err(err) => {
                                        let l = t.into_inner();
                                        let mut e = tlua::WrongType::info(
                                            "converting Lua table to struct"
                                        ) #expected;
                                        match err {
                                            tlua::LuaError::WrongType(subtype) => {
                                                let actual_msg = ::std::concat!(
                                                    "wrong field type for key '",
                                                    #field_names,
                                                    "'",
                                                );
                                                e = e.actual(actual_msg).subtype(subtype);
                                            }
                                            other => {
                                                e = e.actual(format!(
                                                    "error in meta method: {}", other
                                                ));
                                            }
                                        }
                                        return Err((l, e))
                                    },
                                },
                            )*
                        }
                    )
                }
            }
            FieldsInfo::Unnamed { field_idents, .. } => {
                quote! {
                    let (#(#field_idents,)*) = tlua::AsLua::read_at(__lua, __index)?;
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

    fn n_values(&self, ctx: &Context) -> TokenStream {
        match self {
            Self::Named { .. } => {
                // Corresponds to a single lua table
                quote! { 1 }
            }
            Self::Unnamed {
                field_types: ty, ..
            } if !ty.is_empty() => {
                let l = &ctx.as_lua_type_param;
                // Corresponds to multiple values on the stack (same as tuple)
                quote! {
                    <(#(#ty),*) as tlua::LuaRead<#l>>::n_values_expected()
                }
            }
            Self::Unnamed { .. } => {
                // Unit structs aren't supported yet, but when they are, they'll
                // probably correspond to a single value
                quote! { 1 }
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
        let variants = data
            .variants
            .iter()
            .map(
                |syn::Variant {
                     ref ident,
                     ref fields,
                     ..
                 }| VariantInfo {
                    name: ident,
                    info: FieldsInfo::new(fields),
                },
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
                    tlua::AsLua::push_one(__lua.as_lua(), #value)
                        .assert_one_and_forget();
                    unsafe { tlua::PushGuard::new(__lua, 1) }
                }
            }
        }
    }

    fn read_and_maybe_return(&self) -> TokenStream {
        let read_variant = self.read();
        let pattern = self.pattern();
        let constructor = self.constructor();
        let (guard, catch_all) = self.optional_match();
        let name = self.name.to_string();
        quote! {
            match #read_variant {
                ::std::result::Result::Ok(#pattern) #guard
                    => return ::std::result::Result::Ok(#constructor),
                #catch_all
                ::std::result::Result::Err((__lua, e)) => {
                    let mut e = tlua::WrongType::info("reading enum variant")
                        .expected(#name)
                        .subtype(e);
                    if let Some(i) = ::std::num::NonZeroI32::new(__index) {
                        e = e.actual_multiple_lua_at(&__lua, dbg!(i), dbg!(n_vals))
                    } else {
                        e = e.actual("no value")
                    }
                    errors.push_back(e);
                    __lua
                }
            }
        }
    }

    fn read(&self) -> TokenStream {
        let Self { name, info } = self;
        match info {
            Some(s @ FieldsInfo::Named { .. }) => {
                let read_struct = s.read_as(Some(quote! { Self::#name }));
                quote! {
                    (|| { #read_struct })()
                }
            }
            Some(FieldsInfo::Unnamed { .. }) => {
                quote! {
                    tlua::AsLua::read_at(__lua, __index)
                }
            }
            None => {
                quote! {
                    tlua::AsLua::read_at::<tlua::StringInLua<_>>(__lua, __index)
                }
            }
        }
    }

    fn pattern(&self) -> TokenStream {
        let Self { info, .. } = self;
        match info {
            Some(FieldsInfo::Named { .. }) | None => quote! { v },
            Some(FieldsInfo::Unnamed { field_idents, .. }) => match field_idents.len() {
                0 => unimplemented!("unit structs aren't supported yet"),
                1 => quote! { v },
                _ => quote! { ( #(#field_idents,)* ) },
            },
        }
    }

    fn constructor(&self) -> TokenStream {
        let Self { name, info } = self;
        match info {
            Some(FieldsInfo::Named { .. }) => quote! { v },
            Some(FieldsInfo::Unnamed { field_idents, .. }) => match field_idents.len() {
                0 => quote! { Self::#name },
                1 => quote! { Self::#name(v) },
                _ => quote! { Self::#name(#(#field_idents,)*) },
            },
            None => quote! { Self::#name },
        }
    }

    fn optional_match(&self) -> (TokenStream, TokenStream) {
        let Self { name, info } = self;
        let value = name.to_string().to_lowercase();
        if info.is_none() {
            let expected = format!("case incensitive match with '{value}'");
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
                    ::std::result::Result::Ok(v) => {
                        let e = tlua::WrongType::info("reading unit struct")
                            .expected(#expected)
                            .actual(format!("'{}'", &*v));
                        errors.push_back(e);
                        v.into_inner()
                    },
                },
            )
        } else {
            (quote! {}, quote! {})
        }
    }
}

struct Context<'a> {
    as_lua_type_param: Ident,
    as_lua_lifetime_param: Lifetime,
    type_params: Vec<&'a Ident>,
    is_push_into: bool,
}

struct PushVariant {
    push_fn: TokenStream,
    push: syn::Path,
    push_one: syn::Path,
}
impl<'a> Context<'a> {
    fn new() -> Self {
        Self {
            as_lua_type_param: Ident::new("__AsLuaTypeParam", Span::call_site()),
            as_lua_lifetime_param: Lifetime::new("'as_lua_life_time_param", Span::call_site()),
            type_params: Vec::new(),
            is_push_into: false,
        }
    }

    fn with_generics(generics: &'a syn::Generics) -> Self {
        Self {
            type_params: generics.type_params().map(|tp| &tp.ident).collect(),
            ..Self::new()
        }
    }

    fn set_is_push_into(self, is_push_into: bool) -> Self {
        Self {
            is_push_into,
            ..self
        }
    }

    fn push_variant(&self) -> PushVariant {
        let l = &self.as_lua_type_param;
        if self.is_push_into {
            PushVariant {
                push_fn: quote! {
                    push_into_lua(self, __lua: #l)
                },
                push: ident!("PushInto").into(),
                push_one: ident!("PushOneInto").into(),
            }
        } else {
            PushVariant {
                push_fn: quote! {
                    push_to_lua(&self, __lua: #l)
                },
                push: ident!("Push").into(),
                push_one: ident!("PushOne").into(),
            }
        }
    }

    fn is_generic(&self, ty: &Type) -> bool {
        struct GenericTypeVisitor<'a> {
            is_generic: bool,
            type_params: &'a [&'a Ident],
        }
        impl<'a, 'ast> syn::visit::Visit<'ast> for GenericTypeVisitor<'a> {
            // These cannot actually appear in struct/enum field types,
            // but who cares
            fn visit_type_impl_trait(&mut self, _: &'ast syn::TypeImplTrait) {
                self.is_generic = true;
            }

            fn visit_type_path(&mut self, tp: &'ast syn::TypePath) {
                for &typar in self.type_params {
                    if tp.path.is_ident(typar) {
                        self.is_generic = true;
                        return;
                    }
                }
                syn::visit::visit_type_path(self, tp)
            }
        }

        let mut visitor = GenericTypeVisitor {
            is_generic: false,
            type_params: &self.type_params,
        };
        syn::visit::visit_type(&mut visitor, ty);
        visitor.is_generic
    }
}

fn split_generics(
    generics: &syn::Generics,
) -> (
    Vec<&syn::LifetimeDef>,
    Vec<&syn::TypeParam>,
    Vec<&syn::ConstParam>,
) {
    let mut res = (vec![], vec![], vec![]);
    for param in &generics.params {
        match param {
            syn::GenericParam::Lifetime(l) => res.0.push(l),
            syn::GenericParam::Type(t) => res.1.push(t),
            syn::GenericParam::Const(c) => res.2.push(c),
        }
    }
    res
}
