use proc_macro::TokenStream;
use proc_macro2::{Span, TokenStream as TokenStream2};
use proc_macro_error::{proc_macro_error, SpanRange};
use quote::{quote, ToTokens};
use syn::{
    parse_macro_input, punctuated::Punctuated, Attribute, AttributeArgs, DeriveInput, FnArg, Ident,
    Item, ItemFn, Signature, Token,
};

// https://git.picodata.io/picodata/picodata/tarantool-module/-/merge_requests/505#note_78473
macro_rules! unwrap_or_compile_error {
    ($expr:expr) => {
        match $expr {
            Ok(v) => v,
            Err(e) => return e.to_compile_error().into(),
        }
    };
}

mod test;

/// Mark a function as a test.
///
/// See `tarantool::test` doc-comments in tarantool crate for details.
#[proc_macro_attribute]
pub fn test(attr: TokenStream, item: TokenStream) -> TokenStream {
    test::impl_macro_attribute(attr, item)
}

mod msgpack {
    use darling::FromDeriveInput;
    use proc_macro2::TokenStream;
    use proc_macro_error::{abort, SpanRange};
    use quote::{format_ident, quote, quote_spanned, ToTokens};
    use syn::{
        parse_quote, spanned::Spanned, Data, Field, Fields, FieldsNamed, FieldsUnnamed,
        GenericParam, Generics, Ident, Index, Path, Type,
    };

    #[derive(Default, FromDeriveInput)]
    #[darling(attributes(encode), default)]
    pub struct Args {
        /// Whether this struct should be serialized as MP_MAP instead of MP_ARRAY.
        pub as_map: bool,
        /// Path to tarantool crate.
        pub tarantool: Option<String>,
        /// Allows optional fields of unnamed structs to be decoded if values are not presented.
        pub allow_array_optionals: bool,
    }

    pub fn add_trait_bounds(mut generics: Generics, tarantool_crate: &Path) -> Generics {
        for param in &mut generics.params {
            if let GenericParam::Type(ref mut type_param) = *param {
                type_param
                    .bounds
                    .push(parse_quote!(#tarantool_crate::msgpack::Encode));
            }
        }
        generics
    }

    trait TypeExt {
        fn is_option(&self) -> bool;
    }

    impl TypeExt for Type {
        fn is_option(&self) -> bool {
            if let Type::Path(ref typepath) = self {
                typepath
                    .path
                    .segments
                    .last()
                    .map(|segment| segment.ident == "Option")
                    .unwrap_or(false)
            } else {
                false
            }
        }
    }

    /// Defines how field will be encoded or decoded according to attribute on it.
    enum FieldAttr {
        /// Field should be serialized without any check of internal value whatsoever.
        Raw,
        /// TODO: Field should be serialized as MP_MAP, ignoring struct-level serialization type.
        Map,
        /// TODO: Field should be serialized as MP_ARRAY, ignoring struct-level serialization type.
        Vec,
    }

    impl FieldAttr {
        /// Returns appropriate `Some(FieldAttr)` for this field according to attribute on it, `None` if
        /// no attribute was on a field, or errors if attribute encoding type is empty/multiple/wrong.
        #[inline]
        fn from_field(field: &Field) -> Result<Option<Self>, syn::Error> {
            let attrs = &field.attrs;

            let mut encode_attr = None;

            for attr in attrs.iter().filter(|attr| attr.path.is_ident("encode")) {
                if encode_attr.is_some() {
                    return Err(syn::Error::new(
                        attr.span(),
                        "multiple encoding types are not allowed",
                    ));
                }

                encode_attr = Some(attr);
            }

            match encode_attr {
                Some(attr) => attr.parse_args_with(|input: syn::parse::ParseStream| {
                    if input.is_empty() {
                        return Err(syn::Error::new(
                            input.span(),
                            "empty encoding type is not allowed",
                        ));
                    }

                    let ident: Ident = input.parse()?;

                    if !input.is_empty() {
                        return Err(syn::Error::new(
                            ident.span(),
                            "multiple encoding types are not allowed",
                        ));
                    }

                    if ident == "as_raw" {
                        let mut field_type_name = proc_macro2::TokenStream::new();
                        field.ty.to_tokens(&mut field_type_name);
                        if field_type_name.to_string() != "Vec < u8 >" {
                            Err(syn::Error::new(
                                ident.span(),
                                "only `Vec<u8>` is supported for `as_raw`",
                            ))
                        } else {
                            Ok(Some(Self::Raw))
                        }
                    } else if ident == "as_map" {
                        Ok(Some(Self::Map))
                    } else if ident == "as_vec" {
                        Ok(Some(Self::Vec))
                    } else {
                        Err(syn::Error::new(ident.span(), "unknown encoding type"))
                    }
                }),
                None => Ok(None),
            }
        }
    }

    fn encode_named_fields(
        fields: &FieldsNamed,
        tarantool_crate: &Path,
        add_self: bool,
    ) -> proc_macro2::TokenStream {
        fields
            .named
            .iter()
            .flat_map(|f| {
                let field_name = f.ident.as_ref().expect("only named fields here");
                let field_repr = format_ident!("{}", field_name).to_string();
                let field_attr = unwrap_or_compile_error!(FieldAttr::from_field(f));

                let s = if add_self {
                    quote! {&self.}
                } else {
                    quote! {}
                };

                let write_key = quote_spanned! {f.span()=>
                    if as_map {
                        #tarantool_crate::msgpack::rmp::encode::write_str(w, #field_repr)?;
                    }
                };
                if let Some(attr) = field_attr {
                    match attr {
                        FieldAttr::Raw => quote_spanned! {f.span()=>
                            #write_key
                            w.write_all(#s #field_name)?;
                        },
                        // TODO: encode with `#[encode(as_map)]` and `#[encode(as_vec)]`
                        FieldAttr::Map => {
                            syn::Error::new(f.span(), "`as_map` is not currently supported")
                                .to_compile_error()
                        }
                        FieldAttr::Vec => {
                            syn::Error::new(f.span(), "`as_vec` is not currently supported")
                                .to_compile_error()
                        }
                    }
                } else {
                    quote_spanned! {f.span()=>
                        #write_key
                        #tarantool_crate::msgpack::Encode::encode(#s #field_name, w, context)?;
                    }
                }
            })
            .collect()
    }

    fn encode_unnamed_fields(
        fields: &FieldsUnnamed,
        tarantool_crate: &Path,
    ) -> proc_macro2::TokenStream {
        fields
            .unnamed
            .iter()
            .enumerate()
            .flat_map(|(i, f)| {
                let index = Index::from(i);
                let field_attr = unwrap_or_compile_error!(FieldAttr::from_field(f));

                if let Some(field) = field_attr {
                    match field {
                        FieldAttr::Raw => quote_spanned! {f.span()=>
                            w.write_all(&self.#index)?;
                        },
                        // TODO: encode with `#[encode(as_map)]` and `#[encode(as_vec)]`
                        FieldAttr::Map => {
                            syn::Error::new(f.span(), "`as_map` is not currently supported")
                                .to_compile_error()
                        }
                        FieldAttr::Vec => {
                            syn::Error::new(f.span(), "`as_vec` is not currently supported")
                                .to_compile_error()
                        }
                    }
                } else {
                    quote_spanned! {f.span()=>
                        #tarantool_crate::msgpack::Encode::encode(&self.#index, w, context)?;
                    }
                }
            })
            .collect()
    }

    pub fn encode_fields(
        data: &Data,
        tarantool_crate: &Path,
        attrs_span: impl Fn() -> SpanRange,
        args: &Args,
    ) -> proc_macro2::TokenStream {
        let as_map = args.as_map;

        match *data {
            Data::Struct(ref data) => match data.fields {
                Fields::Named(ref fields) => {
                    let field_count = fields.named.len() as u32;
                    let fields = encode_named_fields(fields, tarantool_crate, true);
                    quote! {
                        let as_map = match context.struct_style() {
                            StructStyle::Default => #as_map,
                            StructStyle::ForceAsMap => true,
                            StructStyle::ForceAsArray => false,
                        };
                        if as_map {
                            #tarantool_crate::msgpack::rmp::encode::write_map_len(w, #field_count)?;
                        } else {
                            #tarantool_crate::msgpack::rmp::encode::write_array_len(w, #field_count)?;
                        }
                        #fields
                    }
                }
                Fields::Unnamed(ref fields) => {
                    if as_map {
                        abort!(
                            attrs_span(),
                            "`as_map` attribute can be specified only for structs with named fields"
                        );
                    }
                    let field_count = fields.unnamed.len() as u32;
                    let fields = encode_unnamed_fields(fields, tarantool_crate);
                    quote! {
                        #tarantool_crate::msgpack::rmp::encode::write_array_len(w, #field_count)?;
                        #fields
                    }
                }
                Fields::Unit => {
                    quote!(#tarantool_crate::msgpack::Encode::encode(&(), w, context)?;)
                }
            },
            Data::Enum(ref variants) => {
                if as_map {
                    abort!(
                        attrs_span(),
                        "`as_map` attribute can be specified only for structs"
                    );
                }
                let variants: proc_macro2::TokenStream = variants
                    .variants
                    .iter()
                    .flat_map(|variant| {
                        let variant_name = &variant.ident;
                        let variant_repr = format_ident!("{}", variant_name).to_string();
                        match variant.fields {
                            Fields::Named(ref fields) => {
                                let field_count = fields.named.len() as u32;
                                let field_names = fields.named.iter().map(|field| field.ident.clone());
                                let fields = encode_named_fields(fields, tarantool_crate, false);
                                // TODO: allow `#[encode(as_map)]` for struct variants
                                quote! {
                                     Self::#variant_name { #(#field_names),*} => {
                                        #tarantool_crate::msgpack::rmp::encode::write_str(w, #variant_repr)?;
                                        #tarantool_crate::msgpack::rmp::encode::write_array_len(w, #field_count)?;
                                        let as_map = false;
                                        #fields
                                    }
                                }
                            },
                            Fields::Unnamed(ref fields) => {
                                let field_count = fields.unnamed.len() as u32;
                                let field_names = fields.unnamed.iter().enumerate().map(|(i, _)| format_ident!("_field_{}", i));
                                let fields: proc_macro2::TokenStream = field_names.clone()
                                    .flat_map(|field_name| quote! {
                                        #tarantool_crate::msgpack::Encode::encode(#field_name, w, context)?;
                                    })
                                    .collect();
                                quote! {
                                    Self::#variant_name ( #(#field_names),* ) => {
                                        #tarantool_crate::msgpack::rmp::encode::write_str(w, #variant_repr)?;
                                        #tarantool_crate::msgpack::rmp::encode::write_array_len(w, #field_count)?;
                                        #fields
                                    }
                               }
                            }
                            Fields::Unit => {
                                quote! {
                                    Self::#variant_name => {
                                        #tarantool_crate::msgpack::rmp::encode::write_str(w, #variant_repr)?;
                                        #tarantool_crate::msgpack::Encode::encode(&(), w, context)?;
                                    }
                                }
                            },
                        }
                    })
                    .collect();
                quote! {
                    #tarantool_crate::msgpack::rmp::encode::write_map_len(w, 1)?;
                    match self {
                        #variants
                    }
                }
            }
            Data::Union(_) => unimplemented!(),
        }
    }

    fn decode_named_fields(
        fields: &FieldsNamed,
        tarantool_crate: &Path,
        enum_variant: Option<&syn::Ident>,
        allow_array_optionals: bool,
    ) -> TokenStream {
        let mut var_names = Vec::with_capacity(fields.named.len());
        let mut met_option = false;
        let fields_amount = fields.named.len();
        let mut fields_passed = fields_amount;
        let code: TokenStream = fields
            .named
            .iter()
            .map(|f| {
                if f.ty.is_option() {
                    met_option = true;
                    fields_passed -= 1;
                    decode_named_optional_field(f, tarantool_crate, &mut var_names, allow_array_optionals, fields_amount, fields_passed)
                } else {
                    if met_option && allow_array_optionals {
                        return syn::Error::new(
                            f.span(),
                            "optional fields must be the last in the parameter list if allow_array_optionals is enabled",
                        )
                        .to_compile_error();
                    }
                    fields_passed -= 1;
                    decode_named_required_field(f, tarantool_crate, &mut var_names)
                }
            })
            .collect();
        let field_names = fields.named.iter().map(|f| &f.ident);
        let enum_variant = if let Some(variant) = enum_variant {
            quote! { ::#variant }
        } else {
            quote! {}
        };
        quote! {
            #code
            Ok(Self #enum_variant {
                #(#field_names: #var_names),*
            })
        }
    }

    #[inline]
    fn decode_named_optional_field(
        field: &Field,
        tarantool_crate: &Path,
        names: &mut Vec<Ident>,
        allow_array_optionals: bool,
        fields_amount: usize,
        fields_passed: usize,
    ) -> TokenStream {
        let field_type = &field.ty;
        let field_attr = unwrap_or_compile_error!(FieldAttr::from_field(field));

        let field_ident = field.ident.as_ref().expect("only named fields here");
        let field_repr = format_ident!("{}", field_ident).to_string();
        let field_name = proc_macro2::Literal::byte_string(field_repr.as_bytes());
        let var_name = format_ident!("_field_{}", field_ident);

        let read_key = quote_spanned! {field.span()=>
            if as_map {
                use #tarantool_crate::msgpack::str_bounds;

                let (byte_len, field_name_len_spaced) = str_bounds(r)
                    .map_err(|err| #tarantool_crate::msgpack::DecodeError::new::<Self>(err).with_part("field name"))?;
                let decoded_field_name = r.get(byte_len..field_name_len_spaced).unwrap();
                if decoded_field_name != #field_name {
                    is_none = true;
                } else {
                    let len = rmp::decode::read_str_len(r).unwrap();
                    *r = &r[(len as usize)..]; // advance if matches field name
                }
            }
        };

        // TODO: allow `#[encode(as_map)]` and `#[encode(as_vec)]` for struct fields
        let out = match field_attr {
            Some(FieldAttr::Map) => unimplemented!("`as_map` is not currently supported"),
            Some(FieldAttr::Vec) => unimplemented!("`as_vec` is not currently supported"),
            Some(FieldAttr::Raw) => quote_spanned! {field.span()=>
                    let mut #var_name: #field_type = None;
                    let mut is_none = false;

                    #read_key
                    if !is_none {
                        #var_name = Some(#tarantool_crate::msgpack::preserve_read(r).expect("only valid msgpack here"));
                    }
            },
            None => quote_spanned! {field.span()=>
                let mut #var_name: #field_type = None;
                let mut is_none = false;

                #read_key
                if !is_none {
                    match #tarantool_crate::msgpack::Decode::decode(r, context) {
                        Ok(val) => #var_name = Some(val),
                        Err(err) => {
                            let markered = err.source.get(err.source.len() - 33..).unwrap_or("") == "failed to read MessagePack marker";
                            let nulled = if err.part.is_some() {
                                err.part.as_ref().expect("Can't fail after a conditional check") == "got Null"
                            } else {
                                false
                            };

                            if !nulled && !#allow_array_optionals && !as_map {
                                let message = format!("not enough fields, expected {}, got {} (note: optional fields must be explicitly null unless `allow_array_optionals` attribute is passed)", #fields_amount, #fields_passed);
                                Err(#tarantool_crate::msgpack::DecodeError::new::<Self>(message))?;
                            } else if !nulled && !markered && #allow_array_optionals {
                                Err(err)?;
                            }
                        },
                    }
                }
            },
        };

        names.push(var_name);
        out
    }

    #[inline]
    fn decode_named_required_field(
        field: &Field,
        tarantool_crate: &Path,
        names: &mut Vec<Ident>,
    ) -> TokenStream {
        let field_attr = unwrap_or_compile_error!(FieldAttr::from_field(field));

        let field_ident = field.ident.as_ref().expect("only named fields here");
        let field_repr = format_ident!("{}", field_ident).to_string();
        let field_name = proc_macro2::Literal::byte_string(field_repr.as_bytes());
        let var_name = format_ident!("_field_{}", field_ident);

        let read_key = quote_spanned! {field.span()=>
            if as_map {
                let len = rmp::decode::read_str_len(r)
                    .map_err(|err| #tarantool_crate::msgpack::DecodeError::from_vre::<Self>(err).with_part("field name"))?;
                let decoded_field_name = r.get(0..(len as usize))
                    .ok_or_else(|| #tarantool_crate::msgpack::DecodeError::new::<Self>("not enough data").with_part("field name"))?;
                *r = &r[(len as usize)..]; // advance
                if decoded_field_name != #field_name {
                    let field_name = String::from_utf8(#field_name.to_vec()).expect("is valid utf8");
                    let err = if let Ok(decoded_field_name) = String::from_utf8(decoded_field_name.to_vec()) {
                        format!("expected field {}, got {}", field_name, decoded_field_name)
                    } else {
                        format!("expected field {}, got invalid utf8 {:?}", field_name, decoded_field_name)
                    };
                    return Err(#tarantool_crate::msgpack::DecodeError::new::<Self>(err));
                }
            }
        };

        // TODO: allow `#[encode(as_map)]` and `#[encode(as_vec)]` for struct fields
        let out = if let Some(FieldAttr::Raw) = field_attr {
            quote_spanned! {field.span()=>
                #read_key
                let #var_name = #tarantool_crate::msgpack::preserve_read(r).expect("only valid msgpack here");
            }
        } else if let Some(FieldAttr::Map) = field_attr {
            unimplemented!("`as_map` is not currently supported");
        } else if let Some(FieldAttr::Vec) = field_attr {
            unimplemented!("`as_vec` is not currently supported");
        } else {
            quote_spanned! {field.span()=>
                #read_key
                let #var_name = #tarantool_crate::msgpack::Decode::decode(r, context)
                    .map_err(|err| #tarantool_crate::msgpack::DecodeError::new::<Self>(err).with_part(format!("field {}", stringify!(#field_ident))))?;
            }
        };

        names.push(var_name);
        out
    }

    fn decode_unnamed_fields(
        fields: &FieldsUnnamed,
        tarantool_crate: &Path,
        enum_variant: Option<&syn::Ident>,
        allow_array_optionals: bool,
    ) -> proc_macro2::TokenStream {
        let mut var_names = Vec::with_capacity(fields.unnamed.len());
        let mut met_option = false;
        let code: proc_macro2::TokenStream = fields
            .unnamed
            .iter()
            .enumerate()
            .map(|(i, f)| {
                let is_option = f.ty.is_option();
                if is_option {
                    met_option = true;
                    decode_unnamed_optional_field(f, i, tarantool_crate, &mut var_names)
                } else if met_option && allow_array_optionals {
                    return syn::Error::new(
                        f.span(),
                        "optional fields must be the last in the parameter list with `allow_array_optionals` attribute",
                    )
                    .to_compile_error();
                } else {
                    decode_unnamed_required_field(f, i, tarantool_crate, &mut var_names)
                }
            })
            .collect();
        let enum_variant = if let Some(variant) = enum_variant {
            quote! { ::#variant }
        } else {
            quote! {}
        };
        quote! {
            #code
            Ok(Self #enum_variant (
                #(#var_names),*
            ))
        }
    }

    fn decode_unnamed_optional_field(
        field: &Field,
        index: usize,
        tarantool_crate: &Path,
        names: &mut Vec<Ident>,
    ) -> TokenStream {
        let field_attr = unwrap_or_compile_error!(FieldAttr::from_field(field));
        let field_type = &field.ty;

        let field_index = Index::from(index);
        let var_name = quote::format_ident!("_field_{}", field_index);

        let out = match field_attr {
            Some(FieldAttr::Map) => unimplemented!("`as_map` is not currently supported"),
            Some(FieldAttr::Vec) => unimplemented!("`as_vec` is not currently supported"),
            Some(FieldAttr::Raw) => quote_spanned! {field.span()=>
                let #var_name = #tarantool_crate::msgpack::preserve_read(r).expect("only valid msgpack here");
            },
            None => quote_spanned! {field.span()=>
                let mut #var_name: #field_type = None;
                match #tarantool_crate::msgpack::Decode::decode(r, context) {
                    Ok(val) => #var_name = Some(val),
                    Err(err) => {
                        let markered = err.source.get(err.source.len() - 33..).unwrap_or("")== "failed to read MessagePack marker";
                        let nulled = if err.part.is_some() {
                            err.part.as_ref().expect("Can't fail after a conditional check") == "got Null"
                        } else {
                            false
                        };

                        if !nulled && !markered {
                            Err(#tarantool_crate::msgpack::DecodeError::new::<Self>(err).with_part(format!("{}", stringify!(#field_index))))?;
                        }
                    },
                }
            },
        };

        names.push(var_name);
        out
    }

    fn decode_unnamed_required_field(
        field: &Field,
        index: usize,
        tarantool_crate: &Path,
        names: &mut Vec<Ident>,
    ) -> TokenStream {
        let field_attr = unwrap_or_compile_error!(FieldAttr::from_field(field));

        let field_index = Index::from(index);
        let var_name = quote::format_ident!("_field_{}", field_index);

        let out = if let Some(FieldAttr::Raw) = field_attr {
            quote_spanned! {field.span()=>
                let #var_name = #tarantool_crate::msgpack::preserve_read(r).expect("only valid msgpack here");
            }
        } else if let Some(FieldAttr::Map) = field_attr {
            unimplemented!("`as_map` is not currently supported");
        } else if let Some(FieldAttr::Vec) = field_attr {
            unimplemented!("`as_vec` is not currently supported");
        } else {
            quote_spanned! {field.span()=>
                let #var_name = #tarantool_crate::msgpack::Decode::decode(r, context)
                    .map_err(|err| #tarantool_crate::msgpack::DecodeError::new::<Self>(err).with_part(format!("field {}", #index)))?;
            }
        };

        names.push(var_name);
        out
    }

    pub fn decode_fields(
        data: &Data,
        tarantool_crate: &Path,
        attrs_span: impl Fn() -> SpanRange,
        args: &Args,
    ) -> proc_macro2::TokenStream {
        let as_map = args.as_map;

        match *data {
            Data::Struct(ref data) => match data.fields {
                Fields::Named(ref fields) => {
                    let first_field_name = fields
                        .named
                        .first()
                        .expect("not a unit struct")
                        .ident
                        .as_ref()
                        .expect("not an unnamed struct")
                        .to_string();
                    let fields = decode_named_fields(
                        fields,
                        tarantool_crate,
                        None,
                        args.allow_array_optionals,
                    );
                    quote! {
                        let as_map = match context.struct_style() {
                            StructStyle::Default => #as_map,
                            StructStyle::ForceAsMap => true,
                            StructStyle::ForceAsArray => false,
                        };
                        // TODO: Assert map and array len with number of struct fields
                        if as_map {
                            #tarantool_crate::msgpack::rmp::decode::read_map_len(r)
                                .map_err(|err| #tarantool_crate::msgpack::DecodeError::from_vre::<Self>(err))?;
                        } else {
                            #tarantool_crate::msgpack::rmp::decode::read_array_len(r)
                                .map_err(|err| #tarantool_crate::msgpack::DecodeError::from_vre_with_field::<Self>(err, #first_field_name))?;
                        }
                        #fields
                    }
                }
                Fields::Unnamed(ref fields) => {
                    if as_map {
                        abort!(
                            attrs_span(),
                            "`as_map` attribute can be specified only for structs with named fields"
                        );
                    }

                    let mut option_key = TokenStream::new();
                    if fields.unnamed.len() == 1 {
                        let first_field = fields.unnamed.first().expect("len is sufficient");
                        let is_option = first_field.ty.is_option();
                        if is_option {
                            option_key = quote! {
                                if r.is_empty() {
                                    return Ok(Self(None));
                                }
                            };
                        }
                    }

                    let fields = decode_unnamed_fields(
                        fields,
                        tarantool_crate,
                        None,
                        args.allow_array_optionals,
                    );
                    quote! {
                        #option_key
                        #tarantool_crate::msgpack::rmp::decode::read_array_len(r)
                            .map_err(|err| #tarantool_crate::msgpack::DecodeError::from_vre::<Self>(err))?;
                        #fields
                    }
                }
                Fields::Unit => {
                    quote! {
                        let () = #tarantool_crate::msgpack::Decode::decode(r, context)?;
                        Ok(Self)
                    }
                }
            },
            Data::Enum(ref variants) => {
                if as_map {
                    abort!(
                        attrs_span(),
                        "`as_map` attribute can be specified only for structs"
                    );
                }
                let mut variant_reprs = Vec::new();
                let variants: proc_macro2::TokenStream = variants
                    .variants
                    .iter()
                    .flat_map(|variant| {
                        let variant_ident = &variant.ident;
                        let variant_repr = format_ident!("{}", variant_ident).to_string();
                        variant_reprs.push(variant_repr.clone());
                        let variant_repr = proc_macro2::Literal::byte_string(variant_repr.as_bytes());

                        match variant.fields {
                            Fields::Named(ref fields) => {
                                let fields = decode_named_fields(fields, tarantool_crate, Some(&variant.ident), args.allow_array_optionals);
                                // TODO: allow `#[encode(as_map)]` for struct variants
                                quote! {
                                    #variant_repr => {
                                        #tarantool_crate::msgpack::rmp::decode::read_array_len(r)
                                            .map_err(|err| #tarantool_crate::msgpack::DecodeError::from_vre::<Self>(err))?;
                                        let as_map = false;
                                        #fields
                                    }
                                }
                            },
                            Fields::Unnamed(ref fields) => {
                                let fields = decode_unnamed_fields(fields, tarantool_crate, Some(&variant.ident), args.allow_array_optionals);
                                quote! {
                                    #variant_repr => {
                                        #tarantool_crate::msgpack::rmp::decode::read_array_len(r)
                                            .map_err(|err| #tarantool_crate::msgpack::DecodeError::from_vre::<Self>(err))?;
                                        let as_map = false;
                                        #fields
                                    }
                                }
                            }
                            Fields::Unit => {
                                quote! {
                                    #variant_repr => {
                                        let () = #tarantool_crate::msgpack::Decode::decode(r, context)
                                            .map_err(|err| #tarantool_crate::msgpack::DecodeError::new::<Self>(err))?;
                                        Ok(Self::#variant_ident)
                                    }
                                }
                            },
                        }
                    })
                    .collect();
                quote! {
                    // TODO: assert map len 1
                    #tarantool_crate::msgpack::rmp::decode::read_map_len(r)
                        .map_err(|err| #tarantool_crate::msgpack::DecodeError::from_vre::<Self>(err))?;
                    let len = rmp::decode::read_str_len(r)
                        .map_err(|err| #tarantool_crate::msgpack::DecodeError::from_vre::<Self>(err).with_part("variant name"))?;
                    let variant_name = r.get(0..(len as usize))
                        .ok_or_else(|| #tarantool_crate::msgpack::DecodeError::new::<Self>("not enough data").with_part("variant name"))?;
                    *r = &r[(len as usize)..]; // advance
                    match variant_name {
                        #variants
                        other => {
                            let err = if let Ok(other) = String::from_utf8(other.to_vec()) {
                                format!("enum variant {} does not exist", other)
                            } else {
                                format!("enum variant {:?} is invalid utf8", other)
                            };
                            return Err(#tarantool_crate::msgpack::DecodeError::new::<Self>(err));
                        }
                    }
                }
            }
            Data::Union(_) => unimplemented!(),
        }
    }
}

/// Utility function to get a span range of the attributes.
fn attrs_span<'a>(attrs: impl IntoIterator<Item = &'a Attribute>) -> SpanRange {
    SpanRange::from_tokens(
        &attrs
            .into_iter()
            .flat_map(ToTokens::into_token_stream)
            .collect::<TokenStream2>(),
    )
}

/// Collects all lifetimes from `syn::Generic` into `syn::Punctuated` iterator
/// in a format like: `'a + 'b + 'c` and so on.
#[inline]
fn collect_lifetimes(generics: &syn::Generics) -> Punctuated<syn::Lifetime, Token![+]> {
    let mut lifetimes = Punctuated::new();
    let mut unique_lifetimes = std::collections::HashSet::new();

    for param in &generics.params {
        if let syn::GenericParam::Lifetime(lifetime_def) = param {
            if unique_lifetimes.insert(lifetime_def.lifetime.clone()) {
                lifetimes.push(lifetime_def.lifetime.clone());
            }
        }
    }

    lifetimes
}

/// Macro to automatically derive `tarantool::msgpack::Encode`
/// Deriving this trait will make this struct encodable into msgpack format.
/// It is meant as a replacement for serde + rmp_serde
/// allowing us to customize it for tarantool case and hopefully also decreasing compile-time due to its simplicity.
///
/// For more information see `tarantool::msgpack::Encode`
#[proc_macro_error]
#[proc_macro_derive(Encode, attributes(encode))]
pub fn derive_encode(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    // Get attribute arguments
    let args: msgpack::Args = darling::FromDeriveInput::from_derive_input(&input).unwrap();
    let tarantool_crate = args.tarantool.as_deref().unwrap_or("tarantool");
    let tarantool_crate = Ident::new(tarantool_crate, Span::call_site()).into();

    // Add a bound to every type parameter.
    let generics = msgpack::add_trait_bounds(input.generics, &tarantool_crate);
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
    let encode_fields = msgpack::encode_fields(
        &input.data,
        &tarantool_crate,
        // Use a closure as the function might be costly, but is only used for errors
        // and we don't want to slow down compilation.
        || attrs_span(&input.attrs),
        &args,
    );
    let expanded = quote! {
        // The generated impl.
        impl #impl_generics #tarantool_crate::msgpack::Encode for #name #ty_generics #where_clause {
            fn encode(&self, w: &mut impl ::std::io::Write, context: &#tarantool_crate::msgpack::Context)
                -> Result<(), #tarantool_crate::msgpack::EncodeError>
            {
                use #tarantool_crate::msgpack::StructStyle;
                #encode_fields
                Ok(())
            }
        }
    };

    expanded.into()
}

/// Macro to automatically derive `tarantool::msgpack::Decode`
/// Deriving this trait will allow decoding this struct from msgpack format.
/// It is meant as a replacement for serde + rmp_serde
/// allowing us to customize it for tarantool case and hopefully also decreasing compile-time due to its simplicity.
///
/// For more information see `tarantool::msgpack::Decode`
#[proc_macro_error]
#[proc_macro_derive(Decode, attributes(encode))]
pub fn derive_decode(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    // Get attribute arguments
    let args: msgpack::Args = darling::FromDeriveInput::from_derive_input(&input).unwrap();
    let tarantool_crate = args.tarantool.as_deref().unwrap_or("tarantool");
    let tarantool_crate = Ident::new(tarantool_crate, Span::call_site()).into();

    // Add a bound to every type parameter.
    let generics = msgpack::add_trait_bounds(input.generics.clone(), &tarantool_crate);
    let mut impl_generics = input.generics;
    impl_generics.params.insert(
        0,
        syn::GenericParam::Lifetime(syn::LifetimeDef {
            attrs: vec![],
            lifetime: syn::Lifetime::new("'de", Span::call_site()),
            colon_token: Some(syn::token::Colon::default()),
            bounds: collect_lifetimes(&generics),
        }),
    );
    // let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
    let (impl_generics, _, where_clause) = impl_generics.split_for_impl();
    let (_, ty_generics, _) = generics.split_for_impl();
    let decode_fields = msgpack::decode_fields(
        &input.data,
        &tarantool_crate,
        // Use a closure as the function might be costly, but is only used for errors
        // and we don't want to slow down compilation.
        || attrs_span(&input.attrs),
        &args,
    );
    let expanded = quote! {
        // The generated impl.
        impl #impl_generics #tarantool_crate::msgpack::Decode<'de> for #name #ty_generics #where_clause {
            fn decode(r: &mut &'de [u8], context: &#tarantool_crate::msgpack::Context)
                -> std::result::Result<Self, #tarantool_crate::msgpack::DecodeError>
            {
                use #tarantool_crate::msgpack::StructStyle;
                #decode_fields
            }
        }
    };

    expanded.into()
}

/// Create a tarantool stored procedure.
///
/// See `tarantool::proc` doc-comments in tarantool crate for details.
#[proc_macro_attribute]
pub fn stored_proc(attr: TokenStream, item: TokenStream) -> TokenStream {
    let args = parse_macro_input!(attr as AttributeArgs);
    let ctx = Context::from_args(args);

    let input = parse_macro_input!(item as Item);

    #[rustfmt::skip]
    let ItemFn { vis, sig, block, attrs, .. } = match input {
        Item::Fn(f) => f,
        _ => panic!("only `fn` items can be stored procedures"),
    };

    let (ident, inputs, output, generics) = match sig {
        Signature {
            asyncness: Some(_), ..
        } => {
            panic!("async stored procedures are not supported yet")
        }
        Signature {
            variadic: Some(_), ..
        } => {
            panic!("variadic stored procedures are not supported yet")
        }
        Signature {
            ident,
            inputs,
            output,
            generics,
            ..
        } => (ident, inputs, output, generics),
    };

    let Inputs {
        inputs,
        input_pattern,
        input_idents,
        inject_inputs,
        n_actual_arguments,
    } = Inputs::parse(&ctx, inputs);

    if ctx.is_packed && n_actual_arguments > 1 {
        panic!("proc with 'packed_args' can only have a single parameter")
    }

    let Context {
        tarantool,
        linkme,
        section,
        debug_tuple,
        wrap_ret,
        ..
    } = ctx;

    let inner_fn_name = syn::Ident::new("__tp_inner", ident.span());
    let desc_name = ident.to_string();
    let desc_ident = syn::Ident::new(&desc_name.to_uppercase(), ident.span());
    let mut public = matches!(vis, syn::Visibility::Public(_));
    if let Some(override_public) = ctx.public {
        public = override_public;
    }

    quote! {
        #[#linkme::distributed_slice(#section)]
        #[linkme(crate = #linkme)]
        #[cfg(not(test))]
        static #desc_ident: #tarantool::proc::Proc = #tarantool::proc::Proc::new(
            #desc_name,
            #ident,
        ).with_public(#public);

        #(#attrs)*
        #[no_mangle]
        pub unsafe extern "C" fn #ident (
            __tp_ctx: #tarantool::tuple::FunctionCtx,
            __tp_args: #tarantool::tuple::FunctionArgs,
        ) -> ::std::os::raw::c_int {
            #debug_tuple
            let #input_pattern =
                match __tp_args.decode() {
                    ::std::result::Result::Ok(__tp_args) => __tp_args,
                    ::std::result::Result::Err(__tp_err) => {
                        #tarantool::set_error!(
                            #tarantool::error::TarantoolErrorCode::ProcC,
                            "{}",
                            __tp_err
                        );
                        return -1;
                    }
                };

            #inject_inputs

            fn #inner_fn_name #generics (#inputs) #output {
                #block
            }

            let __tp_res = __tp_inner(#(#input_idents),*);

            #wrap_ret

            #tarantool::proc::Return::ret(__tp_res, __tp_ctx)
        }
    }
    .into()
}

struct Context {
    tarantool: syn::Path,
    section: syn::Path,
    linkme: syn::Path,
    debug_tuple: TokenStream2,
    is_packed: bool,
    public: Option<bool>,
    wrap_ret: TokenStream2,
}

impl Context {
    fn from_args(args: AttributeArgs) -> Self {
        let mut tarantool: syn::Path = syn::parse2(quote! { ::tarantool }).unwrap();
        let mut linkme = None;
        let mut section = None;
        let mut debug_tuple_needed = false;
        let mut is_packed = false;
        let mut public = None;
        let mut wrap_ret = quote! {};

        for arg in args {
            if let Some(path) = imp::parse_lit_str_with_key(&arg, "tarantool") {
                tarantool = path;
                continue;
            }
            if let Some(path) = imp::parse_lit_str_with_key(&arg, "linkme") {
                linkme = Some(path);
                continue;
            }
            if let Some(path) = imp::parse_lit_str_with_key(&arg, "section") {
                section = Some(path);
                continue;
            }
            if imp::is_path_eq_to(&arg, "custom_ret") {
                wrap_ret = quote! {
                    let __tp_res = #tarantool::proc::ReturnMsgpack(__tp_res);
                };
                continue;
            }
            if imp::is_path_eq_to(&arg, "packed_args") {
                is_packed = true;
                continue;
            }
            if imp::is_path_eq_to(&arg, "debug") {
                debug_tuple_needed = true;
                continue;
            }
            if let Some(v) = imp::parse_bool_with_key(&arg, "public") {
                public = Some(v);
                continue;
            }
            panic!("unsuported attribute argument `{}`", quote!(#arg))
        }

        let section = section.unwrap_or_else(|| {
            imp::path_from_ts2(quote! { #tarantool::proc::TARANTOOL_MODULE_STORED_PROCS })
        });
        let linkme = linkme.unwrap_or_else(|| imp::path_from_ts2(quote! { #tarantool::linkme }));

        let debug_tuple = if debug_tuple_needed {
            quote! {
                ::std::dbg!(#tarantool::tuple::Tuple::from(&__tp_args));
            }
        } else {
            quote! {}
        };
        Self {
            tarantool,
            linkme,
            section,
            debug_tuple,
            is_packed,
            wrap_ret,
            public,
        }
    }
}

struct Inputs {
    inputs: Punctuated<FnArg, Token![,]>,
    input_pattern: TokenStream2,
    input_idents: Vec<syn::Pat>,
    inject_inputs: TokenStream2,
    n_actual_arguments: usize,
}

impl Inputs {
    fn parse(ctx: &Context, mut inputs: Punctuated<FnArg, Token![,]>) -> Self {
        let mut input_idents = vec![];
        let mut actual_inputs = vec![];
        let mut injected_inputs = vec![];
        let mut injected_exprs = vec![];
        for i in &mut inputs {
            let syn::PatType {
                ref pat,
                ref mut attrs,
                ..
            } = match i {
                FnArg::Receiver(_) => {
                    panic!("`self` receivers aren't supported in stored procedures")
                }
                FnArg::Typed(pat_ty) => pat_ty,
            };
            let mut inject_expr = None;
            attrs.retain(|attr| {
                let path = &attr.path;
                if path.is_ident("inject") {
                    match attr.parse_args() {
                        Ok(AttrInject { expr, .. }) => {
                            inject_expr = Some(expr);
                            false
                        }
                        Err(e) => panic!("attribute argument error: {}", e),
                    }
                } else {
                    // Skip doc comments as they are not allowed for inner functions
                    !path.is_ident("doc")
                }
            });
            if let Some(expr) = inject_expr {
                injected_inputs.push(pat.clone());
                injected_exprs.push(expr);
            } else {
                actual_inputs.push(pat.clone());
            }
            input_idents.push((**pat).clone());
        }

        let input_pattern = if inputs.is_empty() {
            quote! { []: [(); 0] }
        } else if ctx.is_packed {
            quote! { #(#actual_inputs)* }
        } else {
            quote! { ( #(#actual_inputs,)* ) }
        };

        let inject_inputs = quote! {
            #( let #injected_inputs = #injected_exprs; )*
        };

        Self {
            inputs,
            input_pattern,
            input_idents,
            inject_inputs,
            n_actual_arguments: actual_inputs.len(),
        }
    }
}

#[derive(Debug)]
struct AttrInject {
    expr: syn::Expr,
}

impl syn::parse::Parse for AttrInject {
    fn parse(input: syn::parse::ParseStream) -> syn::Result<Self> {
        Ok(AttrInject {
            expr: input.parse()?,
        })
    }
}

mod kw {
    syn::custom_keyword! {inject}
}

mod imp {
    use proc_macro2::{Group, Span, TokenStream, TokenTree};
    use syn::parse::{self, Parse};

    #[track_caller]
    pub(crate) fn parse_lit_str_with_key<T>(nm: &syn::NestedMeta, key: &str) -> Option<T>
    where
        T: Parse,
    {
        match nm {
            syn::NestedMeta::Meta(syn::Meta::NameValue(syn::MetaNameValue {
                path, lit, ..
            })) if path.is_ident(key) => match &lit {
                syn::Lit::Str(s) => Some(crate::imp::parse_lit_str(s).unwrap()),
                _ => panic!("{key} value must be a string literal"),
            },
            _ => None,
        }
    }

    #[track_caller]
    pub(crate) fn parse_bool_with_key(nm: &syn::NestedMeta, key: &str) -> Option<bool> {
        match nm {
            syn::NestedMeta::Meta(syn::Meta::NameValue(syn::MetaNameValue {
                path, lit, ..
            })) if path.is_ident(key) => match &lit {
                syn::Lit::Bool(b) => Some(b.value),
                _ => panic!("value for attribute '{key}' must be a bool literal (true | false)"),
            },
            syn::NestedMeta::Meta(syn::Meta::Path(path)) if path.is_ident(key) => {
                panic!("expected ({key} = true|false), got just {key}");
            }
            _ => None,
        }
    }

    #[track_caller]
    pub(crate) fn is_path_eq_to(nm: &syn::NestedMeta, expected: &str) -> bool {
        matches!(
            nm,
            syn::NestedMeta::Meta(syn::Meta::Path(path)) if path.is_ident(expected)
        )
    }

    pub(crate) fn path_from_ts2(ts: TokenStream) -> syn::Path {
        syn::parse2(ts).unwrap()
    }

    // stolen from serde

    pub(crate) fn parse_lit_str<T>(s: &syn::LitStr) -> parse::Result<T>
    where
        T: Parse,
    {
        let tokens = spanned_tokens(s)?;
        syn::parse2(tokens)
    }

    fn spanned_tokens(s: &syn::LitStr) -> parse::Result<TokenStream> {
        let stream = syn::parse_str(&s.value())?;
        Ok(respan(stream, s.span()))
    }

    fn respan(stream: TokenStream, span: Span) -> TokenStream {
        stream
            .into_iter()
            .map(|token| respan_token(token, span))
            .collect()
    }

    fn respan_token(mut token: TokenTree, span: Span) -> TokenTree {
        if let TokenTree::Group(g) = &mut token {
            *g = Group::new(g.delimiter(), respan(g.stream(), span));
        }
        token.set_span(span);
        token
    }
}
