use proc_macro::TokenStream;
use proc_macro2::{Span, TokenStream as TokenStream2};
use proc_macro_error::{proc_macro_error, SpanRange};
use quote::{quote, ToTokens};
use syn::{
    parse_macro_input, punctuated::Punctuated, Attribute, AttributeArgs, DeriveInput, FnArg, Ident,
    Item, ItemFn, Signature, Token,
};

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
    use proc_macro_error::{abort, SpanRange};
    use quote::{format_ident, quote, quote_spanned};
    use syn::{
        parse_quote, spanned::Spanned, Data, Fields, FieldsNamed, FieldsUnnamed, GenericParam,
        Generics, Index, Path,
    };

    #[derive(Default, FromDeriveInput)]
    #[darling(attributes(encode), default)]
    pub struct EncodeArgs {
        /// Whether this struct should be serialized as MP_MAP instead of MP_ARRAY.
        pub as_map: bool,
        /// Path to tarantool crate.
        pub tarantool: Option<String>,
    }

    pub fn add_trait_bounds(mut generics: Generics, tarantool_crate: &Path) -> Generics {
        for param in &mut generics.params {
            if let GenericParam::Type(ref mut type_param) = *param {
                type_param
                    .bounds
                    .push(parse_quote!(#tarantool_crate::tuple::_Encode));
            }
        }
        generics
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
                let name = &f.ident;
                let s = if add_self {
                    quote! {&self.}
                } else {
                    quote! {}
                };
                // TODO: allow `#[encode(as_map)]` and `#[encode(as_vec)]` for struct fields
                // to overwrite external structure encoding behavior
                quote_spanned! {f.span()=>
                    if as_map {
                        #tarantool_crate::tuple::rmp::encode::write_str(w,
                            stringify!(#name).trim_start_matches("r#"))?;
                    }
                    #tarantool_crate::tuple::_Encode::encode(#s #name, w, EncodeStyle::Default)?;
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
                quote_spanned! {f.span()=>
                    #tarantool_crate::tuple::_Encode::encode(&self.#index, w, EncodeStyle::Default)?;
                }
            })
            .collect()
    }

    pub fn encode_fields(
        data: &Data,
        tarantool_crate: &Path,
        attrs_span: impl Fn() -> SpanRange,
        as_map: bool,
    ) -> proc_macro2::TokenStream {
        match *data {
            Data::Struct(ref data) => match data.fields {
                Fields::Named(ref fields) => {
                    let field_count = fields.named.len() as u32;
                    let fields = encode_named_fields(fields, tarantool_crate, true);
                    quote! {
                        let as_map = match style {
                            EncodeStyle::Default => #as_map,
                            EncodeStyle::ForceAsMap => true,
                            EncodeStyle::ForceAsArray => false,
                        };
                        if as_map {
                            #tarantool_crate::tuple::rmp::encode::write_map_len(w, #field_count)?;
                        } else {
                            #tarantool_crate::tuple::rmp::encode::write_array_len(w, #field_count)?;
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
                        #tarantool_crate::tuple::rmp::encode::write_array_len(w, #field_count)?;
                        #fields
                    }
                }
                Fields::Unit => {
                    quote!(#tarantool_crate::tuple::_Encode::encode(&(), w, EncodeStyle::Default)?;)
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
                    .flat_map(|variant| match variant.fields {
                        Fields::Named(ref fields) => {
                            let field_count = fields.named.len() as u32;
                            let variant_name = &variant.ident;
                            let field_names = fields.named.iter().map(|field| field.ident.clone());
                            let fields = encode_named_fields(fields, tarantool_crate, false);
                            // TODO: allow `#[encode(as_map)]` for struct variants
                            quote! {
                                 Self::#variant_name { #(#field_names),*} => {
                                    #tarantool_crate::tuple::rmp::encode::write_map_len(w, 1)?;
                                    #tarantool_crate::tuple::rmp::encode::write_str(w, stringify!(#variant_name).trim_start_matches("r#"))?;
                                    #tarantool_crate::tuple::rmp::encode::write_array_len(w, #field_count)?;
                                    let as_map = false;
                                    #fields
                                }
                            }
                        },
                        Fields::Unnamed(ref fields) => {
                            let field_count = fields.unnamed.len() as u32;
                            let variant_name = &variant.ident;
                            let field_names = fields.unnamed.iter().enumerate().map(|(i, _)| format_ident!("t{}", i));
                            let fields: proc_macro2::TokenStream = field_names.clone()
                                .flat_map(|field_name| quote! {
                                    #tarantool_crate::tuple::_Encode::encode(#field_name, w, EncodeStyle::Default)?;
                                })
                                .collect();
                            if field_count > 1 {
                                quote! {
                                    Self::#variant_name ( #(#field_names),* ) => {
                                        #tarantool_crate::tuple::rmp::encode::write_map_len(w, 1)?;
                                        #tarantool_crate::tuple::rmp::encode::write_str(w,
                                            stringify!(#variant_name).trim_start_matches("r#"))?;
                                        #tarantool_crate::tuple::rmp::encode::write_array_len(w, #field_count)?;
                                        #fields
                                    }
                                }
                            } else {
                                quote! {
                                    Self::#variant_name ( v ) => {
                                        #tarantool_crate::tuple::rmp::encode::write_map_len(w, 1)?;
                                        #tarantool_crate::tuple::rmp::encode::write_str(w,
                                            stringify!(#variant_name).trim_start_matches("r#"))?;
                                        #tarantool_crate::tuple::_Encode::encode(v, w, EncodeStyle::Default)?;
                                    }
                                }
                            }
                        }
                        Fields::Unit => {
                            let variant_name = &variant.ident;
                            quote! {
                                Self::#variant_name => {
                                    #tarantool_crate::tuple::rmp::encode::write_str(w, stringify!(#variant_name))?;
                                }
                            }
                        },
                    })
                    .collect();
                quote! {
                    match self {
                        #variants
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

/// Macro to automatically derive `tarantool::tuple::_Encode`
/// Deriving this trait will make this struct encodable into msgpack format.
/// It is meant as a replacement for serde + rmp_serde
/// allowing us to customize it for tarantool case and hopefully also decreasing compile-time due to its simplicity.
///
/// For more information see `tarantool::tuple::_Encode`
#[proc_macro_error]
#[proc_macro_derive(Encode, attributes(encode))]
pub fn derive_encode(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    // Get attribute arguments
    let args: msgpack::EncodeArgs = darling::FromDeriveInput::from_derive_input(&input).unwrap();
    let tarantool_crate = args.tarantool.unwrap_or_else(|| "tarantool".to_string());
    let tarantool_crate = Ident::new(tarantool_crate.as_str(), Span::call_site()).into();

    // Add a bound to every type parameter.
    let generics = msgpack::add_trait_bounds(input.generics, &tarantool_crate);
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
    let encode_fields = msgpack::encode_fields(
        &input.data,
        &tarantool_crate,
        // Use a closure as the function might be costly, but is only used for errors
        // and we don't want to slow down compilation.
        || attrs_span(&input.attrs),
        args.as_map,
    );
    let expanded = quote! {
        // The generated impl.
        impl #impl_generics #tarantool_crate::tuple::_Encode for #name #ty_generics #where_clause {
            fn encode(&self, w: &mut impl ::std::io::Write, style: #tarantool_crate::tuple::EncodeStyle) -> #tarantool_crate::Result<()> {
                use #tarantool_crate::tuple::EncodeStyle;
                #encode_fields
                Ok(())
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

    let ItemFn {
        sig, block, attrs, ..
    } = match input {
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

    quote! {
        #[#linkme::distributed_slice(#section)]
        #[linkme(crate = #linkme)]
        #[cfg(not(test))]
        static #desc_ident: #tarantool::proc::Proc = #tarantool::proc::Proc::new(
            #desc_name,
            #ident,
        );

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
    wrap_ret: TokenStream2,
}

impl Context {
    fn from_args(args: AttributeArgs) -> Self {
        let mut tarantool: syn::Path = syn::parse2(quote! { ::tarantool }).unwrap();
        let mut linkme = None;
        let mut section = None;
        let mut debug_tuple_needed = false;
        let mut is_packed = false;
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
            panic!("unsuported attribute argument: {:?}", arg)
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
                if attr.path.is_ident("inject") {
                    match attr.parse_args() {
                        Ok(AttrInject { expr, .. }) => {
                            inject_expr = Some(expr);
                            false
                        }
                        Err(e) => panic!("attribute argument error: {}", e),
                    }
                } else {
                    true
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
