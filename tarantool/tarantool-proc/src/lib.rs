use proc_macro::TokenStream;
use proc_macro2::{Span, TokenStream as TokenStream2};
use quote::quote;
use syn::{
    parse_macro_input, punctuated::Punctuated, AttributeArgs, DeriveInput, FnArg, Ident, Item,
    ItemFn, Lit, Meta, MetaNameValue, NestedMeta, Signature, Token,
};

mod msgpack {
    use darling::FromDeriveInput;
    use quote::{format_ident, quote, quote_spanned};
    use syn::{
        parse_quote, spanned::Spanned, Data, Fields, FieldsNamed, FieldsUnnamed, GenericParam,
        Generics, Index, Path,
    };

    #[derive(FromDeriveInput)]
    #[darling(attributes(encode))]
    pub struct EncodeArgs {
        /// Path to tarantool crate
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
                quote_spanned! {f.span()=>
                    if struct_as_map {
                        #tarantool_crate::tuple::rmp::encode::write_str(w,
                            stringify!(#name).trim_start_matches("r#"))?;
                    }
                    #tarantool_crate::tuple::_Encode::encode(#s #name, w, struct_as_map)?;
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
                    #tarantool_crate::tuple::_Encode::encode(&self.#index, w, struct_as_map)?;
                }
            })
            .collect()
    }

    pub fn encode_fields(data: &Data, tarantool_crate: &Path) -> proc_macro2::TokenStream {
        match *data {
            Data::Struct(ref data) => match data.fields {
                Fields::Named(ref fields) => {
                    let field_count = fields.named.len() as u32;
                    let fields = encode_named_fields(fields, tarantool_crate, true);
                    quote! {
                        if struct_as_map {
                            #tarantool_crate::tuple::rmp::encode::write_map_len(w, #field_count)?;
                        } else {
                            #tarantool_crate::tuple::rmp::encode::write_array_len(w, #field_count)?;
                        }
                        #fields
                    }
                }
                Fields::Unnamed(ref fields) => {
                    let field_count = fields.unnamed.len() as u32;
                    let fields = encode_unnamed_fields(fields, tarantool_crate);
                    quote! {
                        #tarantool_crate::tuple::rmp::encode::write_array_len(w, #field_count)?;
                        #fields
                    }
                }
                Fields::Unit => {
                    quote!(#tarantool_crate::tuple::_Encode::encode(&(), w, struct_as_map)?;)
                }
            },
            Data::Enum(ref variants) => {
                let variants: proc_macro2::TokenStream = variants
                    .variants
                    .iter()
                    .flat_map(|variant| match variant.fields {
                        Fields::Named(ref fields) => {
                            let field_count = fields.named.len() as u32;
                            let variant_name = &variant.ident;
                            let field_names = fields.named.iter().map(|field| field.ident.clone());
                            let fields = encode_named_fields(fields, tarantool_crate, false);
                            quote! {
                                 Self::#variant_name { #(#field_names),*} => {
                                    #tarantool_crate::tuple::rmp::encode::write_map_len(w, 1)?;
                                    #tarantool_crate::tuple::rmp::encode::write_str(w,
                                        stringify!(#variant_name).trim_start_matches("r#"))?;
                                    if struct_as_map {
                                        #tarantool_crate::tuple::rmp::encode::write_map_len(w, #field_count)?;
                                    } else {
                                        #tarantool_crate::tuple::rmp::encode::write_array_len(w, #field_count)?;
                                    }
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
                                    #tarantool_crate::tuple::_Encode::encode(#field_name, w, struct_as_map)?;
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
                                        #tarantool_crate::tuple::_Encode::encode(v, w, struct_as_map)?;
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

/// Macro to automatically derive `tarantool::tuple::_Encode`
/// Deriving this trait will make this struct encodable into msgpack format.
/// It is meant as a replacement for serde + rmp_serde
/// allowing us to customize it for tarantool case and hopefully also decreasing compile-time due to its simplicity.
///
/// For more information see `tarantool::tuple::_Encode`
#[proc_macro_derive(Encode, attributes(encode))]
pub fn derive_encode(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;
    let args: msgpack::EncodeArgs = darling::FromDeriveInput::from_derive_input(&input).unwrap();
    let tarantool_crate = args.tarantool.unwrap_or_else(|| "tarantool".to_string());
    let tarantool_crate = Ident::new(tarantool_crate.as_str(), Span::call_site()).into();
    // Add a bound to every type parameter.
    let generics = msgpack::add_trait_bounds(input.generics, &tarantool_crate);
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
    let encode_fields = msgpack::encode_fields(&input.data, &tarantool_crate);
    let expanded = quote! {
        // The generated impl.
        impl #impl_generics #tarantool_crate::tuple::_Encode for #name #ty_generics #where_clause {
            fn encode(&self, w: &mut impl ::std::io::Write, struct_as_map: bool) -> #tarantool_crate::Result<()> {
                #encode_fields
                Ok(())
            }
        }
    };

    expanded.into()
}

#[proc_macro]
pub fn impl_tuple_encode(_input: TokenStream) -> TokenStream {
    let mut impls = vec![];
    for i in 1usize..=16usize {
        let is: Vec<_> = (0..i).into_iter().map(syn::Index::from).collect();
        let tys: Vec<_> = (0..i)
            .into_iter()
            .map(|i| Ident::new(format!("T{i}").as_str(), Span::call_site()))
            .collect();
        impls.push(quote! {
            impl<#(#tys),*> _Encode for (#(#tys,)*)
            where
                #(#tys: _Encode,)*
            {
                fn encode(&self, w: &mut impl Write, struct_as_map: bool) -> Result<()> {
                    rmp::encode::write_array_len(w, #i as u32)?;
                    #(self.#is.encode(w, struct_as_map)?;)*
                    Ok(())
                }
            }
        });
    }
    impls
        .into_iter()
        .flatten()
        .collect::<proc_macro2::TokenStream>()
        .into()
}

#[proc_macro_attribute]
pub fn stored_proc(attr: TokenStream, item: TokenStream) -> TokenStream {
    let args = parse_macro_input!(attr as AttributeArgs);
    let ctx = Context::from_args(args);

    let input = parse_macro_input!(item as Item);

    let ItemFn { sig, block, .. } = match input {
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
        debug_tuple,
        wrap_ret,
        ..
    } = ctx;

    let inner_fn_name = syn::Ident::new("__tp_inner", ident.span());

    quote! {
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
    tarantool: TokenStream2,
    debug_tuple: TokenStream2,
    is_packed: bool,
    wrap_ret: TokenStream2,
}

impl Context {
    fn from_args(args: AttributeArgs) -> Self {
        let mut tarantool = quote! { ::tarantool };
        let mut debug_tuple_needed = false;
        let mut is_packed = false;
        let mut wrap_ret = quote! {};

        use syn::NestedMeta::{Lit as NMLit, Meta as NMMeta};
        for arg in args {
            match arg {
                NMLit(lit) => {
                    eprintln!("unsuported attribute argument: {:?}", lit)
                }
                NMMeta(Meta::Path(path)) if path.is_ident("custom_ret") => {
                    wrap_ret = quote! {
                        let __tp_res = #tarantool::proc::ReturnMsgpack(__tp_res);
                    }
                }
                NMMeta(Meta::Path(path)) if path.is_ident("packed_args") => is_packed = true,
                NMMeta(Meta::Path(path)) if path.is_ident("debug") => debug_tuple_needed = true,
                NMMeta(Meta::NameValue(MetaNameValue { path, lit, .. }))
                    if path.get_ident().map(|p| p == "tarantool").unwrap_or(false) =>
                {
                    match &lit {
                        Lit::Str(s) => {
                            let tp: syn::Path = imp::parse_lit_str(s).unwrap();
                            tarantool = quote! { #tp };
                        }
                        _ => panic!("tarantool value must be a string literal"),
                    }
                }
                NestedMeta::Meta(meta) => {
                    eprintln!("unsuported attribute argument: {:?}", meta)
                }
            }
        }

        let debug_tuple = if debug_tuple_needed {
            quote! {
                ::std::dbg!(#tarantool::tuple::Tuple::from(&__tp_args));
            }
        } else {
            quote! {}
        };
        Self {
            tarantool,
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
