use proc_macro::TokenStream;
use proc_macro2::{TokenStream as TokenStream2};
use syn::{
    AttributeArgs, parse_macro_input, FnArg, Item, ItemFn,
    Lit, Meta, MetaNameValue, NestedMeta, Signature,
    punctuated::Punctuated, Token,
};
use quote::quote;

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
        Signature { asyncness: Some(_), .. } => {
            panic!("async stored procedures are not supported yet")
        }
        Signature { variadic: Some(_), .. } => {
            panic!("variadic stored procedures are not supported yet")
        }
        Signature { ident, inputs, output, generics, .. } => {
            (ident, inputs, output, generics)
        }
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
            let __tp_tuple = #tarantool::tuple::Tuple::from(__tp_args);
            #debug_tuple
            let #input_pattern =
                match __tp_tuple.into_struct() {
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
    }.into()
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
        let mut debug_tuple = quote! {};
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
                NMMeta(Meta::Path(path)) if path.is_ident("packed_args") => {
                    is_packed = true
                }
                NMMeta(Meta::Path(path)) if path.is_ident("debug") => {
                    debug_tuple = quote! {
                        let __tp_tuple = ::std::dbg!(__tp_tuple);
                    }
                }
                NMMeta(Meta::NameValue(MetaNameValue {
                    path,
                    lit,
                    ..
                })) if path.get_ident()
                        .map(|p| p == "tarantool")
                        .unwrap_or(false) => {
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
            let syn::PatType { ref pat, ref mut attrs, .. } = match i {
                FnArg::Receiver(_) => {
                    panic!("`self` receivers aren't supported in stored procedures")
                }
                FnArg::Typed(pat_ty) => pat_ty,
            };
            let mut inject_expr = None;
            attrs.retain(|attr|
                if attr.path.is_ident("inject") {
                    match attr.parse_args() {
                        Ok(AttrInject { expr, .. }) =>  {
                            inject_expr = Some(expr);
                            false
                        }
                        Err(e) => panic!("attribute argument error: {}", e)
                    }
                } else {
                    true
                }
            );
            if let Some(expr) = inject_expr {
                injected_inputs.push(pat.clone());
                injected_exprs.push(expr);
            } else {
                actual_inputs.push(pat.clone());
            }
            input_idents.push((**pat).clone());
        }

        let input_pattern = if inputs.is_empty() {
            quote!{ []: [(); 0] }
        } else if ctx.is_packed {
            quote!{ #(#actual_inputs)* }
        } else {
            quote!{ ( #(#actual_inputs,)* ) }
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
    syn::custom_keyword!{inject}
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

