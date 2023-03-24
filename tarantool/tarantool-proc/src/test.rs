use crate::imp;
use proc_macro::TokenStream as TS1;
use quote::quote;
use syn::parse_macro_input;

pub fn impl_macro_attribute(attr: TS1, item: TS1) -> TS1 {
    let fn_item = parse_macro_input!(item as syn::ItemFn);
    let args = parse_macro_input!(attr as syn::AttributeArgs);
    let ctx = Context::from_args(args);
    let fn_name = &fn_item.sig.ident;
    let test_name = fn_name.to_string();
    let test_name_ident = syn::Ident::new(&test_name.to_uppercase(), fn_name.span());
    let Context {
        tarantool,
        section,
        linkme,
        should_panic,
    } = ctx;

    let fn_item = if fn_item.sig.asyncness.is_some() {
        let body = fn_item.block;
        quote! {
            fn #fn_name() {
                #tarantool::fiber::block_on(async { #body })
            }
        }
    } else {
        quote! {
            #fn_item
        }
    };

    quote! {
        #[#linkme::distributed_slice(#section)]
        #[linkme(crate = #linkme)]
        static #test_name_ident: #tarantool::test::TestCase = #tarantool::test::TestCase::new(
            ::std::concat!(::std::module_path!(), "::", #test_name),
            #fn_name,
            #should_panic,
        );

        #fn_item
    }
    .into()
}

#[derive(Debug)]
struct Context {
    tarantool: syn::Path,
    section: syn::Path,
    linkme: syn::Path,
    should_panic: bool,
}

impl Context {
    fn from_args(args: syn::AttributeArgs) -> Self {
        let mut tarantool = imp::path_from_ts2(quote! { ::tarantool });
        let mut linkme = None;
        let mut section = None;
        let mut should_panic = false;

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
            if imp::is_path_eq_to(&arg, "should_panic") {
                should_panic = true;
                continue;
            }
            panic!("unsuported attribute argument: {:?}", arg)
        }

        let section = section.unwrap_or_else(|| {
            imp::path_from_ts2(quote! { #tarantool::test::TARANTOOL_MODULE_TESTS })
        });

        let linkme = linkme.unwrap_or_else(|| imp::path_from_ts2(quote! { #tarantool::linkme }));

        Self {
            tarantool,
            section,
            linkme,
            should_panic,
        }
    }
}
