use quote::quote;

use crate::default_tarantool_crate_path;

macro_rules! unwrap_or_compile_error {
    ($expr:expr) => {
        match $expr {
            Ok(v) => v,
            Err(e) => {
                return e.to_compile_error().into();
            }
        }
    };
}

pub fn impl_macro_attribute(
    attr: proc_macro::TokenStream,
    item: proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    let fn_item = syn::parse_macro_input!(item as syn::ItemFn);
    let ctx = unwrap_or_compile_error!(Context::from_args(attr.into()));
    let fn_name = &fn_item.sig.ident;
    let test_name = fn_name.to_string();
    let unique_name = format!("TARANTOOL_MODULE_TEST_CASE_{}", test_name.to_uppercase());
    let test_name_ident = syn::Ident::new(&unique_name, fn_name.span());
    let Context {
        tarantool,
        section,
        linkme,
        should_panic,
        skip_if,
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

    let make_test_case = |skip: proc_macro2::TokenStream| {
        quote! {
            #[#linkme::distributed_slice(#section)]
            #[linkme(crate = #linkme)]
            #[used]
            static #test_name_ident: #tarantool::test::TestCase = #tarantool::test::TestCase::new(
                ::std::concat!(::std::module_path!(), "::", #test_name),
                #fn_name,
                #should_panic,
                #skip,
            );
        }
    };

    let test_case_registration = match skip_if {
        Some(SkipIf {
            cfg_condition,
            reason,
        }) => {
            let skipped = make_test_case(quote! { ::std::option::Option::Some(#reason) });
            let not_skipped = make_test_case(quote! { ::std::option::Option::None });
            quote! {
                #[cfg(#cfg_condition)]
                #skipped

                #[cfg(not(#cfg_condition))]
                #not_skipped
            }
        }
        None => make_test_case(quote! { ::std::option::Option::None }),
    };

    quote! {
        #test_case_registration
        #fn_item
    }
    .into()
}

#[derive(Debug)]
struct SkipIf {
    cfg_condition: proc_macro2::TokenStream,
    reason: syn::LitStr,
}

#[derive(Debug)]
struct Context {
    tarantool: syn::Path,
    section: syn::Path,
    linkme: syn::Path,
    should_panic: syn::Expr,
    skip_if: Option<SkipIf>,
}

impl Context {
    fn from_args(tokens: proc_macro2::TokenStream) -> Result<Self, syn::Error> {
        let mut tarantool = default_tarantool_crate_path();
        let mut linkme = None;
        let mut section = None;
        let mut should_panic = syn::parse_quote! { false };
        let mut skip_if = None;

        syn::parse::Parser::parse2(
            |input: syn::parse::ParseStream| -> Result<(), syn::Error> {
                while !input.is_empty() {
                    let ident: syn::Ident = input.parse()?;
                    if ident == "tarantool" {
                        input.parse::<syn::Token![=]>()?;
                        let value: syn::LitStr = input.parse()?;
                        tarantool = value.parse()?;
                    } else if ident == "linkme" {
                        input.parse::<syn::Token![=]>()?;
                        let value: syn::LitStr = input.parse()?;
                        linkme = Some(value.parse()?);
                    } else if ident == "section" {
                        input.parse::<syn::Token![=]>()?;
                        let value: syn::LitStr = input.parse()?;
                        section = Some(value.parse()?);
                    } else if ident == "should_panic" {
                        if input.parse::<syn::Token![=]>().is_ok() {
                            should_panic = input.parse()?;
                        } else {
                            should_panic = syn::parse_quote! { true };
                        }
                    } else if ident == "skip_if" {
                        mod kw {
                            syn::custom_keyword!(cfg);
                            syn::custom_keyword!(reason);
                        }

                        let content;
                        syn::parenthesized!(content in input);

                        content.parse::<kw::cfg>()?;

                        let cfg_content;
                        syn::parenthesized!(cfg_content in content);
                        let cfg_condition: proc_macro2::TokenStream = cfg_content.parse()?;

                        content.parse::<syn::Token![,]>()?;
                        content.parse::<kw::reason>()?;
                        content.parse::<syn::Token![=]>()?;

                        let reason: syn::LitStr = content.parse()?;

                        skip_if = Some(SkipIf {
                            cfg_condition,
                            reason,
                        });
                    } else {
                        return Err(syn::Error::new(
                            ident.span(),
                            format!("unknown argument `{ident}`, expected one of `tarantool`, `linkme`, `section`, `should_panic`, `skip_if`"),
                        ));
                    }

                    if !input.is_empty() {
                        input.parse::<syn::Token![,]>()?;
                    }
                }

                Ok(())
            },
            tokens,
        )?;

        let section = section
            .unwrap_or_else(|| syn::parse_quote! { #tarantool::test::TARANTOOL_MODULE_TESTS });

        let linkme = linkme.unwrap_or_else(|| syn::parse_quote! { #tarantool::linkme });

        Ok(Self {
            tarantool,
            section,
            linkme,
            should_panic,
            skip_if,
        })
    }
}
