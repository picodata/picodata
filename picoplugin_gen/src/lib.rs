extern crate proc_macro;
use proc_macro::TokenStream;

use quote::quote;
use syn::{parse_macro_input, ReturnType, Signature};

#[proc_macro_attribute]
pub fn proc_service_registrar(_attr: TokenStream, input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as syn::Item);

    let syn::ItemFn { sig, block, .. } = match input {
        syn::Item::Fn(f) => f,
        _ => panic!("only `fn` items allowed"),
    };

    let (ident, inputs, _generics) = match sig {
        Signature {
            asyncness: Some(_), ..
        } => {
            panic!("async factory are not supported yet")
        }
        Signature {
            variadic: Some(_), ..
        } => {
            panic!("variadic factory are not supported yet")
        }
        Signature {
            ident,
            output,
            inputs,
            generics,
            ..
        } => {
            if !matches!(output, ReturnType::Default) {
                panic!("function must have no output")
            }

            if inputs.len() != 1 {
                panic!("there is only 1 input argument supported")
            }

            (ident, inputs, generics)
        }
    };

    let inner_fn_name =
        syn::Ident::new(&("__inner_".to_string() + &ident.to_string()), ident.span());

    // embeds function for check input argument
    quote! {
        #[linkme::distributed_slice(picoplugin::interface::REGISTRARS)]
        extern "C" fn #ident(registry: &mut picoplugin::interface::ServiceRegistry) {
            fn #inner_fn_name (#inputs) {
                   #block
            }

            #inner_fn_name(registry)
        }
    }
    .into()
}
