extern crate proc_macro;
use proc_macro::TokenStream;

use quote::quote;
use syn::{parse_macro_input, ReturnType, Signature};

fn create_plugin_proc_macro_fn(
    input: TokenStream,
    export_name: &str,
    parameter_type: &str,
) -> TokenStream {
    let parameter_type: syn::Type =
        syn::parse_str(parameter_type).expect("failed to parse ServiceRegistry type");
    let input = parse_macro_input!(input as syn::Item);

    let syn::ItemFn { sig, block, .. } = match input {
        syn::Item::Fn(f) => f,
        _ => panic!("only `fn` items allowed"),
    };

    let (ident, inputs) = match sig {
        Signature {
            asyncness: Some(_), ..
        } => {
            panic!("async factories are not yet supported")
        }
        Signature {
            variadic: Some(_), ..
        } => {
            panic!("variadic factories are not yet supported")
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

            if !generics.params.is_empty() {
                panic!("generic factories are not yet supported")
            }

            if inputs.len() != 1 {
                panic!("only one input parameter is supported")
            }

            (ident, inputs)
        }
    };

    let inner_fn_name =
        syn::Ident::new(&("__inner_".to_string() + &ident.to_string()), ident.span());

    quote! {
        #[export_name = #export_name]
        pub extern "C" fn #ident(registry: #parameter_type) {
            #[inline(always)]
            fn #inner_fn_name (#inputs) {
                picodata_plugin::internal::set_panic_hook();

                #block
            }

            #inner_fn_name(registry)
        }
    }
    .into()
}

#[proc_macro_attribute]
pub fn proc_service_registrar(_attr: TokenStream, input: TokenStream) -> TokenStream {
    create_plugin_proc_macro_fn(
        input,
        "pico_service_registrar",
        "&mut picodata_plugin::plugin::interface::ServiceRegistry",
    )
}

/// A procedural macro that lets you set up the migration validator. Right now, you
/// can validate only the migration context, but using the `set_context_validator` for
/// the whole context validation, and `set_context_parameter_validator` for per-parameter
/// validation. For more information, check out these functions' doc comments.
///
/// Example usage:
/// ```ignore
/// #[migration_validator]
/// pub fn migration_validator(mv: &mut picodata_plugin::plugin::interface::MigrationValidator) {
///     mv.set_context_validator(|ctx| {
///         if ctx.len() >= 3 {
///             Err("this context is too long, man".into())
///         } else {
///             Ok(())
///         }
///     });
///     mv.set_context_parameter_validator(|k, v| match k.as_str() {
///         "always_ok_parameter" => Ok(()),
///         "always_bad_parameter" => Err("don't use this parameter, please".into()),
///         "short_parameter" => {
///             if v.len() > 15 {
///                 Err("this parameter can't be that long!".into())
///             } else {
///                 Ok(())
///             }
///         }
///         _ => Ok(()),
///     });
/// }
/// ```
#[proc_macro_attribute]
pub fn proc_migration_validator(_attr: TokenStream, input: TokenStream) -> TokenStream {
    create_plugin_proc_macro_fn(
        input,
        "pico_migration_validator",
        "&mut picodata_plugin::plugin::interface::MigrationValidator",
    )
}
