mod internal;

extern crate proc_macro;
use proc_macro::TokenStream;

use quote::quote;
use syn::{parse_macro_input, Item, ReturnType, Signature};

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

/// Mark an API as picodata-internal.
///
/// You can apply this attribute to an item in your public API that you would
/// like to expose to picodata, but do not want it exposed to plugin SDK users. This is
/// useful when you want to share some piece of code between the plugin SDK and picodata,
/// but do not want to commit any stability guarantees for.
///
/// This attribute does the following things to annotated items:
///
/// - Changes the visibility of the item from `pub` to `pub(crate)`, unless the
///   `internal` crate feature is enabled. This ensures that internal code within
///   the crate can always use the item, but downstream consumers cannot access
///   it unless they opt-in to the `interal` API¸which only picodata should do.
///   - Visibility of certain child items of the annotated item will also be
///     changed to match the new item visibility, such as struct fields. Children
///     that are not public will not be affected.
///   - Child items of annotated modules will *not* have their visibility changed,
///     as it might be desirable to be able to re-export them even if the module
///     visibility is restricted. You should apply the attribute to each item
///     within the module with the same feature name if you want to restrict the
///     module's contents itself and not just the module namespace.
/// - Appends an "Availability" section to the item's documentation that notes
///   that the item is picodata-internal, and indicates the name of the crate feature to
///   enable it.
///
/// Applying this attribute to non-`pub` items is pointless and does nothing.
///
/// # Examples
///
/// We can apply the attribute to a public function like so:
///
/// ```
/// /// This function does something really risky!
/// ///
/// /// Don't use it yet!
/// #[picodata_plugin_proc_macro::internal]
/// pub fn risky_function() {
///     unimplemented!()
/// }
/// ```
///
/// This will essentially be expanded to the following:
///
/// ```
/// /// This function does something really risky!
/// ///
/// /// Don't use it yet!
/// ///
/// /// # Availability
/// ///
/// /// **This API is marked as picodata-internal** and is only available when the `internal` crate feature is enabled.
/// /// This comes with no stability guarantees, and could be changed or removed at any time.
/// #[cfg(feature = "internal")]
/// pub fn risky_function() {
///     unimplemented!()
/// }
///
/// /// This function does something really risky!
/// ///
/// /// Don't use it yet!
/// #[cfg(not(feature = "internal"))]
/// pub(crate) fn risky_function() {
///     unimplemented!()
/// }
/// ```
#[proc_macro_attribute]
pub fn internal(_args: TokenStream, input: TokenStream) -> TokenStream {
    let attribute = internal::InternalAttribute;
    match parse_macro_input!(input as Item) {
        Item::Type(item_type) => attribute.expand(item_type),
        Item::Enum(item_enum) => attribute.expand(item_enum),
        Item::Struct(item_struct) => attribute.expand(item_struct),
        Item::Fn(item_fn) => attribute.expand(item_fn),
        Item::Mod(item_mod) => attribute.expand(item_mod),
        Item::Trait(item_trait) => attribute.expand(item_trait),
        Item::Const(item_const) => attribute.expand(item_const),
        Item::Static(item_static) => attribute.expand(item_static),
        Item::Use(item_use) => attribute.expand(item_use),
        _ => panic!("unsupported item type"),
    }
}
