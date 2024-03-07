use quote::quote;

#[allow(clippy::single_match)]
#[proc_macro_derive(Introspection)]
pub fn derive_introspection(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = syn::parse_macro_input!(input as syn::DeriveInput);
    let name = &input.ident;

    let mut field_names = vec![];
    match &input.data {
        syn::Data::Struct(ds) => match &ds.fields {
            syn::Fields::Named(fs) => {
                for field in &fs.named {
                    let field_name = field.ident.as_ref().unwrap();
                    field_names.push(field_name.to_string());
                }
            }
            _ => {}
        },
        _ => {}
    }

    quote! {
        impl #name {
            pub const FIELD_NAMES: &'static [&'static str] = &[
                #( #field_names, )*
            ];
        }
    }
    .into()
}
