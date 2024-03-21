use quote::quote;

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

#[allow(clippy::single_match)]
#[proc_macro_derive(Introspection, attributes(introspection))]
pub fn derive_introspection(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = syn::parse_macro_input!(input as syn::DeriveInput);
    let name = &input.ident;

    let args = unwrap_or_compile_error!(Args::from_attributes(input.attrs));

    let mut context = Context {
        args,
        fields: vec![],
    };
    match input.data {
        syn::Data::Struct(ds) => match ds.fields {
            syn::Fields::Named(fs) => {
                for mut field in fs.named {
                    let attrs = std::mem::take(&mut field.attrs);
                    let attrs = unwrap_or_compile_error!(FieldAttrs::from_attributes(attrs));
                    if attrs.ignore {
                        continue;
                    }

                    context.fields.push(FieldInfo {
                        name: field_name(&field),
                        ident: field
                            .ident
                            .clone()
                            .expect("Fields::Named has fields with names"),
                        attrs,
                        field,
                    });
                }
            }
            _ => {}
        },
        _ => {}
    }

    let body_for_field_infos = generate_body_for_field_infos(&context);
    let body_for_set_field_from_yaml = generate_body_for_set_field_from_yaml(&context);
    let body_for_get_field_as_rmpv = generate_body_for_get_field_as_rmpv(&context);

    let crate_ = &context.args.crate_;
    quote! {
        #[automatically_derived]
        impl #crate_::introspection::Introspection for #name {
            const FIELD_INFOS: &'static [#crate_::introspection::FieldInfo] = &[
                #body_for_field_infos
            ];

            fn set_field_from_yaml(&mut self, path: &str, yaml: &str) -> ::std::result::Result<(), #crate_::introspection::IntrospectionError> {
                use #crate_::introspection::IntrospectionError;
                #body_for_set_field_from_yaml
            }

            fn get_field_as_rmpv(&self, path: &str) -> ::std::result::Result<#crate_::introspection::RmpvValue, #crate_::introspection::IntrospectionError> {
                use #crate_::introspection::IntrospectionError;
                #body_for_get_field_as_rmpv
            }
        }
    }
    .into()
}

fn generate_body_for_field_infos(context: &Context) -> proc_macro2::TokenStream {
    let crate_ = &context.args.crate_;

    let mut code = quote! {};

    for field in &context.fields {
        let name = &field.name;
        #[allow(non_snake_case)]
        let Type = &field.field.ty;

        if !field.attrs.nested {
            code.extend(quote! {
                #crate_::introspection::FieldInfo {
                    name: #name,
                    nested_fields: &[],
                },
            });
        } else {
            code.extend(quote! {
                #crate_::introspection::FieldInfo {
                    name: #name,
                    nested_fields: #Type::FIELD_INFOS,
                },
            });
        }
    }

    code
}

fn generate_body_for_set_field_from_yaml(context: &Context) -> proc_macro2::TokenStream {
    let mut set_non_nestable = quote! {};
    let mut set_nestable = quote! {};
    let mut error_if_nestable = quote! {};
    let mut non_nestable_names = vec![];
    for field in &context.fields {
        let name = &field.name;
        let ident = &field.ident;
        #[allow(non_snake_case)]
        let Type = &field.field.ty;

        if !field.attrs.nested {
            non_nestable_names.push(name);

            // Handle assigning to a non-nestable field
            set_non_nestable.extend(quote! {
                #name => {
                    match serde_yaml::from_str(yaml) {
                        Ok(v) => {
                            self.#ident = v;
                            return Ok(());
                        }
                        Err(error) => return Err(IntrospectionError::FromSerdeYaml { field: path.into(), error }),
                    }
                }
            });
        } else {
            // Handle assigning to a nested sub-field
            set_nestable.extend(quote! {
                #name => {
                    return self.#ident.set_field_from_yaml(tail, yaml)
                        .map_err(|e| e.with_prepended_prefix(head));
                }
            });

            // Handle if trying to assign to field marked with `#[introspection(nested)]`
            // This is not currently supported, all of it's subfields must be assigned individually
            error_if_nestable.extend(quote! {
                #name => return Err(IntrospectionError::AssignToNested {
                    field: path.into(),
                    example: if let Some(field) = #Type::FIELD_INFOS.get(0) {
                        field.name
                    } else {
                        "<actually there's no fields in this struct :(>"
                    },
                }),
            })
        }
    }

    // Handle if a nested path is specified for non-nestable field
    let mut error_if_non_nestable = quote! {};
    if !non_nestable_names.is_empty() {
        error_if_non_nestable.extend(quote! {
            #( #non_nestable_names )|* => {
                return Err(IntrospectionError::NotNestable { field: head.into() })
            }
        });
    }

    // Actual generated body:
    quote! {
        match path.split_once('.') {
            Some((head, tail)) => {
                let head = head.trim();
                if head.is_empty() {
                    return Err(IntrospectionError::InvalidPath {
                        expected: "expected a field name before",
                        path: format!(".{tail}"),
                    })
                }
                let tail = tail.trim();
                if !tail.chars().next().map_or(false, char::is_alphabetic) {
                    return Err(IntrospectionError::InvalidPath {
                        expected: "expected a field name after",
                        path: format!("{head}."),
                    })
                }
                match head {
                    #error_if_non_nestable
                    #set_nestable
                    _ => {
                        return Err(IntrospectionError::NoSuchField {
                            parent: "".into(),
                            field: head.into(),
                            expected: Self::FIELD_INFOS,
                        });
                    }
                }
            }
            None => {
                match path {
                    #set_non_nestable
                    #error_if_nestable
                    _ => {
                        return Err(IntrospectionError::NoSuchField {
                            parent: "".into(),
                            field: path.into(),
                            expected: Self::FIELD_INFOS,
                        });
                    }
                }
            }
        }
    }
}

fn generate_body_for_get_field_as_rmpv(context: &Context) -> proc_macro2::TokenStream {
    let crate_ = &context.args.crate_;

    let mut get_non_nestable = quote! {};
    let mut get_whole_nestable = quote! {};
    let mut get_nested_subfield = quote! {};
    let mut non_nestable_names = vec![];
    for field in &context.fields {
        let name = &field.name;
        let ident = &field.ident;
        #[allow(non_snake_case)]
        let Type = &field.field.ty;

        if !field.attrs.nested {
            non_nestable_names.push(name);

            // Handle getting a non-nestable field
            get_non_nestable.extend(quote! {
                #name => {
                    match #crate_::introspection::to_rmpv_value(&self.#ident) {
                        Err(e) => {
                            return Err(IntrospectionError::ToRmpvValue { field: path.into(), details: e });
                        }
                        Ok(value) => return Ok(value),
                    }
                }
            });
        } else {
            // Handle getting a field marked with `#[introspection(nested)]`.
            get_whole_nestable.extend(quote! {
                #name => {
                    use #crate_::introspection::RmpvValue;
                    let field_infos = #Type::FIELD_INFOS;
                    let mut fields = Vec::with_capacity(field_infos.len());
                    for sub_field in field_infos {
                        let key = RmpvValue::from(sub_field.name);
                        let value = self.#ident.get_field_as_rmpv(sub_field.name)
                            .map_err(|e| e.with_prepended_prefix(#name))?;
                        fields.push((key, value));
                    }
                    return Ok(RmpvValue::Map(fields));
                }
            });

            // Handle getting a nested field
            get_nested_subfield.extend(quote! {
                #name => {
                    return self.#ident.get_field_as_rmpv(tail)
                        .map_err(|e| e.with_prepended_prefix(head));
                }
            });
        }
    }

    // Handle if a nested path is specified for non-nestable field
    let mut error_if_non_nestable = quote! {};
    if !non_nestable_names.is_empty() {
        error_if_non_nestable = quote! {
            #( #non_nestable_names )|* => {
                return Err(IntrospectionError::NotNestable { field: head.into() })
            }
        };
    }

    // Actual generated body:
    quote! {
        match path.split_once('.') {
            Some((head, tail)) => {
                let head = head.trim();
                if head.is_empty() {
                    return Err(IntrospectionError::InvalidPath {
                        expected: "expected a field name before",
                        path: format!(".{tail}"),
                    })
                }
                let tail = tail.trim();
                if !tail.chars().next().map_or(false, char::is_alphabetic) {
                    return Err(IntrospectionError::InvalidPath {
                        expected: "expected a field name after",
                        path: format!("{head}."),
                    })
                }
                match head {
                    #error_if_non_nestable
                    #get_nested_subfield
                    _ => {
                        return Err(IntrospectionError::NoSuchField {
                            parent: "".into(),
                            field: head.into(),
                            expected: Self::FIELD_INFOS,
                        });
                    }
                }
            }
            None => {
                match path {
                    #get_non_nestable
                    #get_whole_nestable
                    _ => {
                        return Err(IntrospectionError::NoSuchField {
                            parent: "".into(),
                            field: path.into(),
                            expected: Self::FIELD_INFOS,
                        });
                    }
                }
            }
        }
    }
}

struct Context {
    fields: Vec<FieldInfo>,
    args: Args,
}

struct FieldInfo {
    name: String,
    ident: syn::Ident,
    attrs: FieldAttrs,
    #[allow(unused)]
    field: syn::Field,
}

struct Args {
    crate_: syn::Path,
}

impl Args {
    fn from_attributes(attrs: Vec<syn::Attribute>) -> Result<Self, syn::Error> {
        let mut result = Self {
            crate_: syn::parse2(quote!(crate)).unwrap(),
        };

        for attr in attrs {
            if !attr.path.is_ident("introspection") {
                continue;
            }

            let meta: PathKeyValue = attr.parse_args()?;
            if meta.key.is_ident("crate") {
                result.crate_ = meta.value;
            }
        }

        Ok(result)
    }
}

struct FieldAttrs {
    ignore: bool,
    nested: bool,
}

impl FieldAttrs {
    fn from_attributes(attrs: Vec<syn::Attribute>) -> Result<Self, syn::Error> {
        let mut result = Self {
            ignore: false,
            nested: false,
        };

        for attr in attrs {
            if !attr.path.is_ident("introspection") {
                continue;
            }

            attr.parse_args_with(|input: syn::parse::ParseStream| {
                // `input` is a stream of those tokens right there
                // `#[introspection(foo, bar, ...)]`
                //                  ^^^^^^^^^^^^^
                while !input.is_empty() {
                    let ident = input.parse::<syn::Ident>()?;
                    if ident == "ignore" {
                        result.ignore = true;
                    } else if ident == "nested" {
                        result.nested = true;
                    } else {
                        return Err(syn::Error::new(
                            ident.span(),
                            format!("unknown attribute argument `{ident}`, expected one of `ignore`, `nested`"),
                        ));
                    }

                    if !input.is_empty() {
                        input.parse::<syn::Token![,]>()?;
                    }
                }

                Ok(())
            })?;
        }

        Ok(result)
    }
}

fn field_name(field: &syn::Field) -> String {
    // TODO: consider using `quote::format_ident!` instead
    let mut name = field.ident.as_ref().unwrap().to_string();
    if name.starts_with("r#") {
        // Remove 2 leading characters
        name.remove(0);
        name.remove(0);
    }
    name
}

#[derive(Debug)]
struct PathKeyValue {
    key: syn::Path,
    #[allow(unused)]
    eq_token: syn::Token![=],
    value: syn::Path,
}

impl syn::parse::Parse for PathKeyValue {
    fn parse(input: syn::parse::ParseStream) -> Result<Self, syn::Error> {
        Ok(Self {
            key: input.parse()?,
            eq_token: input.parse()?,
            value: input.parse()?,
        })
    }
}
