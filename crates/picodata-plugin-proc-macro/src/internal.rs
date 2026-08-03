// this code is based on https://github.com/sagebind/stability/blob/241f78729fac623b1f0845784b12f86eb809c459/src/unstable.rs
// changes:
// - removed support for arbitrary feature names and for linking to issues
// - "unstable" renamed to "internal"

use proc_macro::TokenStream;
use quote::{quote, ToTokens};
use syn::{parse_quote, Visibility};

#[derive(Debug, Default)]
pub struct InternalAttribute;

impl InternalAttribute {
    pub fn expand(&self, mut item: impl ItemLike + ToTokens + Clone) -> TokenStream {
        // We only care about public items.
        if item.is_public() {
            let feature_name = "internal";

            let doc_addendum = format!(
                "\n\
                    # Availability\n\
                    \n\
                    **This API is marked as picodata-internal** and is only available when \
                    the `{}` crate feature is enabled. This comes with no stability \
                    guarantees, and could be changed or removed at any time.\
                ",
                feature_name
            );
            item.push_attr(parse_quote! {
                #[doc = #doc_addendum]
            });

            let mut hidden_item = item.clone();
            hidden_item.set_visibility(parse_quote! {
                pub(crate)
            });

            TokenStream::from(quote! {
                #[cfg(feature = #feature_name)]
                #item

                #[cfg(not(feature = #feature_name))]
                #[allow(dead_code)]
                #hidden_item
            })
        } else {
            item.into_token_stream().into()
        }
    }
}

pub trait ItemLike {
    fn push_attr(&mut self, attr: syn::Attribute);

    fn visibility(&self) -> &Visibility;

    fn set_visibility(&mut self, visibility: Visibility);

    fn is_public(&self) -> bool {
        matches!(self.visibility(), Visibility::Public(_))
    }
}

macro_rules! impl_has_visibility {
    ($($ty:ty),+ $(,)?) => {
        $(
            impl ItemLike for $ty {
                fn push_attr(&mut self, attr: syn::Attribute) {
                    self.attrs.push(attr);
                }

                fn visibility(&self) -> &Visibility {
                    &self.vis
                }

                fn set_visibility(&mut self, visibility: Visibility) {
                    self.vis = visibility;
                }
            }
        )*
    };
}

impl_has_visibility!(
    syn::ItemType,
    syn::ItemEnum,
    syn::ItemFn,
    syn::ItemMod,
    syn::ItemTrait,
    syn::ItemConst,
    syn::ItemStatic,
    syn::ItemUse,
);

impl ItemLike for syn::ItemStruct {
    fn push_attr(&mut self, attr: syn::Attribute) {
        self.attrs.push(attr);
    }

    fn visibility(&self) -> &Visibility {
        &self.vis
    }

    fn set_visibility(&mut self, visibility: Visibility) {
        // Also constrain visibility of all fields to be at most the given
        // item visibility.
        self.fields
            .iter_mut()
            .filter(|field| matches!(&field.vis, Visibility::Public(_)))
            .for_each(|field| field.vis = visibility.clone());

        self.vis = visibility;
    }
}
