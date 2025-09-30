/*
 * Original work Copyright (c) 2025 Neon, Inc
 * Modified work Copyright (c) 2025 Picodata
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//! Definition and parser for channel binding flag (a part of the `GS2` header).

use base64::prelude::BASE64_STANDARD;
use base64::Engine as _;

/// Channel binding flag (possibly with params).
#[derive(Debug, PartialEq, Eq)]
pub enum ChannelBinding<T> {
    /// Client doesn't support channel binding.
    NotSupportedClient,
    /// Client thinks server doesn't support channel binding.
    NotSupportedServer,
    /// Client wants to use this type of channel binding.
    Required(T),
}

impl<T> ChannelBinding<T> {
    pub fn and_then<R, E>(self, f: impl FnOnce(T) -> Result<R, E>) -> Result<ChannelBinding<R>, E> {
        Ok(match self {
            Self::NotSupportedClient => ChannelBinding::NotSupportedClient,
            Self::NotSupportedServer => ChannelBinding::NotSupportedServer,
            Self::Required(x) => ChannelBinding::Required(f(x)?),
        })
    }
}

impl<'a> ChannelBinding<&'a str> {
    // NB: FromStr doesn't work with lifetimes
    pub fn parse(input: &'a str) -> Option<Self> {
        Some(match input {
            "n" => Self::NotSupportedClient,
            "y" => Self::NotSupportedServer,
            other => Self::Required(other.strip_prefix("p=")?),
        })
    }
}

impl<T: std::fmt::Display> ChannelBinding<T> {
    /// Encode channel binding data as base64 for subsequent checks.
    pub fn encode<'a, E>(
        &self,
        get_cbind_data: impl FnOnce(&T) -> Result<&'a [u8], E>,
    ) -> Result<std::borrow::Cow<'static, str>, E> {
        Ok(match self {
            Self::NotSupportedClient => {
                // base64::encode("n,,")
                "biws".into()
            }
            Self::NotSupportedServer => {
                // base64::encode("y,,")
                "eSws".into()
            }
            Self::Required(mode) => {
                let mut cbind_input = format!("p={mode},,",).into_bytes();
                cbind_input.extend_from_slice(get_cbind_data(mode)?);
                BASE64_STANDARD.encode(&cbind_input).into()
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn channel_binding_encode() -> anyhow::Result<()> {
        use ChannelBinding::*;

        let cases = [
            (NotSupportedClient, BASE64_STANDARD.encode("n,,")),
            (NotSupportedServer, BASE64_STANDARD.encode("y,,")),
            (Required("foo"), BASE64_STANDARD.encode("p=foo,,bar")),
        ];

        for (cb, input) in cases {
            assert_eq!(cb.encode(|_| anyhow::Ok(b"bar"))?, input);
        }

        Ok(())
    }
}
