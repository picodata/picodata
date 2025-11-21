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

//! Implementation of the SCRAM authentication algorithm.

use super::{
    messages::{
        ClientFinalMessage, ClientFirstMessage, OwnedServerFirstMessage, SCRAM_RAW_NONCE_LEN,
    },
    secret::ServerSecret,
    signature::SignatureBuilder,
    ScramKey, TlsServerEndpoint,
};
use crate::sasl::{self, ChannelBinding, Error as SaslError};
use std::convert::Infallible;

#[derive(Debug)]
enum ChannelBindingMode {
    /// The only channel binding mode we currently support.
    TlsServerEndPoint,
}

impl std::fmt::Display for ChannelBindingMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "tls-server-end-point")
    }
}

impl std::str::FromStr for ChannelBindingMode {
    type Err = sasl::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "tls-server-end-point" => Ok(Self::TlsServerEndPoint),
            _ => Err(sasl::Error::ChannelBindingBadMethod(s.into())),
        }
    }
}

struct SaslSentInner {
    cbind_flag: ChannelBinding<ChannelBindingMode>,
    client_first_message_bare: String,
    server_first_message: OwnedServerFirstMessage,
}

struct SaslInitial {
    nonce: fn() -> [u8; SCRAM_RAW_NONCE_LEN],
}

enum ExchangeState {
    /// Waiting for [`ClientFirstMessage`].
    Initial(SaslInitial),
    /// Waiting for [`ClientFinalMessage`].
    SaltSent(SaslSentInner),
}

/// Server's side of SCRAM auth algorithm.
pub struct Exchange<'a> {
    state: ExchangeState,
    secret: &'a ServerSecret,
    tls_server_endpoint: TlsServerEndpoint,
}

impl<'a> Exchange<'a> {
    pub fn new(
        secret: &'a ServerSecret,
        nonce: fn() -> [u8; SCRAM_RAW_NONCE_LEN],
        tls_server_endpoint: TlsServerEndpoint,
    ) -> Self {
        Self {
            state: ExchangeState::Initial(SaslInitial { nonce }),
            secret,
            tls_server_endpoint,
        }
    }
}

impl sasl::Mechanism for Exchange<'_> {
    type Output = ScramKey;

    fn exchange(mut self, input: &str) -> sasl::Result<sasl::Step<Self, Self::Output>> {
        use sasl::Step;
        use ExchangeState;
        match &self.state {
            ExchangeState::Initial(init) => {
                match init.transition(self.secret, &self.tls_server_endpoint, input)? {
                    Step::Continue(sent, msg) => {
                        self.state = ExchangeState::SaltSent(sent);
                        Ok(Step::Continue(self, msg))
                    }
                    Step::Failure => Ok(Step::Failure),
                }
            }
            ExchangeState::SaltSent(sent) => {
                match sent.transition(self.secret, &self.tls_server_endpoint, input)? {
                    Step::Success(keys, msg) => Ok(Step::Success(keys, msg)),
                    Step::Failure => Ok(Step::Failure),
                }
            }
        }
    }
}

impl SaslInitial {
    fn transition(
        &self,
        secret: &ServerSecret,
        tls_server_endpoint: &TlsServerEndpoint,
        input: &str,
    ) -> sasl::Result<sasl::Step<SaslSentInner, Infallible>> {
        let client_first_message = ClientFirstMessage::parse(input)
            .ok_or(SaslError::BadClientMessage("invalid client-first-message"))?;

        // If the flag is set to "y" and the server supports channel
        // binding, the server MUST fail authentication
        if client_first_message.cbind_flag == ChannelBinding::NotSupportedServer
            && tls_server_endpoint.supported()
        {
            return Err(SaslError::ChannelBindingFailed("SCRAM-PLUS not used"));
        }

        let server_first_message = client_first_message.build_server_first_message(
            &(self.nonce)(),
            &secret.salt_base64,
            secret.iterations,
        );
        let msg = server_first_message.as_str().to_owned();

        let next = SaslSentInner {
            cbind_flag: client_first_message.cbind_flag.and_then(str::parse)?,
            client_first_message_bare: client_first_message.bare.to_owned(),
            server_first_message,
        };

        Ok(sasl::Step::Continue(next, msg))
    }
}

impl SaslSentInner {
    fn transition(
        &self,
        secret: &ServerSecret,
        tls_server_endpoint: &TlsServerEndpoint,
        input: &str,
    ) -> sasl::Result<sasl::Step<Infallible, ScramKey>> {
        let Self {
            cbind_flag,
            client_first_message_bare,
            server_first_message,
        } = self;

        let client_final_message = ClientFinalMessage::parse(input)
            .ok_or(SaslError::BadClientMessage("invalid client-final-message"))?;

        let channel_binding = cbind_flag.encode(|_| match tls_server_endpoint {
            TlsServerEndpoint::Sha256(x) => Ok(x),
            TlsServerEndpoint::Undefined => Err(SaslError::MissingBinding),
        })?;

        // This might've been caused by a MITM attack
        if client_final_message.channel_binding != channel_binding {
            return Err(SaslError::ChannelBindingFailed(
                "insecure connection: secure channel data mismatch",
            ));
        }

        if client_final_message.nonce != server_first_message.nonce() {
            return Err(SaslError::BadClientMessage("combined nonce doesn't match"));
        }

        let signature_builder = SignatureBuilder {
            client_first_message_bare,
            server_first_message: server_first_message.as_str(),
            client_final_message_without_proof: client_final_message.without_proof,
        };

        let client_key = signature_builder
            .build(&secret.stored_key)
            .derive_client_key(&client_final_message.proof);

        // Auth fails either if keys don't match or it's pre-determined to fail.
        if secret.is_client_key_invalid(&client_key).into() {
            return Ok(sasl::Step::Failure);
        }

        let msg =
            client_final_message.build_server_final_message(signature_builder, &secret.server_key);

        Ok(sasl::Step::Success(client_key, msg))
    }
}
