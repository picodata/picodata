#!/usr/bin/env bash

set -eux

openssl genrsa -out root-ca.key 4096
openssl req -x509 -new -nodes -key root-ca.key -sha256 -days 36500 -out root-ca.crt -subj "/CN=Root CA"

cat > intermediate-ca.cnf << EOF
[v3_intermediate_ca]
subjectKeyIdentifier = hash
authorityKeyIdentifier = keyid:always,issuer
basicConstraints = critical, CA:true, pathlen:0
keyUsage = critical, digitalSignature, cRLSign, keyCertSign
EOF
openssl genrsa -out intermediate-ca.key 4096
openssl req -new -key intermediate-ca.key -out intermediate-ca.csr -subj "/CN=Intermediate CA"
openssl x509 -req -in intermediate-ca.csr -CA root-ca.crt -CAkey root-ca.key \
    -extensions v3_intermediate_ca -extfile intermediate-ca.cnf \
    -CAcreateserial -out intermediate-ca.crt -days 36500 -sha256

openssl genrsa -out server.key 2048
openssl req -new -key server.key -out server.csr -subj "/CN=example.com"
openssl x509 -req -in server.csr -CA intermediate-ca.crt -CAkey intermediate-ca.key \
    -CAcreateserial -out server.crt -days 36500 -sha256

openssl genrsa -out client.key 2048
openssl req -new -key client.key -out client.csr -subj "/CN=Client"
openssl x509 -req -in client.csr -CA intermediate-ca.crt -CAkey intermediate-ca.key \
    -CAcreateserial -out client.crt -days 36500 -sha256

# Cleanup
rm *.srl *.csr *.cnf

# Test
openssl verify -CAfile root-ca.crt -untrusted intermediate-ca.crt server.crt
openssl verify -CAfile root-ca.crt -untrusted intermediate-ca.crt client.crt

rm -f combined-ca.crt
openssl x509 -in root-ca.crt         >> combined-ca.crt
openssl x509 -in intermediate-ca.crt >> combined-ca.crt

# Test
openssl verify -CAfile combined-ca.crt server.crt
openssl verify -CAfile combined-ca.crt client.crt

rm -f server-fullchain.crt
openssl x509 -in root-ca.crt         >> server-fullchain.crt
openssl x509 -in intermediate-ca.crt >> server-fullchain.crt
openssl x509 -in server.crt          >> server-fullchain.crt

# Test
openssl verify -CAfile server-fullchain.crt server-fullchain.crt

# Certificate authentication for user "pico_service"
openssl req -new -key server.key -out server.csr -subj "/CN=pico_service@example.com"
# Generate server-with-ext.crt with SAN.IP field (for iproto tls)
openssl x509 -req -in server.csr -CA intermediate-ca.crt -CAkey intermediate-ca.key \
    -out server-with-ext.crt -days 36500 -sha256 \
    -extfile server_ext.cnf -extensions server_ext
