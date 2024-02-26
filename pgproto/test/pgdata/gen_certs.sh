
#!/bin/bash

# The tls guide [1] from postgres works fine with psql, but not with pg8000.
# pg8000 is a little more strict about certificates and replies with errors
# to the secrtificates from the guide.
# Here's how you can generate tls certificates that wont lead to errors from pg8000.
# The following is essentially a combination of [1] and [2].
#
# [1] https://www.postgresql.org/docs/current/ssl-tcp.html
# [2] https://stackoverflow.com/questions/52855924/problems-using-paho-mqtt-client-with-python-3-7

openssl req -new -nodes -text -out server.csr \
   -keyout server.key \
   -extfile <(printf "subjectAltName=IP:127.0.0.1")

openssl req -new -nodes -text -out root.csr \
	-keyout root.key -subj "/CN=root.yourdomain.com"
chmod og-rwx root.key

openssl req -new -nodes -text -out root.csr	\
	-keyout root.key -subj "/CN=root.yourdomain.com"
chmod og-rwx root.key

openssl x509 -req -in root.csr -text -days 36500 \
	-extfile /etc/ssl/openssl.cnf -extensions v3_ca \
	-signkey root.key -out root.crt

openssl req -new -nodes -text -out server.csr \
	-keyout server.key -subj "/CN=dbhost.yourdomain.com"
chmod og-rwx server.key

openssl x509 -req -in server.csr \
	-extfile <(printf "subjectAltName=IP:127.0.0.1") \
	-CA root.crt \
	-CAkey root.key \
	-CAcreateserial \
	-out server.crt \
	-days 36500
