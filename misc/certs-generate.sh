#!/bin/bash

mkdir -p ./certs

# generate cert files for server
mkcert -cert-file ./certs/server-cert.pem -key-file ./certs/server-key.pem localhost 127.0.0.1 ::1

# generate cert files for client
mkcert -client -cert-file ./certs/client-cert.pem -key-file ./certs/client-key.pem localhost 127.0.0.1 ::1