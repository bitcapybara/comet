#!/bin/bash

mkdir -p ./certs && rm -rf ./certs/*

ROOT_CA_FILE="$(mkcert -CAROOT)"/rootCA.pem

if [ ! -e $ROOT_CA_FILE ]; then
    echo "please exec `mkcert -install` first!"
    exit 1
fi

cp $ROOT_CA_FILE ./certs/ca-cert.pem

# generate cert files for server
mkcert -cert-file ./certs/server-cert.pem -key-file ./certs/server-key.pem localhost 127.0.0.1 ::1

# generate cert files for client
mkcert -client -cert-file ./certs/client-cert.pem -key-file ./certs/client-key.pem localhost 127.0.0.1 ::1