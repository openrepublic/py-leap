#!/bin/sh

mkdir -p py_eosio/bin

docker build \
    --tag bindings-builder \
    -f docker/bindings/Dockerfile .

docker run \
    -it \
    --rm \
    --mount type=bind,src="$(pwd)",target=/root/outside \
    bindings-builder \
    bash -c 'cp build/py-eosio/lib/* /root/outside/py_eosio/bin'

chown guille -R py_eosio/
