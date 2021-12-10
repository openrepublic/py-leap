#!/bin/sh

docker build \
    --tag bindings-builder \
    -f docker/bindings/Dockerfile docker/bindings

docker run \
    -it \
    --rm \
    --mount type=bind,src="$(pwd)",target=/root/work/bindings \
    bindings-builder \
    bash -c ". docker/bindings/perform_build.sh"
