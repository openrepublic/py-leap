#!/bin/bash

(ls .python-version && mv .python-version .disabled.python-version)

mkdir -p src/build

pushd src/build

cmake -DCMAKE_INSTALL_PREFIX="$(pwd)" ..

make -j4

make install py_eosio

popd

(ls .disabled.python-version && mv .disabled.python-version .python-version)

cp src/build/py-eosio/lib/* py_eosio/__ctypes/
