FROM ubuntu:20.04

ENV DEBIAN_FRONTEND=noninteractive

# https://developers.eos.io/welcome/latest/getting-started/development-environment/introduction

RUN apt-get update && apt-get install -y \
    git \
    wget \
    make \
    curl \
    sudo \
    ruby \
    clang \
    python3 \
    libtool \
    doxygen \
    autoconf \
    automake \
    graphviz \
    python2.7 \
    pkg-config \
    llvm-7-dev \
    zlib1g-dev \
    libicu-dev \
    libbz2-dev \
    libssl-dev \
    libgmp3-dev \
    python3-dev \
    libncurses5 \
    python2.7-dev \
    autotools-dev \
    build-essential \
    libusb-1.0-0-dev \
    libcurl4-gnutls-dev

RUN mkdir /root/work
WORKDIR /root/work

RUN wget https://github.com/Kitware/CMake/releases/download/v3.22.0/cmake-3.22.0.tar.gz
RUN tar xvf cmake-3.22.0.tar.gz
WORKDIR /root/work/cmake-3.22.0
RUN ./bootstrap
RUN make -j $(nproc)
RUN make install

WORKDIR /root/work
RUN wget https://boostorg.jfrog.io/artifactory/main/release/1.71.0/source/boost_1_71_0.tar.gz
RUN tar xvf boost_1_71_0.tar.gz
WORKDIR /root/work/boost_1_71_0
RUN ./bootstrap.sh
RUN ./b2 link=shared install

WORKDIR /root
RUN git clone git://github.com/pyenv/pyenv.git .pyenv
ENV HOME  /root
ENV PYENV_ROOT $HOME/.pyenv
ENV PATH $PYENV_ROOT/shims:$PYENV_ROOT/bin:$PATH

RUN pyenv install 3.10.0
RUN pyenv global 3.10.0

WORKDIR /root/work/bindings
COPY libs libs
COPY src/CMakeLists.txt CMakeLists.txt
COPY src/python_bindings.cpp python_bindings.cpp

RUN cmake -DCMAKE_INSTALL_PREFIX="$(pwd)/build" .
RUN make -j 4 
RUN make install py_eosio
