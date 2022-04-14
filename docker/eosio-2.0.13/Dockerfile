FROM ubuntu:18.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y wget

# install eosio tools
RUN wget https://github.com/EOSIO/eos/releases/download/v2.0.13/eosio_2.0.13-1-ubuntu-18.04_amd64.deb

RUN apt-get install -y ./eosio_2.0.13-1-ubuntu-18.04_amd64.deb

RUN mkdir -p /root/nodeos
WORKDIR /root/nodeos
COPY config.ini config.ini
COPY contracts contracts

EXPOSE 8888

CMD sleep 9999999999
