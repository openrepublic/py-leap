FROM ubuntu:20.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y wget

# install eosio tools
RUN wget https://github.com/EOSIO/eos/releases/download/v2.1.0/eosio_2.1.0-1-ubuntu-20.04_amd64.deb

RUN apt-get install -y ./eosio_2.1.0-1-ubuntu-20.04_amd64.deb

RUN mkdir -p /root/nodeos
WORKDIR /root/nodeos
COPY config.ini config.ini
COPY contracts contracts
COPY genesis genesis

EXPOSE 8888
EXPOSE 9876

CMD sleep 9999999999
