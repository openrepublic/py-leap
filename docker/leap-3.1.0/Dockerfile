FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y wget

# install eosio tools
RUN wget https://github.com/AntelopeIO/leap/releases/download/v3.1.0/leap-3.1.0-ubuntu22.04-x86_64.deb

RUN apt-get install -y ./leap-3.1.0-ubuntu22.04-x86_64.deb

RUN mkdir -p /root/nodeos
WORKDIR /root/nodeos
COPY config.ini config.ini
COPY contracts contracts
COPY genesis genesis

EXPOSE 8888
EXPOSE 9876
EXPOSE 19999
EXPOSE 29999

CMD sleep 9999999999
