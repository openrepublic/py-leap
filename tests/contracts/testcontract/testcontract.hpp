#include <eosio/eosio.hpp>
#include <eosio/asset.hpp>
#include <eosio/singleton.hpp>

using namespace eosio;

class [[eosio::contract]] testcontract : public contract {
    public:
        using contract::contract;

        [[eosio::action]]
        void checkasset(const asset& ass, int64_t checkam);
};
