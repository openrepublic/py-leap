#include <eosio/eosio.hpp>
#include <eosio/singleton.hpp>

using namespace eosio;

class [[eosio::contract]] testcontract : public contract {
    public:
        using contract::contract;

        testcontract(name receiver, name code, datastream<const char*> ds) :
            contract(receiver, code, ds),
            cfg(receiver, receiver.value)
            {}

        [[eosio::action]]
        void testmultisig(name a, name b); 

        [[eosio::action]]
        void initcfg(uint64_t val);

        [[eosio::action]]
        void addsecidx(uint64_t other);

    private:

        struct [[eosio::table]] config {
            uint128_t value;
            uint64_t primary_key() const { return (uint64_t)value; }
        };

        using conf_type = eosio::singleton<"config"_n, config>;
        conf_type cfg;

        struct [[eosio::table]] sec_index_test_t {
            uint64_t id;
            uint64_t other;

            uint64_t primary_key() const {
                return id;
            }

            uint128_t secondary_key() const {
                return ((uint128_t)id << 64) | (uint128_t)other;
            }
        };

        typedef eosio::multi_index<"sectest"_n, sec_index_test_t,
            indexed_by<"dword"_n, const_mem_fun<sec_index_test_t, uint128_t, &sec_index_test_t::secondary_key>>
        > secidx_table;

};
