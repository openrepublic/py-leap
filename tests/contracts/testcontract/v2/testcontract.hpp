#include <eosio/eosio.hpp>
#include <eosio/asset.hpp>
#include <eosio/singleton.hpp>

#define VERSION "v2"

using std::string;
using std::to_string;

using namespace eosio;

template<std::size_t N>
std::string string_to_hex(const std::array<uint8_t, N>& data) {
    static const char* const lut = "0123456789abcdef";
    std::string output;

    for (const uint8_t c : data) {
        output.push_back(lut[c >> 4]);
        output.push_back(lut[c & 15]);
    }

    return output;
}

class [[eosio::contract]] testcontract : public contract {
    public:
        using contract::contract;

        [[eosio::action]]
        void checkasset(const asset& ass, int64_t checkam);

        [[eosio::action]]
        void checkexasset(const extended_asset& eass, name contract, int64_t checkam);

        [[eosio::action]]
        void checkripmd(const std::optional<checksum160>& val, const string& check_str);

        [[eosio::action]]
        void testversion(const name test, const int64_t val);
};
