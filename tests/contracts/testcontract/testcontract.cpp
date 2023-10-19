#include "testcontract.hpp"


void testcontract::checkasset(const asset& ass, int64_t checkam) {
    check(ass.is_amount_within_range(), "magnitude of asset amount must be less than 2^62" );
    check(ass.symbol.is_valid(),        "invalid symbol name" );
    check(ass.amount == checkam,
          "invalid amount: " + std::to_string(ass.amount) + " " +
          "should be: " + std::to_string(checkam));
}


void testcontract::checkripmd(const std::optional<checksum160>& val,  const string& check_str) {
    check(val.has_value(), "optional should have value");
    auto hex_val = string_to_hex<20>(val->extract_as_byte_array());
    check(hex_val == check_str, hex_val + " != " + check_str);
}
