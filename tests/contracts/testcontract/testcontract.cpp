#include "testcontract.hpp"


void testcontract::checkasset(const asset& ass, int64_t checkam) {
    check(ass.is_amount_within_range(), "magnitude of asset amount must be less than 2^62" );
    check(ass.symbol.is_valid(),        "invalid symbol name" );
    check(ass.amount == checkam,
          "invalid amount: " + std::to_string(ass.amount) + " " +
          "should be: " + std::to_string(checkam));
}
