#include "testcontract.hpp"

void testcontract::testmultisig(name a, name b) {
    require_auth(a);
    require_auth(b);
    auto entry = cfg.get_or_create(get_self(), config{0});
    entry.value = (uint128_t)a.value + (uint128_t)b.value; 
    cfg.set(entry, get_self());
}

void testcontract::initcfg(uint64_t val) {
    auto entry = cfg.get_or_create(get_self(), config{0});
    entry.value = val; 
    cfg.set(entry, get_self());
}

void testcontract::addsecidx(uint64_t other) {
    secidx_table test_table(get_self(), get_self().value);
    test_table.emplace(get_self(), [&](auto& row) {
        row.id = test_table.available_primary_key();
        row.other = other;
    });
}
