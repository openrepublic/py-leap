#include <vector>

#include <eosio/chain/name.hpp>
#include <eosio/chain/types.hpp>
#include <eosio/chain/block.hpp>
#include <eosio/chain/controller.hpp>
#include <eosio/chain/block_header.hpp>
#include <eosio/chain/backing_store.hpp>
#include <eosio/chain/block_timestamp.hpp>
#include <eosio/chain/block_log_config.hpp>
#include <eosio/chain/protocol_feature_activation.hpp>


#include <chainbase/pinnable_mapped_file.hpp>


#include <pybind11/stl.h>
#include <pybind11/pybind11.h>
#include <pybind11/operators.h>


namespace py = pybind11;
namespace chain = eosio::chain;


using std::pair;
using std::vector;

typedef pair<chain::account_name, chain::action_name> contract_action;


PYBIND11_MAKE_OPAQUE(chain::flat_set<chain::account_name>);
PYBIND11_MAKE_OPAQUE(chain::flat_set<contract_action>);


#define FLAT_SET_DEF(V, MODULE, CLASS_NAME) \
py::class_<chain::flat_set<V>>(MODULE, CLASS_NAME) \
    .def("list", [](const chain::flat_set<V>& set) { \
        py::list ret; \
        for (const V& n : set) \
            ret.append(&n); \
        return ret; \
    }) \
    .def("len", &chain::flat_set<V>::size) \
    .def("add", [](chain::flat_set<V>& set, V n) { set.insert(n); }) \
    .def("discard", [](chain::flat_set<V>& set, V n) { set.erase(n); }) \
    .def("clear", [](chain::flat_set<V>& set) { set.clear(); }) \
    .def("__contains__", [](const chain::flat_set<V>& set, V n) { \
        return set.contains(n); \
    });



PYBIND11_MODULE(py_eosio, root_mod) {

    py::module collections = root_mod.def_submodule("collections");

    FLAT_SET_DEF(chain::account_name, collections, "AccountNameFlatSet");
    FLAT_SET_DEF(contract_action, collections, "ActionBlackList");

    py::module chain_mod = root_mod.def_submodule("chain");

    py::module time_mod = chain_mod.def_submodule("time");
    py::module types_mod = chain_mod.def_submodule("types");
    py::module config_mod = chain_mod.def_submodule("config");
    py::module crypto_mod = chain_mod.def_submodule("crypto");
    py::module testing_mod = chain_mod.def_submodule("testing");

    /*
     *
     * chain.time
     *
     */

    py::class_<fc::microseconds>(time_mod, "Microseconds")
        .def(py::init([] (int64_t c) {
            return new fc::microseconds(c);
        }))
        .def(py::self  + py::self)
        .def(py::self  - py::self)
        .def(py::self  > py::self)
        .def(py::self >= py::self)
        .def(py::self  < py::self)
        .def(py::self <= py::self)
        .def(py::self += py::self)
        .def(py::self -= py::self)
        .def("count", &fc::microseconds::count)
        .def("seconds", &fc::microseconds::to_seconds);

    py::class_<fc::time_point>(time_mod, "TimePoint")
        .def(py::init([] (fc::microseconds e) {
            return new fc::time_point(e);
        }))
        .def("now", (fc::time_point (*)()) &fc::time_point::now)
        .def("max", (fc::time_point (*)()) &fc::time_point::maximum)
        .def("min", (fc::time_point (*)()) &fc::time_point::min)
        .def("since_epoch", &fc::time_point::time_since_epoch)
        .def("sec_since_epoch", &fc::time_point::sec_since_epoch)
        .def(py::self  > py::self)
        .def(py::self >= py::self)
        .def(py::self  < py::self)
        .def(py::self <= py::self)
        .def(py::self == py::self)
        .def(py::self != py::self)
        .def(py::self += fc::microseconds())
        .def(py::self -= fc::microseconds())
        .def(py::self  + fc::microseconds())
        .def(py::self  - fc::microseconds());

    /*
     *
     * chain.types
     *
     */

    py::class_<chain::bytes>(types_mod, "Bytes")
        .def(py::init([] (vector<char> buf) {
            return new chain::bytes(buf);
        }));

    py::class_<chain::signature_type>(types_mod, "Signature")
        .def(py::init())
        .def(py::init<const std::string&>())
        .def(py::init<const chain::signature_type&>())
        .def("to_string", [](const chain::signature_type &sig) {
            return sig.to_string();
        })
        .def("which", &chain::signature_type::which)
        .def("variable_size", &chain::signature_type::variable_size);

    py::class_<chain::extensions_type>(types_mod, "Extensions")
        .def(py::init([] (vector<pair<uint16_t,vector<char>>> exts) {
            return new chain::extensions_type(exts);
        }))
        .def("emplace_extension", [](
            chain::extensions_type &exts,
            uint16_t eid,
            vector<char>&& data
        ) {
            chain::emplace_extension(exts, eid, std::move(data));    
        });

    py::class_<chain::path>(types_mod, "Path")
        .def(py::init())
        .def(py::init<std::string>())
        .def(py::self /= py::self)
        .def(py::self  / py::self)
        .def(py::self == py::self)
        .def(py::self != py::self)
        .def(py::self  < py::self)
        .def("replace_extension", &chain::path::replace_extension)
        .def("string", &chain::path::string)
        .def("generic_string", &chain::path::string)
        .def("preferred_string", &chain::path::string)
        .def("parent_path", &chain::path::parent_path)
        .def("filename", &chain::path::filename)
        .def("stem", &chain::path::stem)
        .def("extension", &chain::path::extension)
        .def("is_absolute", &chain::path::is_absolute)
        .def("is_relative", &chain::path::is_relative);

    py::enum_<chain::backing_store_type>(types_mod, "BackingStoreType")
        .value("CHAINBASE", chain::backing_store_type::CHAINBASE)
        .value("ROCKSDB", chain::backing_store_type::ROCKSDB)
        .export_values();

    py::enum_<chain::wasm_interface::vm_type>(types_mod, "VMType")
        .value("eos_vm", chain::wasm_interface::vm_type::eos_vm)
        .value("eos_vm_jit", chain::wasm_interface::vm_type::eos_vm_jit)
        .value("eos_vm_oc", chain::wasm_interface::vm_type::eos_vm_oc)
        .export_values();

    py::enum_<chain::db_read_mode>(types_mod, "DBReadMode")
        .value("SPECULATIVE", chain::db_read_mode::SPECULATIVE)
        .value("HEAD", chain::db_read_mode::HEAD)
        .value("READ_ONLY", chain::db_read_mode::READ_ONLY)
        .value("IRREVERSIBLE", chain::db_read_mode::IRREVERSIBLE)
        .export_values();

    py::enum_<chain::validation_mode>(types_mod, "ValidationMode")
        .value("FULL", chain::validation_mode::FULL)
        .value("LIGHT", chain::validation_mode::LIGHT)
        .export_values();

    py::enum_<chainbase::pinnable_mapped_file::map_mode>(types_mod, "MappedFileMode")
        .value("mapped",  chainbase::pinnable_mapped_file::map_mode::mapped)
        .value("heap", chainbase::pinnable_mapped_file::map_mode::heap)
        .value("locked", chainbase::pinnable_mapped_file::map_mode::locked)
        .export_values();

    /*
     *
     * chain.config
     *
     */

    py::class_<chain::block_log_config>(config_mod, "BlockLogConfig")
        .def(py::init());

    py::class_<chain::eosvmoc::config>(config_mod, "EOSVMOCConfig")
        .def(py::init());

    py::class_<chain::controller::config>(config_mod, "ControllerConfig")
        .def(py::init());

    /*
     *
     * chain.crypto
     *
     */

    py::class_<fc::sha256>(crypto_mod, "SHA256")
        .def(py::init([] (std::string hex) {
            return new fc::sha256(hex);
        }))
        .def("__str__", &fc::sha256::str)
        .def("data", [](const fc::sha256 &a) {
                return a.data();
        })
        .def("data_size", &fc::sha256::data_size)
        .def("hash_str", (fc::sha256 (*)()) &fc::sha256::hash<std::string>)
        .def("hash", (fc::sha256 (*)()) &fc::sha256::hash<fc::sha256>)
        .def(py::self == py::self)
        .def(py::self != py::self)
        .def(py::self >= py::self)
        .def(py::self  > py::self)
        .def(py::self  < py::self)
        .def("pop_count", &fc::sha256::pop_count)
        .def("clz", &fc::sha256::clz)
        .def("approx_log_32", &fc::sha256::approx_log_32)
        .def("set_to_inverse_approx_log_32", &fc::sha256::set_to_inverse_approx_log_32)
        .def("inverse_approx_log_32_double", (double (*)()) &fc::sha256::inverse_approx_log_32_double);

    py::class_<fc::sha512>(crypto_mod, "SHA512")
        .def(py::init([] (std::string hex) {
            return new fc::sha512(hex);
        }))
        .def("__str__", &fc::sha512::str)
        .def("data", [](const fc::sha512 &a) {
                return a.data();
        })
        .def("data_size", &fc::sha512::data_size)
        .def("hash_str", (fc::sha512 (*)(const std::string&)) &fc::sha512::hash)
        .def(py::self == py::self)
        .def(py::self != py::self)
        .def(py::self >= py::self)
        .def(py::self  > py::self)
        .def(py::self  < py::self);

    py::class_<fc::ripemd160>(crypto_mod, "RIPEMD160")
        .def(py::init([] (std::string hex) {
            return new fc::ripemd160(hex);
        }))
        .def("__str__", &fc::ripemd160::str)
        .def("data", [](const fc::ripemd160 &a) {
                return a.data();
        })
        .def("data_size", &fc::ripemd160::data_size)
        .def("hash_str", (fc::ripemd160 (*)()) &fc::ripemd160::hash<std::string>)
        .def("hash_sha256", (fc::ripemd160 (*)()) &fc::ripemd160::hash<fc::sha256>)
        .def("hash_sha512", (fc::ripemd160 (*)()) &fc::ripemd160::hash<fc::sha512>)
        .def(py::self == py::self)
        .def(py::self != py::self)
        .def(py::self >= py::self)
        .def(py::self  > py::self)
        .def(py::self  < py::self);

    /*
     *
     * chain
     *
     */

    py::class_<chain::protocol_feature_activation>(chain_mod, "ProtocolFeatureActivation")
        .def(py::init())
        .def(py::init<const vector<chain::digest_type>&>())
        .def("extension_id", &chain::protocol_feature_activation::extension_id)
        .def("enforce_unique", &chain::protocol_feature_activation::enforce_unique)
        .def("reflector_init", &chain::protocol_feature_activation::reflector_init)
        .def_readwrite("protocol_features", &chain::protocol_feature_activation::protocol_features);

    py::class_<chain::name>(chain_mod, "Name")
        .def(py::init([] () {
            return new chain::name();
        }))
        .def(py::init([] (std::string str) {
            return new chain::name(str);
        }))
        .def(py::init([] (uint64_t v) {
            return new chain::name(v);
        }))
        .def("__str__", &chain::name::to_string)
        .def("as_int",  &chain::name::to_uint64_t)
        .def(py::self  < py::self)
        .def(py::self  > py::self)
        .def(py::self <= py::self)
        .def(py::self >= py::self)
        .def(py::self == py::self)
        .def(py::self != py::self)
        .def(py::self == uint64_t())
        .def(py::self != uint64_t());


    py::class_<chain::block_timestamp_type>(chain_mod, "BlockTimestamp")
        .def(py::init([] (uint32_t s) {
            return new chain::block_timestamp_type(s);
        }))
        .def(py::init([] (fc::time_point t) {
            return new chain::block_timestamp_type(t);
        }))
        .def("max", (chain::block_timestamp_type (*)()) &chain::block_timestamp_type::maximum)
        .def("min", (chain::block_timestamp_type (*)()) &chain::block_timestamp_type::min)
        .def("next", &chain::block_timestamp_type::next)
        .def("as_time_point", &chain::block_timestamp_type::to_time_point)
        .def(py::self  > py::self)
        .def(py::self >= py::self)
        .def(py::self  < py::self)
        .def(py::self <= py::self)
        .def(py::self == py::self)
        .def(py::self != py::self)
        .def_readwrite("slot", &chain::block_timestamp_type::slot);

    py::class_<chain::block_header>(chain_mod, "BlockHeader")
        .def(py::init())
        .def_readwrite("timestamp", &chain::block_header::timestamp)
        .def_readwrite("producer", &chain::block_header::producer)
        .def_readwrite("confirmed", &chain::block_header::confirmed)
        .def_readwrite("previous", &chain::block_header::previous)
        .def_readwrite("transaction_mroot", &chain::block_header::transaction_mroot)
        .def_readwrite("action_mroot", &chain::block_header::action_mroot)
        .def_readwrite("header_extensions", &chain::block_header::header_extensions)
        .def("digest", &chain::block_header::digest)
        .def("calculate_id", &chain::block_header::calculate_id)
        .def("block_num", &chain::block_header::block_num)
        .def("num_from_id", &chain::block_header::num_from_id);

    py::class_<
        chain::signed_block_header,
        chain::block_header
    >(chain_mod, "SignedBlockHeader")
        .def(py::init())
        .def_readwrite("producer_signature", &chain::signed_block_header::producer_signature);

    py::class_<chain::block_header_extension>(chain_mod, "BlockHeaderExtension")
        .def(py::init());

    py::class_<
        chain::signed_block,
        chain::signed_block_header
    > signed_block(chain_mod, "SignedBlock");

    py::enum_<chain::signed_block::prune_state_type>(signed_block, "PruneState")
        .value("incomplete", chain::signed_block::prune_state_type::incomplete)
        .value("complete", chain::signed_block::prune_state_type::complete)
        .value("complete_legacy", chain::signed_block::prune_state_type::complete_legacy)
        .export_values();

    signed_block
        .def(py::init())
        .def(py::init<const chain::signed_block_header&>())
        .def("clone", &chain::signed_block::clone)
        .def("get_prune_state", [](const chain::signed_block& b) {
            return b.prune_state.value;
        })
        .def("set_prune_state", [](chain::signed_block& b, chain::signed_block::prune_state_type val) {
            b.prune_state = val; 
        });

    /*
     *
     * chain.testing
     *
     */



}
