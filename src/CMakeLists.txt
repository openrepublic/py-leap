cmake_minimum_required(VERSION 3.21)
project(python_bindings)

set( Boost_USE_STATIC_LIBS OFF CACHE STRING "ON or OFF" )

set(CMAKE_POSITION_INDEPENDENT_CODE ON)
add_subdirectory(libs/pybind11)

# EOSIO CONF

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/libs/eos/CMakeModules")

include(GNUInstallDirs)
include(InstallDirectoryPermissions)
include(MASSigning)

set( BLOCKCHAIN_NAME "EOSIO" )
set( CMAKE_CXX_STANDARD 17 )
set( CMAKE_CXX_EXTENSIONS ON )
set( CXX_STANDARD_REQUIRED ON)

set(VERSION_MAJOR 2)
set(VERSION_MINOR 1)
set(VERSION_PATCH 0)
#set(VERSION_SUFFIX rc3)

if(VERSION_SUFFIX)
    set(VERSION_FULL "${VERSION_MAJOR}.${VERSION_MINOR}.${VERSION_PATCH}-${VERSION_SUFFIX}")
else()
    set(VERSION_FULL "${VERSION_MAJOR}.${VERSION_MINOR}.${VERSION_PATCH}")
endif()

set( CLI_CLIENT_EXECUTABLE_NAME cleos )
set( NODE_EXECUTABLE_NAME nodeos )
set( KEY_STORE_EXECUTABLE_NAME keosd )
set( RODEOS_EXECUTABLE_NAME rodeos )
set( TESTER_EXECUTABLE_NAME eosio-tester )

# http://stackoverflow.com/a/18369825
if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
    if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS 8.0)
        message(FATAL_ERROR "GCC version must be at least 8.0!")
    endif()
elseif ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang" OR "${CMAKE_CXX_COMPILER_ID}" STREQUAL "AppleClang")
    if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS 5.0)
        message(FATAL_ERROR "Clang version must be at least 5.0!")
    endif()
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wsign-compare -Wrange-loop-analysis")
endif()

if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "AppleClang")
  set(CMAKE_VISIBILITY_INLINES_HIDDEN ON)
endif()

if ("${CMAKE_GENERATOR}" STREQUAL "Ninja")
   add_compile_options(-fdiagnostics-color=always)
endif()

set(BUILD_DOXYGEN FALSE CACHE BOOL "Build doxygen documentation on every make")
set(ENABLE_MULTIVERSION_PROTOCOL_TEST FALSE CACHE BOOL "Enable nodeos multiversion protocol test")

# add defaults for openssl
if(APPLE AND UNIX AND "${OPENSSL_ROOT_DIR}" STREQUAL "")
   set(OPENSSL_ROOT_DIR "/usr/local/opt/openssl@1.1")
endif()

# WASM runtimes to enable. Each runtime in this list will have:
#  * definition EOSIO_<RUNTIME>_RUNTIME_ENABLED defined in public libchain interface
#  * ctest entries with --runtime
# TODO reenable later
if(CMAKE_SIZEOF_VOID_P EQUAL 8 AND NOT WIN32)
   if("${CMAKE_SYSTEM_NAME}" STREQUAL "Linux" AND "${CMAKE_SYSTEM_PROCESSOR}" STREQUAL "x86_64")
      list(APPEND EOSIO_WASM_RUNTIMES eos-vm-oc)
      # EOS VM OC requires LLVM, but move the check up here to a central location so that the EosioTester.cmakes
      # can be created with the exact version found
      find_package(LLVM REQUIRED CONFIG)
     if(LLVM_VERSION_MAJOR VERSION_LESS 7 OR LLVM_VERSION_MAJOR VERSION_GREATER_EQUAL 12)
        message(FATAL_ERROR "EOSIO requires an LLVM version 7 through 11")
     endif()
   endif()
endif()

if(CMAKE_SIZEOF_VOID_P EQUAL 8 AND NOT WIN32)
   if(CMAKE_SYSTEM_PROCESSOR STREQUAL x86_64)
      list(APPEND EOSIO_WASM_RUNTIMES eos-vm eos-vm-jit)
   endif()
endif()

if(UNIX)
  if(APPLE)
    set(whole_archive_flag "-force_load")
    set(no_whole_archive_flag "")
  else()
    set(whole_archive_flag "--whole-archive")
    set(no_whole_archive_flag "--no-whole-archive")
  endif()
else()
  set(whole_archive_flag "--whole-archive")
  set(no_whole_archive_flag "--no-whole-archive")
endif()

set(Boost_USE_MULTITHREADED      ON)
# Most boost deps get implictly picked up via fc, as just about everything links to fc. In addition we pick up
# the pthread dependency through fc.
find_package(Boost REQUIRED COMPONENTS 
    date_time
    filesystem
    system
    chrono
    iostreams
    program_options
    unit_test_framework)

if( APPLE AND UNIX )
# Apple Specific Options Here
    message( STATUS "Configuring EOSIO on macOS" )
    set( CMAKE_C_FLAGS   "${CMAKE_C_FLAGS}   -Wall -Wno-deprecated-declarations" )
    set( CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wno-deprecated-declarations" )
else()
    # Linux Specific Options Here
    message( STATUS "Configuring EOSIO on Linux" )
    set( CMAKE_C_FLAGS   "${CMAKE_C_FLAGS}   -Wall" )
    set( CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall" )
    if ( FULL_STATIC_BUILD )
      set( CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -static-libstdc++ -static-libgcc")
    endif ( FULL_STATIC_BUILD )

    if( "${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang" )
        if( CMAKE_CXX_COMPILER_VERSION VERSION_EQUAL 4.0.0 OR CMAKE_CXX_COMPILER_VERSION VERSION_GREATER 4.0.0 )
            set( CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-invalid-partial-specialization" )
        endif()
    endif()
endif()

option(EOSIO_ENABLE_DEVELOPER_OPTIONS "enable developer options for EOSIO" OFF)

# based on http://www.delorie.com/gnu/docs/gdb/gdb_70.html
# uncomment this line to tell GDB about macros (slows compile times)
# set( CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -gdwarf-2 -g3" )

set(ENABLE_COVERAGE_TESTING FALSE CACHE BOOL "Build EOSIO for code coverage analysis")

if(ENABLE_COVERAGE_TESTING)
  SET(CMAKE_C_FLAGS   "--coverage ${CMAKE_C_FLAGS}")
  SET(CMAKE_CXX_FLAGS "--coverage ${CMAKE_CXX_FLAGS}")
  find_program( LCOV_PATH lcov )
  find_program( LLVMCOV_PATH  llvm-cov )
  find_program( GENHTML_PATH NAMES genhtml)
endif()

include(utils)

if ("${CORE_SYMBOL_NAME}" STREQUAL "")
  set( CORE_SYMBOL_NAME "SYS" )
endif()
string(TOUPPER ${CORE_SYMBOL_NAME} CORE_SYMBOL_NAME)

string(LENGTH ${CORE_SYMBOL_NAME} CORE_SYMBOL_NAME_LENGTH)
if (CORE_SYMBOL_NAME_LENGTH GREATER 7)
  message(FATAL_ERROR "CORE_SYMBOL_NAME length must be between 1 and 7 characters")
endif()

message( STATUS "Using '${CORE_SYMBOL_NAME}' as CORE symbol name" )

if ("${EOSIO_ROOT_KEY}" STREQUAL "")
   set(EOSIO_ROOT_KEY "EOS6MRyAjQq8ud7hVNYcfnVPJqcVpscN5So8BhtHuGYqET5GDW5CV")
endif()

message( STATUS "Using '${EOSIO_ROOT_KEY}' as public key for 'eosio' account" )

find_package( Gperftools QUIET )
if( GPERFTOOLS_FOUND )
    message( STATUS "Found gperftools; compiling EOSIO with TCMalloc")
    #if doing this by the book, simply link_libraries( ${GPERFTOOLS_TCMALLOC} ) here. That will
    #give the performance benefits of tcmalloc but since it won't be linked last
    #the heap profiler & checker may not be accurate. This here is rather undocumented behavior
    #to stuff a library toward the end of the link list
    set(CMAKE_C_STANDARD_LIBRARIES "${CMAKE_C_STANDARD_LIBRARIES} ${GPERFTOOLS_TCMALLOC}")
    set(CMAKE_CXX_STANDARD_LIBRARIES "${CMAKE_CXX_STANDARD_LIBRARIES} ${GPERFTOOLS_TCMALLOC}")
endif()

add_subdirectory(libs/eos/libraries)

add_library(py_eosio MODULE python_bindings.cpp)

find_package(Boost REQUIRED COMPONENTS 
    date_time
    filesystem
    system
    chrono
    iostreams
    unit_test_framework)

target_include_directories(
    py_eosio
    PUBLIC
    ${Boost_INCLUDE_DIRS}
    "libs/eos/libraries/fc/include"
    "libs/eos/libraries/chain/include"
    "libs/eos/libraries/testing/include"
    "libs/eos/libraries/wasm-jit/Include"
    "libs/eos/libraries/chainbase/include"
    "libs/eos/libraries/softfloat/source/include")

target_link_libraries(
    py_eosio
    PUBLIC
    pybind11::module pybind11::lto
    eosio_chain
    chainbase
    fc
    ${ROCKSDB_SHARED_LIB}
    ${Boost_LIBRARIES})


pybind11_extension(py_eosio)
pybind11_strip(py_eosio)

set_target_properties(py_eosio PROPERTIES CXX_VISIBILITY_PRESET "hidden"
                                         CUDA_VISIBILITY_PRESET "hidden")



install(TARGETS py_eosio
    DESTINATION "${CMAKE_INSTALL_PREFIX}/py-eosio/lib")

install(CODE [[
    file(GET_RUNTIME_DEPENDENCIES
        RESOLVED_DEPENDENCIES_VAR RES
        UNRESOLVED_DEPENDENCIES_VAR UNRES
        CONFLICTING_DEPENDENCIES_PREFIX CONFLICTING_DEPENDENCIES
        LIBRARIES $<TARGET_FILE:py_eosio>
    )

    set(DEP_WHITELIST, "")
    list(APPEND DEP_WHITELIST "fc")
    list(APPEND DEP_WHITELIST "ssl")
    list(APPEND DEP_WHITELIST "crypto")
    list(APPEND DEP_WHITELIST "secp256k1")
    list(APPEND DEP_WHITELIST "eosio_chain")
    list(APPEND DEP_WHITELIST "boost_chrono")
    list(APPEND DEP_WHITELIST "boost_date_time")
    list(APPEND DEP_WHITELIST "boost_iostreams")
    list(APPEND DEP_WHITELIST "boost_filesystem")

    foreach(DEP ${RES})
        cmake_path(GET DEP STEM DEPFILE)
        message("Searching ^${DEPFILE}")
        foreach(WDEP ${DEP_WHITELIST})
            if (DEPFILE MATCHES "^lib${WDEP}")
                message("Found dependency: ${DEP}")
                file(INSTALL
                    DESTINATION "${CMAKE_INSTALL_PREFIX}/py-eosio/lib"
                    TYPE SHARED_LIBRARY
                    FOLLOW_SYMLINK_CHAIN
                    FILES "${DEP}"
                )
                break()
            endif()
        endforeach()
    endforeach()
]])
