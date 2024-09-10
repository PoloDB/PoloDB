PROJECT("polodb")

ADD_LIBRARY(polodb INTERFACE)
TARGET_INCLUDE_DIRECTORIES(polodb INTERFACE include)
TARGET_LINK_DIRECTORIES(polodb INTERFACE ${CMAKE_SOURCE_DIR}/../../target/release)
TARGET_LINK_LIBRARIES(polodb INTERFACE
    polodb_c_exporter.lib
    Ws2_32             # Winsock library for networking
    Userenv            # Library for user environment functions
    ntdll              # NT Native API library
    kernel32           # Core Windows API library
    bcrypt             # Cryptography API (CNG) library for BCryptGenRandom
    Rpcrt4             # RPC Runtime library for UUID functions
    Shlwapi            # Shell light-weight utility library for path functions
)
