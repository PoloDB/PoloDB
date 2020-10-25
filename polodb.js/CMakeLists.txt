project (polodb-js)
include_directories(${CMAKE_JS_INC})
file(GLOB SOURCE_FILES "db.c")
add_library(${PROJECT_NAME} SHARED ${SOURCE_FILES} ${CMAKE_JS_SRC})
set_target_properties(${PROJECT_NAME} PROPERTIES PREFIX "" SUFFIX ".node")

IF (WIN32)
    get_filename_component(PROJECT_LIB "../target/release/polodb_clib.lib" ABSOLUTE ${PROJECT_SOURCE_DIR})
    message("poldob win32: " ${PROJECT_LIB})
    target_link_libraries(${PROJECT_NAME} ${CMAKE_JS_LIB} ${PROJECT_LIB}
        Userenv.lib
        shell32.lib
        Ws2_32.lib
        Advapi32.lib
    )
ELSE()
    set(PROJECT_LIB ${PROJECT_SOURCE_DIR}/../target/release/libpolodb_clib.a)
    target_link_libraries(${PROJECT_NAME} ${CMAKE_JS_LIB} ${PROJECT_LIB})
ENDIF()
