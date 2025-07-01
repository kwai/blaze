cmake_minimum_required(VERSION 3.15)
project(blaze-cudf-bridge)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(VCPKG_BUILD_TYPE release)
set(VCPKG_OVERLAY_PORTS "${CMAKE_SOURCE_DIR}/vcpkg_ports")

find_package(Arrow CONFIG REQUIRED)
find_package(CUDAToolkit REQUIRED)
find_package(cudf REQUIRED)
find_package(spdlog CONFIG REQUIRED)

include_directories("${CMAKE_SOURCE_DIR}")
file(GLOB_RECURSE SOURCES CONFIGURE_DEPENDS "src/*.cpp")
list(FILTER SOURCES EXCLUDE REGEX ".*_test\\.cpp$")
add_library(${PROJECT_NAME} SHARED ${SOURCES})

target_link_libraries(${PROJECT_NAME}
    PRIVATE
    Arrow::arrow_static
    CUDA::cudart
    cudf::cudf
    spdlog::spdlog
)
