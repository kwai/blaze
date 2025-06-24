vcpkg_from_github(
    OUT_SOURCE_PATH SOURCE_PATH
    REPO rapidsai/cudf
    REF "branch-25.06"
    SHA512 49fb54e301e070308d2d3be933c61b249b8e67073dbe36db597950aaff84f61d14038ece830e0d61e322d415547b45cb11b37155fd6606f7c5edf1667363c2e4
    HEAD_REF main
)

# check for nvcc
execute_process(
    COMMAND nvcc --version
    OUTPUT_VARIABLE NVCC_VERSION_OUTPUT
    ERROR_VARIABLE NVCC_VERSION_ERROR
    RESULT_VARIABLE NVCC_VERSION_RESULT
)
if(NOT ${NVCC_VERSION_RESULT} EQUAL 0)
    message(FATAL_ERROR "execute nvcc --version failed: ${NVCC_VERSION_ERROR}")
endif()

set(BUILD_ARGS --disable_large_strings --cmake-args="-DCUDA_STATIC_RUNTIME=OFF -DBUILD_SHARED_LIBS=OFF")
set(ENV{INSTALL_PREFIX} "${CURRENT_PACKAGES_DIR}")

# build
vcpkg_execute_required_process(
    COMMAND "${SOURCE_PATH}/build.sh" libcudf ${BUILD_ARGS} -n
    WORKING_DIRECTORY "${SOURCE_PATH}"
    LOGNAME "build-${TARGET_TRIPLET}"
)

# patch for installation
vcpkg_execute_required_process(
  COMMAND sed -i "230,231s/^/#/" "./cpp/build/_deps/rapids-cmake-src/rapids-cmake/cpm/nvcomp.cmake"
  WORKING_DIRECTORY "${SOURCE_PATH}"
  LOGNAME "build-${TARGET_TRIPLET}"
)
file(MAKE_DIRECTORY "${CURRENT_PACKAGES_DIR}/usr/lib64")

# install
vcpkg_execute_required_process(
    COMMAND "${SOURCE_PATH}/build.sh" libcudf ${BUILD_ARGS}
    WORKING_DIRECTORY "${SOURCE_PATH}"
    LOGNAME "build-${TARGET_TRIPLET}"
)

# fix some file conflicts
file(RENAME "${CURRENT_PACKAGES_DIR}/include/zdict.h" "${CURRENT_PACKAGES_DIR}/include/cudf-zdict.h")
file(RENAME "${CURRENT_PACKAGES_DIR}/include/zstd.h" "${CURRENT_PACKAGES_DIR}/include/cudf-zstd.h")
file(RENAME "${CURRENT_PACKAGES_DIR}/include/zstd_errors.h" "${CURRENT_PACKAGES_DIR}/include/cudf-zstd_errors.h")

