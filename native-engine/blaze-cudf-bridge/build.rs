fn main() {
    let lib_path = "native-engine/blaze-cudf-bridge/bridge-cpp/build";
    println!("cargo::rustc-link-search=native={lib_path}");
    println!("cargo::rustc-link-arg=-Wl,-rpath,$ORIGIN/{lib_path}");
    println!("cargo::rustc-link-lib=dylib=blaze-cudf-bridge");
    println!("cargo::rerun-if-changed={lib_path}");
}
