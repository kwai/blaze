use flapigen::{LanguageConfig, JavaConfig};
use std::{
    env,
    path::{Path, PathBuf},
};

fn main() {
    println!("cargo:rerun-if-changed");
    let out_dir = env::var("OUT_DIR").expect("no OUT_DIR, but cargo should provide it");
    let jni_c_headers_rs = Path::new(&out_dir).join("jni_c_header.rs");
    gen_jni_bindings(&jni_c_headers_rs);

    let java_cfg = JavaConfig::new(
        Path::new("lib")
        .join("src")
        .join("main")
        .join("java")
        .join("com")
        .join("kwai")
        .join("sod"),
        "com.kwai.sod".into(),
    );

    let in_src = Path::new("src").join("java_glue.rs.in");
    let out_src = Path::new(&out_dir).join("java_glue.rs");
    let flap_gen =
        flapigen::Generator::new(
            LanguageConfig::JavaConfig(java_cfg)
        ).rustfmt_bindings(true);

    flap_gen.expand("java bindings", &in_src, &out_src);
    println!("cargo:rerun-if-changed={}", in_src.display());
}

fn gen_jni_bindings(jni_c_headers_rs: &Path) {
    let java_home = env::var("JAVA_HOME").expect("JAVA_HOME env variable not settted");
    let java_include_dir = Path::new(&java_home).join("include");
    let target = env::var("TARGET").expect("target env var not set");

    let java_sys_include_dir = java_include_dir.join(if target.contains("windows") {
        "win32"
    } else if target.contains("darwin") {
        "darwin"
    } else {
        "linux"
    });

    let include_dirs = [java_include_dir, java_sys_include_dir];
    println!("jni include dirs {:?}", include_dirs);

    let jni_h_path = search_file_in_directory(&include_dirs[..], "jni.h").expect("Can not find jni.h");
    println!("cargo:rerun-if-changed={}", jni_h_path.display());

    gen_binding(&include_dirs[..], &jni_h_path, jni_c_headers_rs).expect("gen_binding failed");
}

fn search_file_in_directory<P: AsRef<Path>>(dirs: &[P], file: &str) -> Result<PathBuf, ()> {
    for dir in dirs {
        let dir = dir.as_ref().to_path_buf();
        let file_path = dir.join(file);
        if file_path.exists() && file_path.is_file() {
            return Ok(file_path);
        }
    }
    Err(())
}

fn gen_binding<P: AsRef<Path>>(
    include_dirs: &[P],
    c_file_path: &Path,
    output_rust: &Path,
) -> Result<(), String> {
    let mut bindings: bindgen::Builder = bindgen::builder().header(c_file_path.to_str().unwrap());

    bindings = include_dirs.iter().fold(bindings, |acc, x| {
        acc.clang_arg("-I".to_string() + x.as_ref().to_str().unwrap())
    });

    let generated_bindings = bindings
        .generate()
        .map_err(|_| "Failed to generate bindings".to_string())?;

    generated_bindings
        .write_to_file(output_rust)
        .map_err(|err| err.to_string())?;

    Ok(())
}
