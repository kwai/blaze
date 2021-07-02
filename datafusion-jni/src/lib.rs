// This is the interface to the JVM that we'll call the majority of our
// methods on.
use jni::JNIEnv;

// These objects are what you should use as arguments to your native
// function. They carry extra lifetime information to prevent them escaping
// this context and getting used after being GC'd.
use jni::objects::{JClass, JString};

// This is just a pointer. We'll be returning it from our function. We
// can't return one of the objects with lifetime information because the
// lifetime checker won't let us.
use jni::sys::jbyteArray;

// This keeps Rust from "mangling" the name and making it unique for this
// crate.
#[no_mangle]
pub extern "system" fn Java_com_kwai_sod_NativeRun_callNative(
    env: JNIEnv,
    _class: JClass,
    stage_desc: jbyteArray,
    input_paths: JString,
    output_dir: JString,
    output_name: JString,
) -> jbyteArray {

    let _stage_desc = env.convert_byte_array(stage_desc).unwrap();
    let _input_paths: String = env.get_string(input_paths).unwrap().into();
    let _output_dir: String = env.get_string(output_dir).unwrap().into();
    let _output_name: String = env.get_string(output_name).unwrap().into();



    let buf = [1; 20];
    let output = env.byte_array_from_slice(&buf).unwrap();
    output
}
