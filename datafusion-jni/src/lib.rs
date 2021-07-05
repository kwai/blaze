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

use ballista_core::task_runner::run_task;

// This keeps Rust from "mangling" the name and making it unique for this
// crate.
#[no_mangle]
pub extern "system" fn Java_com_kwai_sod_NativeRun_callNative(
    env: JNIEnv,
    _class: JClass,
    task: jbyteArray,
    executor_id: JString,
    work_dir: JString,
    file_name: JString,
) -> jbyteArray {
    let task = env.convert_byte_array(task).unwrap();
    let executor_id: String = env.get_string(executor_id).unwrap().into();
    let work_dir: String = env.get_string(work_dir).unwrap().into();
    let file_name: String = env.get_string(file_name).unwrap().into();

    let buf = run_task(task, executor_id, work_dir, file_name);
    env.byte_array_from_slice(&buf).unwrap()
}
