// Copyright 2022 The Blaze Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use datafusion::common::Result;
use jni::{
    objects::GlobalRef,
    sys::{JNI_FALSE, JNI_TRUE},
};
use once_cell::sync::OnceCell;

pub mod conf;
pub mod jni_bridge;

pub fn is_jni_bridge_inited() -> bool {
    jni_bridge::JavaClasses::inited()
}

pub fn is_task_running() -> bool {
    fn is_task_running_impl() -> Result<bool> {
        if !jni_call_static!(JniBridge.isTaskRunning() -> bool).unwrap() {
            jni_exception_clear!()?;
            return Ok(false);
        }
        Ok(true)
    }
    if !is_jni_bridge_inited() {
        // only for testing
        return true;
    }
    is_task_running_impl().expect("calling JniBridge.isTaskRunning() error")
}

pub fn java_true() -> &'static GlobalRef {
    static OBJ_TRUE: OnceCell<GlobalRef> = OnceCell::new();
    OBJ_TRUE.get_or_init(|| {
        let true_local = jni_new_object!(JavaBoolean(JNI_TRUE)).unwrap();
        jni_new_global_ref!(true_local.as_obj()).unwrap()
    })
}

pub fn java_false() -> &'static GlobalRef {
    static OBJ_FALSE: OnceCell<GlobalRef> = OnceCell::new();
    OBJ_FALSE.get_or_init(|| {
        let false_local = jni_new_object!(JavaBoolean(JNI_FALSE)).unwrap();
        jni_new_global_ref!(false_local.as_obj()).unwrap()
    })
}
