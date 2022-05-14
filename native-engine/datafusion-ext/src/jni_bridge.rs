use std::cell::Cell;
use std::sync::Arc;
use std::sync::Mutex;

use crate::ResultExt;
use jni::errors::Result as JniResult;
use jni::objects::JClass;
use jni::objects::JMethodID;
use jni::objects::JObject;
use jni::objects::JStaticMethodID;
use jni::signature::JavaType;
use jni::signature::Primitive;
use jni::JNIEnv;
use jni::JavaVM;

#[macro_export]
macro_rules! jni_map_error {
    ($result:expr) => {{
        match $result {
            Ok(result) => datafusion::error::Result::Ok(result),
            Err(jni::errors::Error::JavaException) => {
                let env = $crate::jni_bridge::JavaClasses::get_thread_jnienv();
                let _ = env.exception_describe();
                Err(datafusion::error::DataFusionError::External(
                    format!("Java exception thrown at {}:{}", file!(), line!()).into(),
                ))
            }
            Err(err) => Err(datafusion::error::DataFusionError::External(
                format!(
                    "Unknown JNI error occurred at {}:{}: {:?}",
                    file!(),
                    line!(),
                    err
                )
                .into(),
            )),
        }
    }};
}

#[macro_export]
macro_rules! jvalues {
    ($($args:expr,)* $(,)?) => {{
        &[$(jni::objects::JValue::from($args)),*] as &[jni::objects::JValue]
    }}
}

#[macro_export]
macro_rules! jni_bridge_new_object {
    ($env:expr, $clsname:ident $(, $args:expr)*) => {{
        log::trace!(
            "jni_bridge_new_object!({}, {:?})",
            stringify!($clsname),
            $crate::jvalues!($($args,)*));
        $crate::jni_map_error!(
            $env.new_object_unchecked(
                paste::paste! {JavaClasses::get().[<c $clsname>].class},
                paste::paste! {JavaClasses::get().[<c $clsname>].ctor},
                $crate::jvalues!($($args,)*))
        )
    }}
}

#[macro_export]
macro_rules! jni_bridge_call_method {
    ($env:expr, $clsname:ident.$method:ident -> $ret:ty, $obj:expr $(, $args:expr)*) => {{
        log::trace!("jni_bridge_call_method!({}.{}, {:?}, {:?})",
            stringify!($clsname),
            stringify!($method),
            $obj,
            $crate::jvalues!($($args,)*));
        $crate::jni_map_error!(
            $env.call_method_unchecked(
                $obj,
                paste::paste! {JavaClasses::get().[<c $clsname>].[<method_ $method>]},
                paste::paste! {JavaClasses::get().[<c $clsname>].[<method_ $method _ret>]}.clone(),
                $crate::jvalues!($($args,)*)
            )
        ).and_then(|result| $crate::jni_map_error!(<$ret>::try_from(result)))
    }}
}

#[macro_export]
macro_rules! jni_bridge_call_static_method {
    ($env:expr, $clsname:ident . $method:ident -> $ret:ty $(,$args:expr)* $(,)?) => {{
        log::trace!(
            "jni_bridge_call_static_method!({}.{}, {:?})",
            stringify!($clsname),
            stringify!($method),
            $crate::jvalues!($($args,)*));
        $crate::jni_map_error!(
            $env.call_static_method_unchecked(
                paste::paste! {JavaClasses::get().[<c $clsname>].class},
                paste::paste! {JavaClasses::get().[<c $clsname>].[<method_ $method>]},
                paste::paste! {JavaClasses::get().[<c $clsname>].[<method_ $method _ret>]}.clone(),
                $crate::jvalues!($($args,)*)
            )
        ).and_then(|result| $crate::jni_map_error!(<$ret>::try_from(result)))
    }}
}

#[macro_export]
macro_rules! jni_weak_global_ref {
    ($env:expr, $obj:expr) => {{
        $crate::jni_map_error!({
            let jnienv = &*(*$env.get_native_interface());
            if let Some(new_weak_global_ref) = &jnienv.NewWeakGlobalRef {
                let weak_global =
                    new_weak_global_ref($env.get_native_interface(), $obj.into_inner());
                if !weak_global.is_null() {
                    jni::errors::Result::Ok(JObject::from(weak_global))
                } else {
                    jni::errors::Result::Err(jni::errors::Error::NullPtr(
                        "NewWeakGlobalRef() returns null",
                    ))
                }
            } else {
                jni::errors::Result::Err(jni::errors::Error::JNIEnvMethodNotFound(
                    "NewWeakGlobalRef",
                ))
            }
        })
    }};
}

#[macro_export]
macro_rules! jni_global_ref {
    ($env:expr, $obj:expr) => {{
        $crate::jni_map_error!($env.new_global_ref($obj))
    }};
}

#[allow(non_snake_case)]
pub struct JavaClasses<'a> {
    pub jvm: JavaVM,
    pub classloader: JObject<'a>,

    pub cJniBridge: JniBridge<'a>,
    pub cClass: JavaClass<'a>,
    pub cJavaRuntimeException: JavaRuntimeException<'a>,
    pub cJavaNioSeekableByteChannel: JavaNioSeekableByteChannel<'a>,
    pub cJavaBoolean: JavaBoolean<'a>,
    pub cJavaLong: JavaLong<'a>,
    pub cJavaList: JavaList<'a>,
    pub cJavaMap: JavaMap<'a>,
    pub cJavaFile: JavaFile<'a>,
    pub cJavaConsumer: JavaConsumer<'a>,

    pub cScalaIterator: ScalaIterator<'a>,
    pub cScalaPromise: ScalaPromise<'a>,
    pub cScalaTuple2: ScalaTuple2<'a>,
    pub cScalaFunction0: ScalaFunction0<'a>,

    pub cHadoopFileSystem: HadoopFileSystem<'a>,
    pub cHadoopPath: HadoopPath<'a>,
    pub cHadoopFileStatus: HadoopFileStatus<'a>,
    pub cHadoopFSDataInputStream: HadoopFSDataInputStream<'a>,

    pub cSparkManagedBuffer: SparkManagedBuffer<'a>,
    pub cSparkSQLMetric: SparkSQLMetric<'a>,
    pub cSparkMetricNode: SparkMetricNode<'a>,
}

#[allow(clippy::non_send_fields_in_send_ty)]
unsafe impl<'a> Send for JavaClasses<'a> {} // safety: see JavaClasses::init()
unsafe impl<'a> Sync for JavaClasses<'a> {}

// safety:
//   All jclasses and jmethodids are implemented in raw pointers and can be
//   safely initialized to zero (null)
//
static mut JNI_JAVA_CLASSES: [u8; std::mem::size_of::<JavaClasses>()] =
    [0; std::mem::size_of::<JavaClasses>()];

impl JavaClasses<'static> {
    pub fn init(env: &JNIEnv) -> JniResult<()> {
        lazy_static::lazy_static! {
            static ref JNI_JAVA_CLASSES_INITIALIZED: Arc<Mutex<Cell<bool>>> =
                Arc::default();
        }
        let jni_java_classes_initialized = JNI_JAVA_CLASSES_INITIALIZED.lock().unwrap();
        if jni_java_classes_initialized.get() {
            return Ok(()); // already initialized
        }

        let mut initialized_java_classes = JavaClasses {
            jvm: env.get_java_vm()?,
            classloader: JObject::null(),

            cJniBridge: JniBridge::new(env)?,
            cClass: JavaClass::new(env)?,
            cJavaRuntimeException: JavaRuntimeException::new(env)?,
            cJavaNioSeekableByteChannel: JavaNioSeekableByteChannel::new(env)?,
            cJavaBoolean: JavaBoolean::new(env)?,
            cJavaLong: JavaLong::new(env)?,
            cJavaList: JavaList::new(env)?,
            cJavaMap: JavaMap::new(env)?,
            cJavaFile: JavaFile::new(env)?,
            cJavaConsumer: JavaConsumer::new(env)?,

            cScalaIterator: ScalaIterator::new(env)?,
            cScalaPromise: ScalaPromise::new(env)?,
            cScalaTuple2: ScalaTuple2::new(env)?,
            cScalaFunction0: ScalaFunction0::new(env)?,

            cHadoopFileSystem: HadoopFileSystem::new(env)?,
            cHadoopPath: HadoopPath::new(env)?,
            cHadoopFileStatus: HadoopFileStatus::new(env)?,
            cHadoopFSDataInputStream: HadoopFSDataInputStream::new(env)?,

            cSparkManagedBuffer: SparkManagedBuffer::new(env)?,
            cSparkSQLMetric: SparkSQLMetric::new(env)?,
            cSparkMetricNode: SparkMetricNode::new(env)?,
        };
        initialized_java_classes.classloader = get_global_ref_jobject(
            env,
            env.call_static_method_unchecked(
                initialized_java_classes.cJniBridge.class,
                initialized_java_classes
                    .cJniBridge
                    .method_getContextClassLoader,
                initialized_java_classes
                    .cJniBridge
                    .method_getContextClassLoader_ret
                    .clone(),
                &[],
            )?
            .l()?,
        )?;

        unsafe {
            // safety:
            //  JavaClasses should be initialized once in jni entrypoint thread
            //  no write/read conflicts will happen
            let jni_java_classes = JNI_JAVA_CLASSES.as_mut_ptr() as *mut JavaClasses;
            *jni_java_classes = initialized_java_classes;
        }
        assert!(!env.exception_check().unwrap());
        jni_java_classes_initialized.set(true);
        Ok(())
    }

    pub fn get() -> &'static JavaClasses<'static> {
        unsafe {
            // safety: see JavaClasses::init()
            let jni_java_classes = JNI_JAVA_CLASSES.as_ptr() as *const JavaClasses;
            &*jni_java_classes
        }
    }

    pub fn get_thread_jnienv() -> JNIEnv<'static> {
        let jvm = &JavaClasses::get().jvm;

        if let Ok(env) = jvm.get_env() {
            return env;
        }
        let env = jvm.attach_current_thread_permanently().unwrap();

        jni_bridge_call_static_method!(
            env,
            JniBridge.setContextClassLoader -> (),
            JavaClasses::get().classloader
        )
        .unwrap_or_fatal();

        env
    }
}

#[allow(non_snake_case)]
pub struct JniBridge<'a> {
    pub class: JClass<'a>,
    pub method_raiseThrowable: JStaticMethodID<'a>,
    pub method_raiseThrowable_ret: JavaType,
    pub method_getContextClassLoader: JStaticMethodID<'a>,
    pub method_getContextClassLoader_ret: JavaType,
    pub method_setContextClassLoader: JStaticMethodID<'a>,
    pub method_setContextClassLoader_ret: JavaType,
    pub method_getHDFSFileSystem: JStaticMethodID<'a>,
    pub method_getHDFSFileSystem_ret: JavaType,
    pub method_getResource: JStaticMethodID<'a>,
    pub method_getResource_ret: JavaType,
    pub method_setTaskContext: JStaticMethodID<'a>,
    pub method_setTaskContext_ret: JavaType,
    pub method_getTaskContext: JStaticMethodID<'a>,
    pub method_getTaskContext_ret: JavaType,
    pub method_readFSDataInputStream: JStaticMethodID<'a>,
    pub method_readFSDataInputStream_ret: JavaType,
}
impl<'a> JniBridge<'a> {
    pub const SIG_TYPE: &'static str = "org/apache/spark/sql/blaze/JniBridge";

    pub fn new(env: &JNIEnv<'a>) -> JniResult<JniBridge<'a>> {
        let class = get_global_jclass(env, Self::SIG_TYPE)?;
        Ok(JniBridge {
            class,
            method_raiseThrowable: env.get_static_method_id(
                class,
                "raiseThrowable",
                "(Ljava/lang/Throwable;)V",
            )?,
            method_raiseThrowable_ret: JavaType::Primitive(Primitive::Void),
            method_getContextClassLoader: env.get_static_method_id(
                class,
                "getContextClassLoader",
                "()Ljava/lang/ClassLoader;",
            )?,
            method_getContextClassLoader_ret: JavaType::Object(
                "java/lang/ClassLoader".to_owned(),
            ),
            method_setContextClassLoader: env.get_static_method_id(
                class,
                "setContextClassLoader",
                "(Ljava/lang/ClassLoader;)V",
            )?,
            method_setContextClassLoader_ret: JavaType::Primitive(Primitive::Void),
            method_getHDFSFileSystem: env.get_static_method_id(
                class,
                "getHDFSFileSystem",
                "()Lorg/apache/hadoop/fs/FileSystem;",
            )?,
            method_getHDFSFileSystem_ret: JavaType::Object(
                HadoopFileSystem::SIG_TYPE.to_owned(),
            ),
            method_getResource: env.get_static_method_id(
                class,
                "getResource",
                "(Ljava/lang/String;)Ljava/lang/Object;",
            )?,
            method_getResource_ret: JavaType::Object(
                HadoopFileSystem::SIG_TYPE.to_owned(),
            ),
            method_getTaskContext: env.get_static_method_id(
                class,
                "getTaskContext",
                "()Lorg/apache/spark/TaskContext;",
            )?,
            method_getTaskContext_ret: JavaType::Object(
                "org/apache/spark/TaskContext".to_owned(),
            ),
            method_setTaskContext: env.get_static_method_id(
                class,
                "setTaskContext",
                "(Lorg/apache/spark/TaskContext;)V",
            )?,
            method_setTaskContext_ret: JavaType::Primitive(Primitive::Void),
            method_readFSDataInputStream: env.get_static_method_id(
                class,
                "readFSDataInputStream",
                "(Lorg/apache/hadoop/fs/FSDataInputStream;Ljava/nio/ByteBuffer;J)I",
            )?,
            method_readFSDataInputStream_ret: JavaType::Primitive(Primitive::Int),
        })
    }
}

#[allow(non_snake_case)]
pub struct JavaClass<'a> {
    pub class: JClass<'a>,
    pub method_getName: JMethodID<'a>,
    pub method_getName_ret: JavaType,
}
impl<'a> JavaClass<'a> {
    pub const SIG_TYPE: &'static str = "java/lang/Class";

    pub fn new(env: &JNIEnv<'a>) -> JniResult<JavaClass<'a>> {
        let class = get_global_jclass(env, Self::SIG_TYPE)?;
        Ok(JavaClass {
            class,
            method_getName: env.get_method_id(
                class,
                "getName",
                "()Ljava/lang/String;",
            )?,
            method_getName_ret: JavaType::Object("java/lang/String".to_owned()),
        })
    }
}

#[allow(non_snake_case)]
pub struct JavaRuntimeException<'a> {
    pub class: JClass<'a>,
    pub ctor: JMethodID<'a>,
}
impl<'a> JavaRuntimeException<'a> {
    pub const SIG_TYPE: &'static str = "java/lang/RuntimeException";

    pub fn new(env: &JNIEnv<'a>) -> JniResult<JavaRuntimeException<'a>> {
        let class = get_global_jclass(env, Self::SIG_TYPE)?;
        Ok(JavaRuntimeException {
            class,
            ctor: env.get_method_id(
                class,
                "<init>",
                "(Ljava/lang/String;Ljava/lang/Throwable;)V",
            )?,
        })
    }
}

#[allow(non_snake_case)]
pub struct JavaNioSeekableByteChannel<'a> {
    pub class: JClass<'a>,
    pub method_read: JMethodID<'a>,
    pub method_read_ret: JavaType,
    pub method_setPosition: JMethodID<'a>,
    pub method_setPosition_ret: JavaType,
    pub method_size: JMethodID<'a>,
    pub method_size_ret: JavaType,
}
impl<'a> JavaNioSeekableByteChannel<'a> {
    pub const SIG_TYPE: &'static str = "java/nio/channels/SeekableByteChannel";

    pub fn new(env: &JNIEnv<'a>) -> JniResult<JavaNioSeekableByteChannel<'a>> {
        let class = get_global_jclass(env, Self::SIG_TYPE)?;
        Ok(JavaNioSeekableByteChannel {
            class,
            method_read: env.get_method_id(class, "read", "(Ljava/nio/ByteBuffer;)I")?,
            method_read_ret: JavaType::Primitive(Primitive::Int),
            method_setPosition: env.get_method_id(
                class,
                "position",
                "(J)Ljava/nio/channels/SeekableByteChannel;",
            )?,
            method_setPosition_ret: JavaType::Object(Self::SIG_TYPE.to_owned()),
            method_size: env.get_method_id(class, "size", "()J")?,
            method_size_ret: JavaType::Primitive(Primitive::Long),
        })
    }
}

#[allow(non_snake_case)]
pub struct JavaBoolean<'a> {
    pub class: JClass<'a>,
    pub ctor: JMethodID<'a>,
}
impl<'a> JavaBoolean<'a> {
    pub const SIG_TYPE: &'static str = "java/lang/Boolean";

    pub fn new(env: &JNIEnv<'a>) -> JniResult<JavaBoolean<'a>> {
        let class = get_global_jclass(env, Self::SIG_TYPE)?;
        Ok(JavaBoolean {
            class,
            ctor: env.get_method_id(class, "<init>", "(Z)V")?,
        })
    }
}

#[allow(non_snake_case)]
pub struct JavaLong<'a> {
    pub class: JClass<'a>,
    pub ctor: JMethodID<'a>,
}
impl<'a> JavaLong<'a> {
    pub const SIG_TYPE: &'static str = "java/lang/Long";

    pub fn new(env: &JNIEnv<'a>) -> JniResult<JavaLong<'a>> {
        let class = get_global_jclass(env, Self::SIG_TYPE)?;
        Ok(JavaLong {
            class,
            ctor: env.get_method_id(class, "<init>", "(J)V")?,
        })
    }
}

#[allow(non_snake_case)]
pub struct JavaList<'a> {
    pub class: JClass<'a>,
    pub method_size: JMethodID<'a>,
    pub method_size_ret: JavaType,
    pub method_get: JMethodID<'a>,
    pub method_get_ret: JavaType,
}
impl<'a> JavaList<'a> {
    pub const SIG_TYPE: &'static str = "java/util/List";

    pub fn new(env: &JNIEnv<'a>) -> JniResult<JavaList<'a>> {
        let class = get_global_jclass(env, Self::SIG_TYPE)?;
        Ok(JavaList {
            class,
            method_size: env.get_method_id(class, "size", "()I")?,
            method_size_ret: JavaType::Primitive(Primitive::Int),
            method_get: env.get_method_id(class, "get", "(I)Ljava/lang/Object;")?,
            method_get_ret: JavaType::Object("java/lang/Object".to_owned()),
        })
    }
}

#[allow(non_snake_case)]
pub struct JavaMap<'a> {
    pub class: JClass<'a>,
    pub method_get: JMethodID<'a>,
    pub method_get_ret: JavaType,
    pub method_put: JMethodID<'a>,
    pub method_put_ret: JavaType,
}
impl<'a> JavaMap<'a> {
    pub const SIG_TYPE: &'static str = "java/util/Map";

    pub fn new(env: &JNIEnv<'a>) -> JniResult<JavaMap<'a>> {
        let class = get_global_jclass(env, Self::SIG_TYPE)?;
        Ok(JavaMap {
            class,
            method_get: env
                .get_method_id(class, "get", "(Ljava/lang/Object;)Ljava/lang/Object;")
                .unwrap(),
            method_get_ret: JavaType::Object("java/lang/Object".to_owned()),
            method_put: env
                .get_method_id(
                    class,
                    "put",
                    "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;",
                )
                .unwrap(),
            method_put_ret: JavaType::Primitive(Primitive::Void),
        })
    }
}

#[allow(non_snake_case)]
pub struct JavaFile<'a> {
    pub class: JClass<'a>,
    pub method_getPath: JMethodID<'a>,
    pub method_getPath_ret: JavaType,
}
impl<'a> JavaFile<'a> {
    pub const SIG_TYPE: &'static str = "java/io/File";

    pub fn new(env: &JNIEnv<'a>) -> JniResult<JavaFile<'a>> {
        let class = get_global_jclass(env, Self::SIG_TYPE)?;
        Ok(JavaFile {
            class,
            method_getPath: env.get_method_id(
                class,
                "getPath",
                "()Ljava/lang/String;",
            )?,
            method_getPath_ret: JavaType::Object("java/lang/String".to_owned()),
        })
    }
}

#[allow(non_snake_case)]
pub struct JavaConsumer<'a> {
    pub class: JClass<'a>,
    pub method_accept: JMethodID<'a>,
    pub method_accept_ret: JavaType,
}
impl<'a> JavaConsumer<'a> {
    pub const SIG_TYPE: &'static str = "java/util/function/Consumer";

    pub fn new(env: &JNIEnv<'a>) -> JniResult<JavaConsumer<'a>> {
        let class = get_global_jclass(env, Self::SIG_TYPE)?;
        Ok(JavaConsumer {
            class,
            method_accept: env.get_method_id(class, "accept", "(Ljava/lang/Object;)V")?,
            method_accept_ret: JavaType::Primitive(Primitive::Void),
        })
    }
}

#[allow(non_snake_case)]
pub struct ScalaIterator<'a> {
    pub class: JClass<'a>,
    pub method_hasNext: JMethodID<'a>,
    pub method_hasNext_ret: JavaType,
    pub method_next: JMethodID<'a>,
    pub method_next_ret: JavaType,
}
impl<'a> ScalaIterator<'a> {
    pub const SIG_TYPE: &'static str = "scala/collection/Iterator";

    pub fn new(env: &JNIEnv<'a>) -> JniResult<ScalaIterator<'a>> {
        let class = get_global_jclass(env, Self::SIG_TYPE)?;
        Ok(ScalaIterator {
            class,
            method_hasNext: env.get_method_id(class, "hasNext", "()Z")?,
            method_hasNext_ret: JavaType::Primitive(Primitive::Boolean),
            method_next: env.get_method_id(class, "next", "()Ljava/lang/Object;")?,
            method_next_ret: JavaType::Object("java/lang/Object".to_owned()),
        })
    }
}

#[allow(non_snake_case)]
pub struct ScalaPromise<'a> {
    pub class: JClass<'a>,
    pub method_apply: JStaticMethodID<'a>,
    pub method_apply_ret: JavaType,
    pub method_success: JMethodID<'a>,
    pub method_success_ret: JavaType,
    pub method_failure: JMethodID<'a>,
    pub method_failure_ret: JavaType,
}
impl<'a> ScalaPromise<'a> {
    pub const SIG_TYPE: &'static str = "scala/concurrent/Promise";

    pub fn new(env: &JNIEnv<'a>) -> JniResult<ScalaPromise<'a>> {
        let class = get_global_jclass(env, Self::SIG_TYPE)?;
        Ok(ScalaPromise {
            class,
            method_apply: env.get_static_method_id(
                class,
                "apply",
                "()Lscala/concurrent/Promise;",
            )?,
            method_apply_ret: JavaType::Object(Self::SIG_TYPE.to_owned()),
            method_success: env.get_method_id(
                class,
                "success",
                "(Ljava/lang/Object;)Lscala/concurrent/Promise;",
            )?,
            method_success_ret: JavaType::Object(Self::SIG_TYPE.to_owned()),
            method_failure: env.get_method_id(
                class,
                "failure",
                "(Ljava/lang/Throwable;)Lscala/concurrent/Promise;",
            )?,
            method_failure_ret: JavaType::Object(Self::SIG_TYPE.to_owned()),
        })
    }
}

#[allow(non_snake_case)]
pub struct ScalaTuple2<'a> {
    pub class: JClass<'a>,
    pub method__1: JMethodID<'a>,
    pub method__1_ret: JavaType,
    pub method__2: JMethodID<'a>,
    pub method__2_ret: JavaType,
}
impl<'a> ScalaTuple2<'a> {
    pub const SIG_TYPE: &'static str = "scala/Tuple2";

    pub fn new(env: &JNIEnv<'a>) -> JniResult<ScalaTuple2<'a>> {
        let class = get_global_jclass(env, Self::SIG_TYPE)?;
        Ok(ScalaTuple2 {
            class,
            method__1: env.get_method_id(class, "_1", "()Ljava/lang/Object;")?,
            method__1_ret: JavaType::Object("java/lang/Object".to_owned()),
            method__2: env.get_method_id(class, "_2", "()Ljava/lang/Object;")?,
            method__2_ret: JavaType::Object("java/lang/Object".to_owned()),
        })
    }
}

#[allow(non_snake_case)]
pub struct ScalaFunction0<'a> {
    pub class: JClass<'a>,
    pub method_apply: JMethodID<'a>,
    pub method_apply_ret: JavaType,
}
impl<'a> ScalaFunction0<'a> {
    pub const SIG_TYPE: &'static str = "scala/Function0";

    pub fn new(env: &JNIEnv<'a>) -> JniResult<ScalaFunction0<'a>> {
        let class = get_global_jclass(env, Self::SIG_TYPE)?;
        Ok(ScalaFunction0 {
            class,
            method_apply: env.get_method_id(class, "apply", "()Ljava/lang/Object;")?,
            method_apply_ret: JavaType::Object("java/lang/Object".to_owned()),
        })
    }
}

#[allow(non_snake_case)]
pub struct HadoopFileSystem<'a> {
    pub class: JClass<'a>,
    pub method_getFileStatus: JMethodID<'a>,
    pub method_getFileStatus_ret: JavaType,
    pub method_open: JMethodID<'a>,
    pub method_open_ret: JavaType,
}
impl<'a> HadoopFileSystem<'a> {
    pub const SIG_TYPE: &'static str = "org/apache/hadoop/fs/FileSystem";

    pub fn new(env: &JNIEnv<'a>) -> JniResult<HadoopFileSystem<'a>> {
        let class = get_global_jclass(env, Self::SIG_TYPE)?;
        Ok(HadoopFileSystem {
            class,
            method_getFileStatus: env.get_method_id(
                class,
                "getFileStatus",
                "(Lorg/apache/hadoop/fs/Path;)Lorg/apache/hadoop/fs/FileStatus;",
            )?,
            method_getFileStatus_ret: JavaType::Object(
                HadoopFileStatus::SIG_TYPE.to_owned(),
            ),
            method_open: env.get_method_id(
                class,
                "open",
                "(Lorg/apache/hadoop/fs/Path;)Lorg/apache/hadoop/fs/FSDataInputStream;",
            )?,
            method_open_ret: JavaType::Object(
                HadoopFSDataInputStream::SIG_TYPE.to_owned(),
            ),
        })
    }
}

#[allow(non_snake_case)]
pub struct HadoopPath<'a> {
    pub class: JClass<'a>,
    pub ctor: JMethodID<'a>,
}
impl<'a> HadoopPath<'a> {
    pub const SIG_TYPE: &'static str = "org/apache/hadoop/fs/Path";

    pub fn new(env: &JNIEnv<'a>) -> JniResult<HadoopPath<'a>> {
        let class = get_global_jclass(env, Self::SIG_TYPE)?;
        Ok(HadoopPath {
            class,
            ctor: env.get_method_id(class, "<init>", "(Ljava/lang/String;)V")?,
        })
    }
}

#[allow(non_snake_case)]
pub struct HadoopFileStatus<'a> {
    pub class: JClass<'a>,
    pub method_getLen: JMethodID<'a>,
    pub method_getLen_ret: JavaType,
}
impl<'a> HadoopFileStatus<'a> {
    pub const SIG_TYPE: &'static str = "org/apache/hadoop/fs/FileStatus";

    pub fn new(env: &JNIEnv<'a>) -> JniResult<HadoopFileStatus<'a>> {
        let class = get_global_jclass(env, Self::SIG_TYPE)?;
        Ok(HadoopFileStatus {
            class,
            method_getLen: env.get_method_id(class, "getLen", "()J")?,
            method_getLen_ret: JavaType::Primitive(Primitive::Long),
        })
    }
}

#[allow(non_snake_case)]
pub struct HadoopFSDataInputStream<'a> {
    pub class: JClass<'a>,
    pub method_seek: JMethodID<'a>,
    pub method_seek_ret: JavaType,
    pub method_read: JMethodID<'a>,
    pub method_read_ret: JavaType,
    pub method_close: JMethodID<'a>,
    pub method_close_ret: JavaType,
}
impl<'a> HadoopFSDataInputStream<'a> {
    pub const SIG_TYPE: &'static str = "org/apache/hadoop/fs/FSDataInputStream";

    pub fn new(env: &JNIEnv<'a>) -> JniResult<HadoopFSDataInputStream<'a>> {
        let class = get_global_jclass(env, Self::SIG_TYPE)?;
        Ok(HadoopFSDataInputStream {
            class,
            method_seek: env.get_method_id(class, "seek", "(J)V")?,
            method_seek_ret: JavaType::Primitive(Primitive::Void),
            method_read: env.get_method_id(class, "read", "(Ljava/nio/ByteBuffer;)I")?,
            method_read_ret: JavaType::Primitive(Primitive::Int),
            method_close: env.get_method_id(class, "close", "()V")?,
            method_close_ret: JavaType::Primitive(Primitive::Void),
        })
    }
}

#[allow(non_snake_case)]
pub struct SparkManagedBuffer<'a> {
    pub class: JClass<'a>,
    pub method_nioByteBuffer: JMethodID<'a>,
    pub method_nioByteBuffer_ret: JavaType,
}
impl<'a> SparkManagedBuffer<'a> {
    pub const SIG_TYPE: &'static str = "org/apache/spark/network/buffer/ManagedBuffer";

    pub fn new(env: &JNIEnv<'a>) -> JniResult<SparkManagedBuffer<'a>> {
        let class = get_global_jclass(env, Self::SIG_TYPE)?;
        Ok(SparkManagedBuffer {
            class,
            method_nioByteBuffer: env.get_method_id(
                class,
                "nioByteBuffer",
                "()Ljava/nio/ByteBuffer;",
            )?,
            method_nioByteBuffer_ret: JavaType::Object("java/nio/ByteBuffer".to_owned()),
        })
    }
}

#[allow(non_snake_case)]
pub struct SparkSQLMetric<'a> {
    pub class: JClass<'a>,
    pub method_add: JMethodID<'a>,
    pub method_add_ret: JavaType,
}
impl<'a> SparkSQLMetric<'a> {
    pub const SIG_TYPE: &'static str = "org/apache/spark/sql/execution/metric/SQLMetric";

    pub fn new(env: &JNIEnv<'a>) -> JniResult<SparkSQLMetric<'a>> {
        let class = get_global_jclass(env, Self::SIG_TYPE)?;
        Ok(SparkSQLMetric {
            class,
            method_add: env.get_method_id(class, "add", "(J)V")?,
            method_add_ret: JavaType::Primitive(Primitive::Void),
        })
    }
}

#[allow(non_snake_case)]
pub struct SparkMetricNode<'a> {
    pub class: JClass<'a>,
    pub method_getChild: JMethodID<'a>,
    pub method_getChild_ret: JavaType,
    pub method_add: JMethodID<'a>,
    pub method_add_ret: JavaType,
}
impl<'a> SparkMetricNode<'a> {
    pub const SIG_TYPE: &'static str = "org/apache/spark/sql/blaze/MetricNode";

    pub fn new(env: &JNIEnv<'a>) -> JniResult<SparkMetricNode<'a>> {
        let class = get_global_jclass(env, Self::SIG_TYPE)?;
        Ok(SparkMetricNode {
            class,
            method_getChild: env
                .get_method_id(
                    class,
                    "getChild",
                    "(I)Lorg/apache/spark/sql/blaze/MetricNode;",
                )
                .unwrap(),
            method_getChild_ret: JavaType::Object(Self::SIG_TYPE.to_owned()),
            method_add: env
                .get_method_id(class, "add", "(Ljava/lang/String;J)V")
                .unwrap(),
            method_add_ret: JavaType::Primitive(Primitive::Void),
        })
    }
}

fn get_global_jclass<'a>(env: &JNIEnv<'a>, cls: &str) -> JniResult<JClass<'static>> {
    let local_jclass = env.find_class(cls)?;
    Ok(get_global_ref_jobject(env, local_jclass.into())?.into())
}

fn get_global_ref_jobject<'a>(
    env: &JNIEnv<'a>,
    obj: JObject<'a>,
) -> JniResult<JObject<'static>> {
    let global = env.new_global_ref::<JObject>(obj)?;

    // safety:
    //  as all global refs to jclass in JavaClasses should never be GC'd during
    // the whole jvm lifetime, we put GlobalRef into ManuallyDrop to prevent
    // deleting these global refs.
    let global_obj =
        unsafe { std::mem::transmute::<_, JObject<'static>>(global.as_obj()) };
    let _ = std::mem::ManuallyDrop::new(global);
    Ok(global_obj)
}
