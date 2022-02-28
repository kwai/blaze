use datafusion::error::DataFusionError;
use datafusion::error::Result as DFResult;
use jni::errors::Error;

pub type Result<T> = std::result::Result<T, BlazeError>;

#[derive(Debug)]
pub enum BlazeError {
    JNIError(jni::errors::Error),
}

impl From<jni::errors::Error> for BlazeError {
    fn from(e: Error) -> Self {
        BlazeError::JNIError(e)
    }
}

impl From<BlazeError> for DataFusionError {
    fn from(e: BlazeError) -> Self {
        DataFusionError::Execution(format!("{:?}", e))
    }
}

pub trait ResultExt<T> {
    fn to_df(self) -> DFResult<T>;
}

impl<T> ResultExt<T> for Result<T> {
    fn to_df(self) -> DFResult<T> {
        match self {
            Ok(x) => Ok(x),
            Err(e) => Err(e.into()),
        }
    }
}
