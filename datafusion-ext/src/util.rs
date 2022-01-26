use datafusion::error::DataFusionError;
use datafusion::error::GenericError;
use datafusion::error::Result as DataFusionResult;

pub struct Util;

impl Util {
    pub fn to_datafusion_external_result<T>(
        result: Result<T, impl std::error::Error>,
    ) -> DataFusionResult<T> {
        result
            .map_err(|e| DataFusionError::External(GenericError::from(format!("{}", e))))
    }
}
