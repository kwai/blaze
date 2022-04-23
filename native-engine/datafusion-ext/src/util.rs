use datafusion::error::DataFusionError;
use datafusion::error::Result as DataFusionResult;

pub struct Util;

impl Util {
    pub fn to_datafusion_external_result<T>(
        result: Result<T, impl std::error::Error>,
    ) -> DataFusionResult<T> {
        result.map_err(|e| DataFusionError::Internal(format!("{}", e)))
    }
}
