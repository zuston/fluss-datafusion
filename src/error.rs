use datafusion::error::DataFusionError;

/// Convert a fluss error into a DataFusion error.
pub fn fluss_err(e: fluss::error::Error) -> DataFusionError {
    DataFusionError::External(Box::new(e))
}
