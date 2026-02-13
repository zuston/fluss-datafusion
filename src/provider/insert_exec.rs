use std::any::Any;
use std::fmt::{self, Debug, Formatter};
use std::sync::Arc;

use arrow::array::{RecordBatch, UInt64Array};
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
use datafusion::error::Result;
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::memory::MemoryStream;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use fluss::client::FlussConnection;
use fluss::metadata::TableInfo;

use super::upsert_batches;

pub struct FlussInsertExec {
    conn: Arc<FlussConnection>,
    table_info: TableInfo,
    input: Arc<dyn ExecutionPlan>,
    props: PlanProperties,
}

impl FlussInsertExec {
    pub fn new(
        conn: Arc<FlussConnection>,
        table_info: TableInfo,
        input: Arc<dyn ExecutionPlan>,
    ) -> Self {
        let count_schema = Arc::new(Schema::new(vec![Field::new(
            "count",
            ArrowDataType::UInt64,
            false,
        )]));
        let props = PlanProperties::new(
            EquivalenceProperties::new(count_schema),
            datafusion::physical_plan::Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        );
        Self {
            conn,
            table_info,
            input,
            props,
        }
    }
}

impl Debug for FlussInsertExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "FlussInsertExec: table={}", self.table_info.table_path)
    }
}

impl DisplayAs for FlussInsertExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        write!(f, "FlussInsertExec: table={}", self.table_info.table_path)
    }
}

impl ExecutionPlan for FlussInsertExec {
    fn name(&self) -> &'static str {
        "FlussInsertExec"
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn properties(&self) -> &PlanProperties {
        &self.props
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }
    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(FlussInsertExec::new(
            self.conn.clone(),
            self.table_info.clone(),
            children.remove(0),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let conn = self.conn.clone();
        let table_info = self.table_info.clone();
        let input = self.input.clone();
        let count_schema = self.props.equivalence_properties().schema().clone();

        let total_rows: u64 = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async move {
                match upsert_batches(&conn, &table_info, input, partition, context).await {
                    Ok(total) => total,
                    Err(e) => {
                        log::error!(
                            "FlussInsertExec failed: table={}, partition={}, error={:?}",
                            table_info.table_path,
                            partition,
                            e
                        );
                        0
                    }
                }
            })
        });

        let count_batch = RecordBatch::try_new(
            count_schema.clone(),
            vec![Arc::new(UInt64Array::from(vec![total_rows]))],
        )?;
        Ok(Box::pin(MemoryStream::try_new(
            vec![count_batch],
            count_schema,
            None,
        )?))
    }
}
