use std::any::Any;
use std::fmt::{self, Debug, Formatter};
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
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

use super::scan_table;

#[derive(Clone)]
pub struct FlussScanExec {
    conn: Arc<FlussConnection>,
    table_info: TableInfo,
    projection: Option<Vec<usize>>,
    limit: Option<usize>,
    props: PlanProperties,
}

impl FlussScanExec {
    pub fn new(
        conn: Arc<FlussConnection>,
        table_info: TableInfo,
        arrow_schema: SchemaRef,
        projection: Option<Vec<usize>>,
        limit: Option<usize>,
    ) -> Self {
        let projected_schema = match &projection {
            Some(indices) => Arc::new(arrow_schema.project(indices).unwrap()),
            None => arrow_schema,
        };
        let props = PlanProperties::new(
            EquivalenceProperties::new(projected_schema),
            datafusion::physical_plan::Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            conn,
            table_info,
            projection,
            limit,
            props,
        }
    }

    fn projected_schema(&self) -> SchemaRef {
        self.props.equivalence_properties().schema().clone()
    }
}

impl Debug for FlussScanExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("FlussScanExec")
            .field("table", &self.table_info.table_path.to_string())
            .field("projection", &self.projection)
            .field("limit", &self.limit)
            .finish()
    }
}

impl DisplayAs for FlussScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "FlussScanExec: table={}, limit={:?}",
            self.table_info.table_path, self.limit
        )
    }
}

impl ExecutionPlan for FlussScanExec {
    fn name(&self) -> &'static str {
        "FlussScanExec"
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn properties(&self) -> &PlanProperties {
        &self.props
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }
    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let conn = self.conn.clone();
        let table_info = self.table_info.clone();
        let projection = self.projection.clone();
        let limit = self.limit;
        let projected_schema = self.projected_schema();

        let batches: Vec<RecordBatch> = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async move {
                scan_table(&conn, &table_info, projection.as_deref(), limit)
                    .await
                    .unwrap_or_default()
            })
        });

        Ok(Box::pin(MemoryStream::try_new(
            batches,
            projected_schema,
            None,
        )?))
    }
}
