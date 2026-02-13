use std::any::Any;
use std::fmt::{self, Debug, Formatter};
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use datafusion::common::ScalarValue;
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

use crate::error::fluss_err;

use super::lookup_pk_row;

#[derive(Clone)]
pub struct FlussLookupExec {
    conn: Arc<FlussConnection>,
    table_info: TableInfo,
    projection: Option<Vec<usize>>,
    pk_index: usize,
    pk_literal: ScalarValue,
    props: PlanProperties,
}

impl FlussLookupExec {
    pub fn new(
        conn: Arc<FlussConnection>,
        table_info: TableInfo,
        arrow_schema: SchemaRef,
        projection: Option<Vec<usize>>,
        pk_index: usize,
        pk_literal: ScalarValue,
    ) -> Self {
        let projected_schema = match &projection {
            Some(indices) => Arc::new(arrow_schema.project(indices).unwrap()),
            None => arrow_schema,
        };
        let props = PlanProperties::new(
            EquivalenceProperties::new(projected_schema),
            datafusion::physical_plan::Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        );
        Self {
            conn,
            table_info,
            projection,
            pk_index,
            pk_literal,
            props,
        }
    }

    fn projected_schema(&self) -> SchemaRef {
        self.props.equivalence_properties().schema().clone()
    }
}

impl Debug for FlussLookupExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("FlussLookupExec")
            .field("table", &self.table_info.table_path.to_string())
            .field("projection", &self.projection)
            .finish()
    }
}

impl DisplayAs for FlussLookupExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        write!(f, "FlussLookupExec: table={}", self.table_info.table_path)
    }
}

impl ExecutionPlan for FlussLookupExec {
    fn name(&self) -> &'static str {
        "FlussLookupExec"
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
        let pk_index = self.pk_index;
        let pk_literal = self.pk_literal.clone();
        let projected_schema = self.projected_schema();

        let batches_res: std::result::Result<Vec<RecordBatch>, fluss::error::Error> =
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(async move {
                    lookup_pk_row(
                        &conn,
                        &table_info,
                        projection.as_deref(),
                        pk_index,
                        &pk_literal,
                    )
                    .await
                })
            });
        let batches = batches_res.map_err(fluss_err)?;

        Ok(Box::pin(MemoryStream::try_new(
            batches,
            projected_schema,
            None,
        )?))
    }
}
