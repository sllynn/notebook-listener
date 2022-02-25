package bp.dwx

import org.apache.commons.configuration.Configuration
import org.apache.spark.internal.Logging
import za.co.absa.spline.harvester.HarvestingContext
import za.co.absa.spline.harvester.postprocessing.PostProcessingFilter
import za.co.absa.spline.producer.model.v1_1.{DataOperation, ExecutionEvent, ExecutionPlan, ReadOperation, WriteOperation}
import za.co.absa.spline.harvester.ExtraMetadataImplicits._


class PostProcessorFilter(configuration: Configuration) extends PostProcessingFilter with Logging {
  override def processExecutionEvent(event: ExecutionEvent, ctx: HarvestingContext): ExecutionEvent = event

  override def processExecutionPlan(plan: ExecutionPlan, ctx: HarvestingContext): ExecutionPlan = {
    val notebookPath: String = ctx.session.sqlContext.getConf("spline.notebook.path")
    plan.withAddedExtra(Map("spline.notebook.path" -> notebookPath))
  }

  override def processReadOperation(op: ReadOperation, ctx: HarvestingContext): ReadOperation = op

  override def processWriteOperation(op: WriteOperation, ctx: HarvestingContext): WriteOperation = op

  override def processDataOperation(op: DataOperation, ctx: HarvestingContext): DataOperation = op
}
