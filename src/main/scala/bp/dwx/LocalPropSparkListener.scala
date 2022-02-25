package bp.dwx

import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent, SparkListenerJobStart}
import org.apache.spark.sql.execution.{QueryExecution, SQLExecution}
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd

import scala.collection.mutable
import scala.util.DynamicVariable
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import za.co.absa.spline.harvester.QueryExecutionEventHandlerFactory
import za.co.absa.spline.harvester.conf.DefaultSplineConfigurer
import za.co.absa.spline.harvester.postprocessing.PostProcessingFilter

import java.util.Properties

class LocalPropSparkListener extends SparkListener with Logging {

  val spark: SparkSession = SparkSession.builder.getOrCreate()

  private val queryExecutionEventHandlerFactory = new QueryExecutionEventHandlerFactory(spark)
  private val splineConfig = DefaultSplineConfigurer(spark)
  private val splineEventHandler =
    queryExecutionEventHandlerFactory.createEventHandler(splineConfig, isCodelessInit = false).get

  val execIdToNotebookPath: mutable.Map[String, String] = mutable.Map.empty[String, String]

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = synchronized {

    val jobProps = Map(
      "spark.databricks.job.id" -> jobStart.properties.getProperty("spark.databricks.job.id", "-1"),
      "spark.databricks.job.runId" -> jobStart.properties.getProperty("spark.databricks.job.runId", "-1"),
      "spark.databricks.job.type" -> jobStart.properties.getProperty("spark.databricks.job.type", "")
    )

    logInfo(jobProps.map({ case (key: String, value: String) => s"$key:$value" }).mkString(","))
    logInfo(jobStart.properties.getProperty(SQLExecution.EXECUTION_ID_KEY))
    val executionId = jobStart.properties.getProperty(SQLExecution.EXECUTION_ID_KEY)
    val notebookPath = jobStart.properties.getProperty("spark.databricks.notebook.path")
    if (execIdToNotebookPath contains executionId) {
      execIdToNotebookPath(executionId) = notebookPath
    } else {
      execIdToNotebookPath += (executionId -> notebookPath)
    }
  }

  override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
    case e: SparkListenerSQLExecutionEnd =>
      val notebookPath = execIdToNotebookPath(e.executionId.toString)


      def getCCParams(cc: AnyRef): Map[String, Any] =
        cc.getClass.getDeclaredFields.foldLeft(Map.empty[String, Any]) { (a, f) =>
          f.setAccessible(true)
          a + (f.getName -> f.get(cc))
        }

      val eventPropsMap = getCCParams(e)

      NotebookPathHolder.notebookPath.withValue(notebookPath) {
        spark.sqlContext.setConf("spline.notebook.path", notebookPath)
        val qe = eventPropsMap("qe").asInstanceOf[QueryExecution]
        splineEventHandler.onSuccess(notebookPath, qe, eventPropsMap("duration").asInstanceOf[Long])
      }

    case _ =>
  }
}

object NotebookPathHolder {
  val notebookPath = new DynamicVariable[String]("")
}
