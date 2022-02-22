package bp.dwx

import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent, SparkListenerJobEnd, SparkListenerJobStart}
import org.apache.spark.sql.execution.{QueryExecution, SQLExecution}
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd

import scala.collection.mutable
import scala.util.DynamicVariable
import org.apache.spark.internal.Logging

class LocalPropSparkListener extends SparkListener with Logging {

  val notebookQueryExecutionListener = new NotebookQueryExecutionListener

  def getCCParams(cc: AnyRef): Map[String, Any] =
    cc.getClass.getDeclaredFields.foldLeft(Map.empty[String, Any]) { (a, f) =>
      f.setAccessible(true)
      a + (f.getName -> f.get(cc))
    }

  val execIdToNotebookPath: mutable.Map[Long, String] = mutable.Map.empty[Long, String]

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = synchronized {
    val executionId = jobStart.properties.getProperty(SQLExecution.EXECUTION_ID_KEY).toLong
    val notebookPath = jobStart.properties.getProperty("spark.databricks.notebook.path")
    if (execIdToNotebookPath contains executionId) {
      execIdToNotebookPath(executionId) = notebookPath
    } else {
      execIdToNotebookPath += (executionId -> notebookPath)
    }
  }

  override def onOtherEvent(event: SparkListenerEvent): Unit = event match {
    case e: SparkListenerSQLExecutionEnd =>
      val notebookPath = execIdToNotebookPath(e.executionId)
      val eventPropsMap = getCCParams(e)

      NotebookPathHolder.notebookPath.withValue(notebookPath) {
        notebookQueryExecutionListener.onSuccess(notebookPath, eventPropsMap("qe").asInstanceOf[QueryExecution], eventPropsMap("duration").asInstanceOf[Long])
      }

    case _ =>
  }
}

object NotebookPathHolder {
  val notebookPath = new DynamicVariable[String]("")
}
