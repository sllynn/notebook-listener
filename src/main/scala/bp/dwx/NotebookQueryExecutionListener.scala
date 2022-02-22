package bp.dwx

import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.QueryExecutionListener
import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.SimpleMode

class NotebookQueryExecutionListener extends QueryExecutionListener with Logging{

  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {

    val simpleExplainPlan = qe.explainString(SimpleMode)

    logInfo(s"onSuccess method called for ${funcName}")
    logInfo(s"simple explain plan: ${simpleExplainPlan}")
    logInfo(s"job duration: ${durationNs}")

  }

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {

  }
}
