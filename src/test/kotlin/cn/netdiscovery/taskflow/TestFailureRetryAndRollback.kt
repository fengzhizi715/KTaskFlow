package cn.netdiscovery.taskflow

import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking

/**
 *
 * @FileName:
 *          cn.netdiscovery.taskflow.TestFailureRetryAndRollback
 * @author: Tony Shen
 * @date: 2025/8/9 21:05
 * @version: V1.0 <描述当前版本功能>
 */
suspend fun testFailureRetryAndRollback() {
    val dag = DAG().apply {
        val t1 = task("1", "Flaky Task", 1, TaskType.IO,
            SmartGenericTaskAction<Unit, String> {
                println("Attempting flaky task")
                delay(300)
                if (Math.random() < 0.7) throw RuntimeException("Random failure")
                "success"
            }
        )
        t1.retries = 3
        t1.retryDelay = 500
        t1.failureCallback = { println("Failure callback triggered") }
        t1.rollbackAction = { println("Rollback triggered") }
    }

    val scheduler = TaskScheduler(dag)
    scheduler.startAsync()

    val value = dag.getTaskResultAsync("1").value
    println(value)
}

fun main() = runBlocking {
    testFailureRetryAndRollback()
}