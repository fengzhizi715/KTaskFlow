package cn.netdiscovery.taskflow

import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking

/**
 *
 * @FileName:
 *          cn.netdiscovery.taskflow.TestParallelTasks
 * @author: Tony Shen
 * @date: 2025/8/9 20:29
 * @version: V1.0 <描述当前版本功能>
 */
suspend fun testParallelTasks() {
    val dag = DAG().apply {
        repeat(3) { i ->
            task("$i", "Parallel Task $i", 1, TaskType.IO,
                SmartGenericTaskAction(Unit::class.java) {
                    println("Parallel task $i running")
                    delay(300)
                    "result $i"
                }
            )
        }
    }

    val scheduler = TaskScheduler(dag)
    scheduler.startAsync()

    val result = dag.getTaskResultAsync("2")
    println(result.value)
}

fun main() = runBlocking {
    testParallelTasks()
}