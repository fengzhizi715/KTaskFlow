package cn.netdiscovery.taskflow

import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking

/**
 *
 * @FileName:
 *          cn.netdiscovery.taskflow.TestSerialTasks
 * @author: Tony Shen
 * @date: 2025/8/9 20:28
 * @version: V1.0 <描述当前版本功能>
 */
suspend fun testSerialTasks() {
    val dag = DAG().apply {
        val t1 = task("1", "Load Config", 1, TaskType.IO,
            SmartGenericTaskAction<Unit, String> {
                println("Task 1 running")
                delay(500)
                "config"
            }
        )

        val t2 = task("2", "Init Service", 1, TaskType.CPU,
            SmartGenericTaskAction<String, String> {
                println("Task 2 received input: $it")
                delay(500)
                "service ready"
            }
        )

        t2.dependsOn(t1)
    }

    val scheduler = TaskScheduler(dag)
    scheduler.start()
}

fun main() = runBlocking {
    testSerialTasks()
}
