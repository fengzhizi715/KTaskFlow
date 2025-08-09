package cn.netdiscovery.taskflow

import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking

/**
 *
 * @FileName:
 *          cn.netdiscovery.taskflow.TestMultiDependencyInput
 * @author: Tony Shen
 * @date: 2025/8/9 20:30
 * @version: V1.0 <描述当前版本功能>
 */
suspend fun testMultiDependencyInput() {
    val dag = DAG().apply {
        val t1 = task("1", "Task 1", 1, TaskType.IO,
            SmartGenericTaskAction<Unit, String> {
                println("Task 1 running")
                delay(300)
                "output1"
            }
        )
        val t2 = task("2", "Task 2", 1, TaskType.IO,
            SmartGenericTaskAction<Unit, String> {
                println("Task 2 running")
                delay(300)
                "output2"
            }
        )
        val t3 = task("3", "Task 3", 1, TaskType.CPU,
            SmartGenericTaskAction<List<String>, String> {
                println("Task 3 received inputs: $it")
                delay(300)
                it.joinToString(", ")
            }
        )

        t3.dependsOn(t1, t2)
    }

    val scheduler = TaskScheduler(dag)
    scheduler.start()
}

fun main() = runBlocking {
    testMultiDependencyInput()
}
