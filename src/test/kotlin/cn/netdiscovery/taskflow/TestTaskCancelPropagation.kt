package cn.netdiscovery.taskflow

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking

/**
 *
 * @FileName:
 *          cn.netdiscovery.taskflow.TestTaskCancelPropagation
 * @author: Tony Shen
 * @date: 2025/8/9 21:10
 * @version: V1.0 <描述当前版本功能>
 */
suspend fun testTaskCancelPropagation() {
    val dag = DAG().apply {
        val t1 = task("1", "Task 1", 1, TaskType.IO,
            SmartGenericTaskAction<Unit, String> {
                println("Task 1 running...")
                delay(1000)
                "done"
            }
        )
        val t2 = task("2", "Task 2", 1, TaskType.CPU,
            SmartGenericTaskAction<String, String> {
                println("Task 2 received: $it")
                delay(1000)
                "done"
            }
        )
        t2.dependsOn(t1)
    }

    val scheduler = TaskScheduler(dag)
    scheduler.startAsync()

    // 取消任务1，应该导致任务2无法运行
    delay(300)
    scheduler.cancelTask("1")
}

fun main() = runBlocking {
    testTaskCancelPropagation()
}