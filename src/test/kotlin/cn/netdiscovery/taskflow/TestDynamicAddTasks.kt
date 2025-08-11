package cn.netdiscovery.taskflow

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking

/**
 *
 * @FileName:
 *          cn.netdiscovery.taskflow.TestDynamicAddTasks
 * @author: Tony Shen
 * @date: 2025/8/9 21:42
 * @version: V1.0 <描述当前版本功能>
 */
suspend fun testDynamicAddTasks() {
    val dag = DAG().apply {
        val task1 = task("1", "Task 1", 1, TaskType.IO, SmartGenericTaskAction<Unit, String> {
            println("Task 1 running...")
            delay(500)
            "result1"
        })
        val task2 = task("2", "Task 2", 1, TaskType.CPU, SmartGenericTaskAction<String, String> {
            println("Task 2 received: $it")
            delay(700)
            "result2"
        })
        task2.dependsOn(task1)
    }

    val scheduler = TaskScheduler(dag)
    scheduler.startAsync()

    // 等待 task2 运行中
    delay(600)

    println("动态添加 task3，依赖 task2")

    // 新增任务 task3，依赖 task2
    val task3 = Task(
        id = "3",
        taskName = "Task 3",
        priority = 1,
        type = TaskType.IO,
        taskAction = SmartGenericTaskAction<String, String> {
            println("Task 3 received: $it")
            delay(400)
            "result3"
        }
    )

    // 添加任务和依赖
    task3.dependsOn(dag.getTaskById("2")!!)
    dag.addTask(task3)

    // 通知调度器任务准备就绪
    scheduler.addAndSchedule(task3)

    val r3 = dag.getTaskResultAsync("3")
    println("Task 3 result: ${r3.value}")

    scheduler.shutdown()

    println(generateDotFile(dag))
}

fun main() = runBlocking {
    testDynamicAddTasks()
}