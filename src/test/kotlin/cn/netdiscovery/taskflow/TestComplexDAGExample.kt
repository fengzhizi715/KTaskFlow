package cn.netdiscovery.taskflow

import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking

/**
 *
 * @FileName:
 *          cn.netdiscovery.taskflow.TestComplexDAGExample
 * @author: Tony Shen
 * @date: 2025/8/9 21:19
 * @version: V1.0 <描述当前版本功能>
 */
suspend fun testComplexDAGExample() {
    val dag = DAG().apply {
        val taskA = task("A", "Load Config", 1, TaskType.IO, SmartGenericTaskAction<Unit, String> {
            println("Task A running")
            delay(300)
            "config done"
        })

        val taskB = task("B", "Initialize DB", 1, TaskType.CPU, SmartGenericTaskAction<String, String> {
            println("Task B received input: $it")
            delay(500)
            "db ready"
        })

        val taskC = task("C", "Start Services", 1, TaskType.IO, SmartGenericTaskAction<String, String> {
            println("Task C received input: $it")
            delay(400)
            "services started"
        })

        val taskD = task("D", "Prepare Cache", 1, TaskType.CPU, SmartGenericTaskAction<Unit, String> {
            println("Task D running")
            delay(350)
            "cache ready"
        })

        val taskE = task("E", "Load Metrics", 1, TaskType.IO, SmartGenericTaskAction<Unit, String> {
            println("Task E running (slow)")
            delay(1500)  // 慢任务，测试弱依赖超时
            "metrics loaded"
        })

        val taskF = task("F", "Finalize Startup", 1, TaskType.CPU, SmartGenericTaskAction<List<String>, String> {
            println("Task F received inputs: $it")
            delay(300)
            "startup finalized"
        })

        val taskG = task("G", "Post Start Cleanup", 1, TaskType.IO, SmartGenericTaskAction<String, String> {
            println("Task G running after weak dep")
            delay(200)
            "cleanup done"
        })

        // 依赖关系
        taskB.dependsOn(taskA)  // A -> B
        taskC.dependsOn(taskB)  // B -> C
        taskF.dependsOn(taskB, taskD)  // F 依赖 B 和 D
        taskG.dependsOn(taskF)  // G 依赖 F

        // 并行无依赖 D, E
        // 弱依赖 E
        taskG.weakDependsOn(taskE)
        taskG.timeout = 1000L  // 超过1秒弱依赖超时继续执行
    }

    val scheduler = TaskScheduler(dag)
    scheduler.start()

    println(generateDotFile(dag))
}

fun main() = runBlocking {
    testComplexDAGExample()
}