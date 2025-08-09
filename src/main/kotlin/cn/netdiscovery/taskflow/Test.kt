package cn.netdiscovery.taskflow

import kotlinx.coroutines.*

/**
 *
 * @FileName:
 *          cn.netdiscovery.taskflow.Test
 * @author: Tony Shen
 * @date: 2024/8/20 15:44
 * @version: V1.0 <描述当前版本功能>
 */
fun main() = runBlocking {

    val dag = dag {
        val task1 = task("1", "Load Config", 1, TaskType.IO,
            action= SmartGenericTaskAction<Unit, String> {
                println("Task 1 running...")
                delay(500)
                "config_loaded"
            }
        )

        val task2 = task("2", "Init Service", 1, TaskType.CPU,
            SmartGenericTaskAction<String, String> {
                println("Task 2 received input: ${it}")
                delay(1000)
                "service_ready"
            }
        )

        task2.dependsOn(task1)
    }

    val scheduler = TaskScheduler(dag)
    scheduler.start()

    println(generateDotFile(dag))
}

