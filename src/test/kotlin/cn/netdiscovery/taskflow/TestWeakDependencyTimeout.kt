package cn.netdiscovery.taskflow

import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking

/**
 *
 * @FileName:
 *          cn.netdiscovery.taskflow.TestWeakDependencyTimeout
 * @author: Tony Shen
 * @date: 2025/8/9 20:31
 * @version: V1.0 <描述当前版本功能>
 */
suspend fun testWeakDependencyTimeout() {
    val dag = DAG().apply {
        val t1 = task("1", "Strong Dependency", 1, TaskType.IO,
            SmartGenericTaskAction<Unit, String> {
                println("Strong dep running")
                delay(300)
                "strong done"
            }
        )
        val t2 = task("2", "Weak Dependency", 1, TaskType.IO,
            SmartGenericTaskAction<Unit, String> {
                println("Weak dep running (will delay long)")
                delay(3000) // 故意长延迟，触发弱依赖超时
                "weak done"
            }
        )
        val t3 = task("3", "Dependent Task", 1, TaskType.CPU,
            SmartGenericTaskAction<String, String> {
                println("Dependent task running after strong dep")
                delay(300)
                it
            }
        )

        t3.dependsOn(t1)
        t3.weakDependsOn(t2)
        t3.timeout = 1000  // 设置弱依赖超时时间1秒
    }

    val scheduler = TaskScheduler(dag)
    scheduler.start()

    val value = dag.getTaskResultAsync("3").value
    println(value)
}

fun main() = runBlocking {
    testWeakDependencyTimeout()
}