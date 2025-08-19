package cn.netdiscovery.taskflow

import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.delay

/**
 *
 * @FileName:
 *          cn.netdiscovery.taskflow.TestDSL
 * @author: Tony Shen
 * @date: 2025/8/18 15:33
 * @version: V1.0 <描述当前版本功能>
 */

suspend fun testDslExample() {

    val myDag = dag {
        val t1 = defineTask("1") {
            name = "Fetch User"
            type = TaskType.IO
            action {
                delay(200)
                "user:42"
            }
            cache("fetch_user_42", ttlMillis = 5_000, policy = CachePolicy.READ_WRITE)
        }

        val t2 = defineTask("2") {
            name = "Process User"
            type = TaskType.CPU
            priority = 1
            // 依赖 t1
            this dependsOn t1
            action(String::class.java) { input ->
                "processed:$input"
            }
        }
    }

    val scheduler = TaskScheduler(myDag)
    scheduler.startAsync()

    val r = myDag.getTaskResultAsync("2")
    println("Final result: ${r.value}")
}

fun main() = runBlocking {
    testDslExample()
}