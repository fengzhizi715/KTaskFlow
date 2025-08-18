package cn.netdiscovery.taskflow

import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking

/**
 *
 * @FileName:
 *          cn.netdiscovery.taskflow.TestTaskEnableCache
 * @author: Tony Shen
 * @date: 2025/8/18 14:15
 * @version: V1.0 <描述当前版本功能>
 */
suspend fun testTaskEnableCacheExample() {

    val dag = DAG().apply {

        val t1 = task("1", "Fetch User", 1, TaskType.IO,
            SmartGenericTaskAction(Unit::class.java) {
                delay(200)
                "user:42"
            }
        ).apply {
            enableCache(key = "fetch_user_42", ttlMillis = 5_000, policy = CachePolicy.READ_WRITE)
        }

        delay(250)

        val t2 = task("2", "Fetch User2", 1, TaskType.IO,
            SmartGenericTaskAction(Unit::class.java) {
                delay(1000)
                "user:43"
            }
        ).apply {
            enableCache(key = "fetch_user_42", ttlMillis = 5_000, policy = CachePolicy.READ_WRITE)
        }

    }

    val scheduler = TaskScheduler(dag)
    scheduler.startAsync()

    val result = dag.getTaskResultAsync("2")
    println(result.value)
}

fun main() = runBlocking {
    testTaskEnableCacheExample()
}