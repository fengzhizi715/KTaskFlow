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
//    val dag = dag {
//        val task1 = task("1", "Task 1", 1) {
//            LoggerProxy.getLogger().i("${System.currentTimeMillis()}, Task 1 start!")
//            Thread.sleep(1000)
//        }
//        val task2 = task("2", "Task 2", 3) {
//            LoggerProxy.getLogger().i("${System.currentTimeMillis()}, Task 2 start!")
//            Thread.sleep(2000)
//        }
//        val task3 = task("3", "Task 3", 2) {
//            LoggerProxy.getLogger().i("${System.currentTimeMillis()}, Task 3 start!")
//            Thread.sleep(1500)
//        }
//        val task4 = task("4", "Task 4", type = TaskType.CPU) {
//            LoggerProxy.getLogger().i("${System.currentTimeMillis()}, Task 4 start!")
//            Thread.sleep(2000)
//        }
//        val task5 = task("5", "Task 5", type = TaskType.CPU) {
//            LoggerProxy.getLogger().i("${System.currentTimeMillis()}, Task 5 start!")
//            Thread.sleep(1000)
//        }
//
//        task3.dependsOn(task1,task2)
//
//        // 弱依赖
//        task4.weakDependsOn(task1,task2)
//
//        task5.dependsOn(task3,task4)
//
//        task1.successCallback = { LoggerProxy.getLogger().i("${System.currentTimeMillis()}, Task 1 completed!") }
//        task2.successCallback = { LoggerProxy.getLogger().i("${System.currentTimeMillis()}, Task 2 completed!") }
//        task3.successCallback = { LoggerProxy.getLogger().i("${System.currentTimeMillis()}, Task 3 completed!") }
//        task4.successCallback = { LoggerProxy.getLogger().i("${System.currentTimeMillis()}, Task 4 completed!") }
//        task5.successCallback = { LoggerProxy.getLogger().i("${System.currentTimeMillis()}, Task 5 completed!") }
//    }
//
//    println(generateDotFile(dag))
//
//    // 创建 TaskScheduler 并启动任务
//    val scheduler = TaskScheduler(dag)
//    scheduler.start()
}

