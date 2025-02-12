package cn.netdiscovery.taskflow

import kotlinx.coroutines.*
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.util.concurrent.PriorityBlockingQueue

/**
 *
 * @FileName:
 *          cn.netdiscovery.taskflow.TaskScheduler
 * @author: Tony Shen
 * @date: 2024/12/10 15:30
 * @version: V1.0 <描述当前版本功能>
 */
class TaskScheduler(private val dag: DAG, private val jobScope: CoroutineScope = CoroutineScope(Dispatchers.Default)) {

    // 并发控制
    private val mutex = Mutex()

    // 执行任务
    private suspend fun execute(task: Task) {
        try {
            withTimeout(task.timeout) {
                task.status = TaskStatus.IN_PROGRESS
                task.taskAction()
                task.successCallback?.invoke()
                task.status = TaskStatus.COMPLETED
            }
        } catch (e: TimeoutCancellationException) {
            task.status = TaskStatus.TIMED_OUT
            task.failureCallback?.invoke()
            retry(task)
        } catch (e: Exception) {
            task.status = TaskStatus.FAILED
            task.failureCallback?.invoke()
            retry(task)
        }
    }

    // 重试机制
    private suspend fun retry(task: Task) {
        if (task.currentRetryCount < task.retries) {
            task.currentRetryCount++
            println("Retrying task: ${task.id}, attempt: ${task.currentRetryCount}")
            delay(task.retryDelay) // 可配置的重试间隔
            execute(task)
        } else {
            println("Task ${task.id} failed after ${task.retries} retries.")
        }
    }

    // 启动任务调度
    suspend fun start() {
        val readyTasks = PriorityBlockingQueue<Task>()

        // 初始化任务的入度，只考虑强依赖，不考虑弱依赖
        for (task in dag.tasks.values) {
            task.indegree = task.dependencies.size // 只计算强依赖
            if (task.indegree == 0) {
                readyTasks.add(task)
            }
        }

        // 持续调度任务，直到所有任务都完成
        while (readyTasks.isNotEmpty()) {
            val task = readyTasks.poll() // 获取优先级最高的任务

            // 执行任务
            if (task.weakDependencies.isEmpty() || task.weakDependencies.any { it.status == TaskStatus.COMPLETED }) {
                execute(task)

                // 完成当前任务后，更新依赖关系
                mutex.withLock {
                    for (dependentTask in task.dependents) {
                        dependentTask.indegree--
                        if (dependentTask.indegree == 0) {
                            readyTasks.add(dependentTask)
                        }
                    }
                }
            } else {
                // 如果有弱依赖未完成，将任务推迟
                mutex.withLock {
                    readyTasks.add(task)
                }
            }
        }

        println("All tasks have been executed.")
    }
}