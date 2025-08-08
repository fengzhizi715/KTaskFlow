package cn.netdiscovery.taskflow

import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.delay
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withTimeout

/**
 *
 * @FileName:
 *          cn.netdiscovery.taskflow.TaskExecutor
 * @author: Tony Shen
 * @date: 2025/8/8 10:18
 * @version: V1.0 <描述当前版本功能>
 */
class TaskExecutor(private val mutex: Mutex) {

    // 执行任务，包含超时和重试逻辑
    suspend fun execute(task: Task) {
        try {
            withTimeout(task.timeout) {
                mutex.withLock {
                    task.status = TaskStatus.IN_PROGRESS
                }

                val inputs = task.dependencies.values.map { it.output }
                task.output = task.taskAction.execute(inputs)

                mutex.withLock {
                    task.status = TaskStatus.COMPLETED
                }

                task.successCallback?.invoke()
            }
        } catch (e: TimeoutCancellationException) {
            mutex.withLock {
                task.status = TaskStatus.TIMED_OUT
            }
            task.failureCallback?.invoke()
            retry(task)
        } catch (e: Exception) {
            mutex.withLock {
                task.status = TaskStatus.FAILED
            }
            task.failureCallback?.invoke()
            retry(task)
        }
    }

    private suspend fun retry(task: Task) {
        if (task.currentRetryCount < task.retries) {
            task.currentRetryCount++
            println("Retrying task: ${task.id}, attempt: ${task.currentRetryCount}")
            delay(task.retryDelay)
            execute(task)
        } else {
            println("Task ${task.id} failed after ${task.retries} retries.")
        }
    }
}