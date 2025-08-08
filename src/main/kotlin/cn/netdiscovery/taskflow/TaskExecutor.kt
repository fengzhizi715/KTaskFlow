package cn.netdiscovery.taskflow

import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.delay
import kotlinx.coroutines.withTimeout

/**
 *
 * @FileName:
 *          cn.netdiscovery.taskflow.TaskExecutor
 * @author: Tony Shen
 * @date: 2025/8/8 10:18
 * @version: V1.0 <描述当前版本功能>
 */
class TaskExecutor {

    // 执行任务，包含超时和重试逻辑
    suspend fun execute(task: Task<*, *>) {
        try {
            withTimeout(task.timeout) {
                task.status = TaskStatus.IN_PROGRESS
                // 由于 taskAction 是泛型，这里需要进行类型转换
                @Suppress("UNCHECKED_CAST")
                val action = task.taskAction as (Any?) -> Any?
                task.output = action(task.input) as Nothing?
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
    private suspend fun retry(task: Task<*, *>) {
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