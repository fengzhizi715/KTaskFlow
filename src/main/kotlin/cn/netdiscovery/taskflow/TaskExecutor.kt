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
    suspend fun execute(task: Task) {
        try {
            withTimeout(task.timeout) {
                task.status = TaskStatus.IN_PROGRESS

                // 始终获取所有强依赖的输出，并作为当前任务的输入列表
                val inputs = task.dependencies.values.map { it.output }

                // 安全地调用任务动作，传递输入列表
                task.output = task.taskAction.execute(inputs)

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