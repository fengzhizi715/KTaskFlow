package cn.netdiscovery.taskflow

import kotlinx.coroutines.*
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlin.coroutines.cancellation.CancellationException

/**
 *
 * @FileName:
 *          cn.netdiscovery.taskflow.TaskExecutor
 * @author: Tony Shen
 * @date: 2025/8/8 10:18
 * @version: V1.0 <描述当前版本功能>
 */
class TaskExecutor(private val mutex: Mutex,
                   private val scope: CoroutineScope
) {

    suspend fun execute(task: Task, input: Any?) {
        println("[Executor] start execute task=${task.id}, input=$input")

        if (task.isCancelled) {
            println("[Executor] Task ${task.id} was cancelled before execution.")
            return
        }

        val timeout = if (task.executionTimeout > 0L) task.executionTimeout else Long.MAX_VALUE

        try {
            withTimeout(timeout) {
                mutex.withLock { task.status = TaskStatus.IN_PROGRESS }

                val result = try {
                    if (task.taskAction is SmartGenericTaskAction<*, *>) {
                        @Suppress("UNCHECKED_CAST")
                        (task.taskAction as SmartGenericTaskAction<Any?, Any?>).execute(input)
                    } else {
                        task.taskAction.execute(input)
                    }
                } catch (e: Throwable) {
                    throw e
                }

                mutex.withLock {
                    task.markCompleted(TaskResult(success = true, value = result))
                }

                println("[Executor] task ${task.id} completed with value=$result")
                task.successCallback?.invoke()
            }
        } catch (e: TimeoutCancellationException) {
            println("[Executor] task ${task.id} execution timed out: ${e.message}")
            handleFailure(task, e, input, TaskStatus.TIMED_OUT)
        } catch (e: CancellationException) {
            println("[Executor] task ${task.id} cancelled during execution.")
            mutex.withLock { task.markFailed(e) }
        } catch (e: Exception) {
            println("[Executor] task ${task.id} failed: ${e.message}")
            handleFailure(task, e, input, TaskStatus.FAILED)
        }
    }

    private fun handleFailure(task: Task, e: Throwable, input: Any?, status: TaskStatus) {
        scope.launch {
            mutex.withLock {
                // 重要：只记录错误但不标记为最终失败
                task.output = TaskResult(success = false, error = e)
                task.status = TaskStatus.FAILED // 临时状态，不是最终状态
            }

            if (task.currentRetryCount >= task.retries) {
                // 最终失败处理
                mutex.withLock {
                    task.markFailed(e) // 这会标记任务为最终失败
                }
                task.failureCallback?.invoke()
                task.rollbackAction?.invoke()
                println("[Executor] Task ${task.id} failed after ${task.retries} retries.")
            } else {
                // 重置状态以便重试
                mutex.withLock {
                    task.resetForRetry() // 使用新方法重置状态
                }
                retry(task, input)
            }
        }
    }

    private suspend fun retry(task: Task, input: Any?) {
        if (!task.isCancelled) {
            task.currentRetryCount++
            println("[Executor] Retrying task: ${task.id}, attempt: ${task.currentRetryCount}")
            delay(task.retryDelay)

            // 重要：通知调度器任务可以重新入队
            mutex.withLock {
                task.status = TaskStatus.NOT_STARTED
                task.indegree = 0 // 确保调度器会重新入队
            }

            // 重新执行任务
            execute(task, input)
        }
    }
}