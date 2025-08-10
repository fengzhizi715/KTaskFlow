package cn.netdiscovery.taskflow

import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.delay
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withTimeout
import kotlin.coroutines.cancellation.CancellationException

/**
 *
 * @FileName:
 *          cn.netdiscovery.taskflow.TaskExecutor
 * @author: Tony Shen
 * @date: 2025/8/8 10:18
 * @version: V1.0 <描述当前版本功能>
 */
class TaskExecutor(private val mutex: Mutex) {

    suspend fun execute(task: Task, input: Any?) {
        println("[Executor] start execute task=${task.id}, input=$input")
        try {
            if (task.isCancelled) {
                println("[Executor] Task ${task.id} was cancelled before execution.")
                return
            }

            val timeout = if (task.executionTimeout > 0L) task.executionTimeout else Long.MAX_VALUE

            withTimeout(timeout) {
                mutex.withLock {
                    task.status = TaskStatus.IN_PROGRESS
                }

                // 执行任务动作
                val result = try {
                    if (task.taskAction is SmartGenericTaskAction<*, *>) {
                        @Suppress("UNCHECKED_CAST")
                        (task.taskAction as SmartGenericTaskAction<Any?, Any?>).execute(input)
                    } else {
                        task.taskAction.execute(input)
                    }
                } catch (e: Throwable) {
                    // action 抛错
                    throw e
                }

                mutex.withLock {
                    task.markCompleted(TaskResult(success = true, value = result))
                    task.status = TaskStatus.COMPLETED
                }

                println("[Executor] task ${task.id} completed with value=$result")
                task.successCallback?.invoke()
            }
        } catch (e: TimeoutCancellationException) {
            println("[Executor] task ${task.id} execution timed out: ${e.message}")
            handleFailure(task, e, TaskStatus.TIMED_OUT)
        } catch (e: CancellationException) {
            println("[Executor] task ${task.id} cancelled during execution.")
            // propagate cancellation, don't retry
            mutex.withLock {
                if (!task.completion.isCompleted) {
                    task.completion.complete(TaskResult(false, error = e))
                }
                task.status = TaskStatus.FAILED
            }
        } catch (e: Exception) {
            println("[Executor] task ${task.id} failed: ${e.message}")
            handleFailure(task, e, TaskStatus.FAILED)
        }
    }

    private suspend fun handleFailure(task: Task, e: Throwable, status: TaskStatus) {
        mutex.withLock {
            task.markFailed(e)
            task.status = status
        }
        task.failureCallback?.invoke()
        // 回滚由你的回滚逻辑处理
        task.rollbackAction?.invoke()
        retry(task)
    }

    private suspend fun retry(task: Task) {
        if (task.currentRetryCount < task.retries && !task.isCancelled) {
            task.currentRetryCount++
            println("[Executor] Retrying task: ${task.id}, attempt: ${task.currentRetryCount}")
            delay(task.retryDelay)
            // 重试时不改变 input（上一级调用者负责传入正确 input）
            execute(task, task.output?.value)
        } else {
            println("[Executor] Task ${task.id} failed after ${task.retries} retries.")
        }
    }
}