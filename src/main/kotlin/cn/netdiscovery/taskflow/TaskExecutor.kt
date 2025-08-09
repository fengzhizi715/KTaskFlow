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

    suspend fun execute(task: Task, input: Any?) {
        try {
            if (task.isCancelled) {
                println("Task ${task.id} was cancelled before execution.")
                return
            }

            withTimeout(task.timeout) {
                mutex.withLock {
                    task.status = TaskStatus.IN_PROGRESS
                }

                // 执行任务动作
                val result = if (task.taskAction is SmartGenericTaskAction<*, *>) {
                    @Suppress("UNCHECKED_CAST")
                    (task.taskAction as SmartGenericTaskAction<Any?, Any?>).execute(input)
                } else {
                    task.taskAction.execute(null)
                }

                mutex.withLock {
                    task.markCompleted(TaskResult(success = true, value = result))
                    task.status = TaskStatus.COMPLETED
                }

                task.successCallback?.invoke()
            }
        } catch (e: TimeoutCancellationException) {
            handleFailure(task, e, TaskStatus.TIMED_OUT)
        } catch (e: Exception) {
            handleFailure(task, e, TaskStatus.FAILED)
        }
    }

    private suspend fun handleFailure(task: Task, e: Throwable, status: TaskStatus) {
        mutex.withLock {
            task.markFailed(e)
            task.status = status
        }
        task.failureCallback?.invoke()
        task.rollbackAction?.invoke()
        retry(task)
    }

    private suspend fun retry(task: Task) {
        if (task.currentRetryCount < task.retries && !task.isCancelled) {
            task.currentRetryCount++
            println("Retrying task: ${task.id}, attempt: ${task.currentRetryCount}")
            delay(task.retryDelay)
            execute(task, task.output?.value)  // 传递上次输入或者 null
        } else {
            println("Task ${task.id} failed after ${task.retries} retries.")
        }
    }
}