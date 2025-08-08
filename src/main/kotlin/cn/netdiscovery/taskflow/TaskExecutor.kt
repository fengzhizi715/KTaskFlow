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

    suspend fun execute(task: Task) {
        try {
            withTimeout(task.timeout) {
                mutex.withLock {
                    task.status = TaskStatus.IN_PROGRESS
                }

                val inputs = task.dependencies.values.mapNotNull { it.output?.value }

                val resolvedInput: Any? = when {
                    inputs.isEmpty() -> Unit
                    inputs.size == 1 -> inputs[0]
                    else -> inputs
                }

//                val result = try {
//                    task.taskAction.execute(resolvedInput)
//                } catch (e: ClassCastException) {
//                    throw IllegalArgumentException(
//                        "Task [${task.id}] received input of type ${resolvedInput?.javaClass?.name}, " +
//                                "which is incompatible with its expected input type. Hint: check your GenericTaskAction declaration.",
//                        e
//                    )
//                }

                val result = when (val deps = task.dependencies.values.map { it.output?.value }) {
                    is List<*> -> when (deps.size) {
                        0 -> task.taskAction.execute(Unit)
                        1 -> task.taskAction.execute(deps[0])
                        else -> task.taskAction.execute(deps)
                    }
                    else -> task.taskAction.execute(deps)
                }

                mutex.withLock {
                    task.output = TaskResult(success = true, value = result)
                    task.status = TaskStatus.COMPLETED
                }

                task.successCallback?.invoke()
            }
        } catch (e: TimeoutCancellationException) {
            mutex.withLock {
                task.output = TaskResult(success = false, error = e)
                task.status = TaskStatus.TIMED_OUT
            }
            task.failureCallback?.invoke()
            retry(task)
        } catch (e: Exception) {
            mutex.withLock {
                task.output = TaskResult(success = false, error = e)
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