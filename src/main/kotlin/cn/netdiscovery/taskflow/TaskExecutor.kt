package cn.netdiscovery.taskflow

import kotlinx.coroutines.*
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.util.concurrent.ConcurrentHashMap
import kotlin.coroutines.cancellation.CancellationException

/**
 *
 * @FileName:
 *          cn.netdiscovery.taskflow.TaskExecutor
 * @author: Tony Shen
 * @date: 2025/8/8 10:18
 * @version: V1.0 <描述当前版本功能>
 */
class TaskExecutor(
    private val mutex: Mutex,
    private val scope: CoroutineScope,
    // enqueueCallback 用于把需要重试的任务重新入队到调度器（由 TaskScheduler 提供）
    private val enqueueCallback: suspend (Task) -> Unit
) {

    // 每个任务单独的执行锁：防止同一任务被并行执行 / 重入
    private val taskLocks = ConcurrentHashMap<String, Mutex>()

    private fun getTaskLock(task: Task): Mutex =
        taskLocks.computeIfAbsent(task.id) { Mutex() }

    /**
     * 执行任务（会在 task 的独占锁内运行，确保同一时刻只有一个执行体）
     */
    suspend fun execute(task: Task, input: Any?) {
        println("[Executor] start execute task=${task.id}, input=$input")

        if (task.isCancelled) {
            println("[Executor] Task ${task.id} was cancelled before execution.")
            // 确保 completion 被标记（若尚未）
            mutex.withLock {
                if (!task.completion.isCompleted) {
                    task.markFailed(CancellationException("Task ${task.id} cancelled before execution"))
                }
            }
            return
        }

        // 使用任务级别锁避免同时执行同一 task 的多个实例（包括重试）
        val taskLock = getTaskLock(task)
        taskLock.withLock {
            // Double-check cancelled inside lock
            if (task.isCancelled) {
                println("[Executor] Task ${task.id} is cancelled (inside taskLock), aborting execution.")
                mutex.withLock {
                    if (!task.completion.isCompleted) {
                        task.markFailed(CancellationException("Task ${task.id} cancelled before execution"))
                    }
                }
                return
            }

            val timeout = if (task.executionTimeout > 0L) task.executionTimeout else Long.MAX_VALUE

            try {
                withTimeout(timeout) {
                    // 标记为进行中（使用外层 mutex 以防并发修改 status）
                    mutex.withLock { task.status = TaskStatus.IN_PROGRESS }

                    val resultValue = try {
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
                        task.markCompleted(TaskResult(success = true, value = resultValue))
                    }

                    println("[Executor] task ${task.id} completed with value=$resultValue")

                    // 成功回调（在 scope 中启动避免阻塞）
                    task.successCallback?.let { cb ->
                        scope.launch {
                            try {
                                cb()
                            } catch (ex: Throwable) {
                                println("[Executor] successCallback error for ${task.id}: ${ex.message}")
                            }
                        }
                    }
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
        } // end taskLock.withLock
    }

    /**
     * 统一的失败处理：记录错误、决定重试或最终失败。
     * 注意：不会直接做重新 execute() 调用（避免递归并发），而是通过 enqueueCallback 通知调度器重新入队。
     */
    private fun handleFailure(task: Task, e: Throwable, input: Any?, status: TaskStatus) {
        scope.launch {
            // 记录错误并将 task 置为临时失败状态（在线程安全锁内）
            mutex.withLock {
                task.output = TaskResult(success = false, error = e)
                task.status = when (status) {
                    TaskStatus.TIMED_OUT -> TaskStatus.TIMED_OUT
                    else -> TaskStatus.FAILED
                }
            }

            // 决定是否重试
            if (task.currentRetryCount >= task.retries) {
                // 达到重试上限，标记最终失败并触发 failure/rollback 回调
                mutex.withLock {
                    task.markFailed(e)
                }

                // failure/rollback 在新的协程中运行，避免阻塞调度
                task.failureCallback?.let { cb ->
                    scope.launch {
                        try {
                            cb()
                        } catch (ex: Throwable) {
                            println("[Executor] failureCallback error for ${task.id}: ${ex.message}")
                        }
                    }
                }
                task.rollbackAction?.let { rb ->
                    scope.launch {
                        try {
                            rb()
                        } catch (ex: Throwable) {
                            println("[Executor] rollbackAction error for ${task.id}: ${ex.message}")
                        }
                    }
                }
                println("[Executor] Task ${task.id} failed after ${task.retries} retries.")
            } else {
                // 计划重试：只重置必要状态（不直接改 indegree），并通过 enqueueCallback 通知调度器重新入队
                task.currentRetryCount++
                println("[Executor] Scheduling retry ${task.currentRetryCount} for task: ${task.id} after delay=${task.retryDelay}ms")
                // 等待重试间隔后再让调度器把任务重新放回执行流程
                scope.launch {
                    delay(task.retryDelay)
                    // 在重试前重置任务状态与 completion（线程安全）
                    mutex.withLock {
                        task.resetForRetry()
                    }
                    try {
                        enqueueCallback(task)
                    } catch (ex: Throwable) {
                        println("[Executor] enqueueCallback error when retrying ${task.id}: ${ex.message}")
                        // 若无法入队，标记为失败
                        mutex.withLock {
                            task.markFailed(ex)
                        }
                    }
                }
            }
        }
    }
}