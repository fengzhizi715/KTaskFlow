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

    // ------------------- 新增：任务级缓存（进程内） -----------------------
    private data class CacheEntry(val result: TaskResult, val ts: Long, val ttl: Long)
    private val resultCache = ConcurrentHashMap<String, CacheEntry>()
    // “同 key 并发合并”（防止缓存击穿/抖动）
    private val inflightByKey = ConcurrentHashMap<String, CompletableDeferred<TaskResult>>()

    private fun getFromCacheFresh(key: String, now: Long): TaskResult? {
        val e = resultCache[key] ?: return null
        val ttl = e.ttl
        if (ttl <= 0L) return e.result
        val fresh = (now - e.ts) <= ttl
        return if (fresh) e.result else {
            resultCache.remove(key, e)
            null
        }
    }
    // -------------------------------------------------------------------

    /**
     * 执行任务（会在 task 的独占锁内运行，确保同一时刻只有一个执行体）
     */
    suspend fun execute(task: Task, input: Any?) {
        println("[Executor] start execute task=${task.id}, input=$input")

        if (task.isCancelled) {
            println("[Executor] Task ${task.id} was cancelled before execution.")
            mutex.withLock {
                if (!task.completion.isCompleted) {
                    task.markFailed(CancellationException("Task ${task.id} cancelled before execution"))
                }
            }
            return
        }

        // ------------------- 新增：执行前缓存快速路径 -----------------------
        val cacheKey = task.cacheKey
        val cachePolicy = task.cachePolicy
        val now = System.currentTimeMillis()

        if (cacheKey != null && cachePolicy.canRead()) {
            // 1) 直接命中已完成缓存
            getFromCacheFresh(cacheKey, now)?.let { cached ->
                println("[Executor] cache hit for task=${task.id}, key=$cacheKey")
                mutex.withLock { task.markCompleted(cached) }
                // 成功回调（非阻塞触发）
                task.successCallback?.let { cb ->
                    scope.launch {
                        try { cb() } catch (ex: Throwable) {
                            println("[Executor] successCallback error for ${task.id}: ${ex.message}")
                        }
                    }
                }
                return
            }

            // 2) 合并同 key 的并发请求：如果已有“领头羊”在跑，就等待它的结果
            val waiter = CompletableDeferred<TaskResult>()
            val existing = inflightByKey.putIfAbsent(cacheKey, waiter)
            if (existing != null) {
                println("[Executor] inflight hit for task=${task.id}, waiting key=$cacheKey")
                try {
                    val res = existing.await()
                    mutex.withLock { task.markCompleted(res) }
                    task.successCallback?.let { cb ->
                        scope.launch { try { cb() } catch (ex: Throwable) { println("[Executor] successCallback error for ${task.id}: ${ex.message}") } }
                    }
                    return
                } catch (ex: Throwable) {
                    // 领头失败，移除并继续由当前任务尝试执行
                    inflightByKey.remove(cacheKey, existing)
                    println("[Executor] inflight await failed for key=$cacheKey, continue executing. reason=${ex.message}")
                }
            } else {
                // 当前任务成为“领头羊”，执行后需要负责写入缓存并唤醒等待者
                // 用 waiter 作为本 key 的占位
            }
        }
        // -------------------------------------------------------------------

        // 使用任务级别锁避免同时执行同一 task 的多个实例（包括重试）
        val taskLock = getTaskLock(task)
        taskLock.withLock {
            if (task.isCancelled) {
                println("[Executor] Task ${task.id} is cancelled (inside taskLock), aborting execution.")
                mutex.withLock {
                    if (!task.completion.isCompleted) {
                        task.markFailed(CancellationException("Task ${task.id} cancelled before execution"))
                    }
                }
                // 如果是“领头羊”，记得撤销 inflight 占位
                if (cacheKey != null) inflightByKey.remove(cacheKey)
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

                    val result = TaskResult(success = true, value = resultValue)

                    // 写缓存（如果允许）
                    if (cacheKey != null && cachePolicy.canWrite()) {
                        resultCache[cacheKey] = CacheEntry(result, System.currentTimeMillis(), task.cacheTTL)
                        println("[Executor] cache write for task=${task.id}, key=$cacheKey, ttl=${task.cacheTTL}")
                    }

                    mutex.withLock { task.markCompleted(result) }
                    println("[Executor] task ${task.id} completed with value=$resultValue")

                    // 唤醒同 key 等待者
                    if (cacheKey != null) {
                        inflightByKey.remove(cacheKey)?.complete(result)
                    }

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
                // 如果是“领头羊”，要通知等待者失败
                if (cacheKey != null) {
                    inflightByKey.remove(cacheKey)?.completeExceptionally(e)
                }
                handleFailure(task, e, input, TaskStatus.TIMED_OUT)
            } catch (e: CancellationException) {
                println("[Executor] task ${task.id} cancelled during execution.")
                if (cacheKey != null) {
                    inflightByKey.remove(cacheKey)?.completeExceptionally(e)
                }
                mutex.withLock { task.markFailed(e) }
            } catch (e: Exception) {
                println("[Executor] task ${task.id} failed: ${e.message}")
                if (cacheKey != null) {
                    inflightByKey.remove(cacheKey)?.completeExceptionally(e)
                }
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
            mutex.withLock {
                task.output = TaskResult(success = false, error = e)
                task.status = when (status) {
                    TaskStatus.TIMED_OUT -> TaskStatus.TIMED_OUT
                    else -> TaskStatus.FAILED
                }
            }

            if (task.currentRetryCount >= task.retries) {
                mutex.withLock {
                    task.markFailed(e)
                }

                task.failureCallback?.let { cb ->
                    scope.launch {
                        try { cb() } catch (ex: Throwable) {
                            println("[Executor] failureCallback error for ${task.id}: ${ex.message}")
                        }
                    }
                }
                task.rollbackAction?.let { rb ->
                    scope.launch {
                        try { rb() } catch (ex: Throwable) {
                            println("[Executor] rollbackAction error for ${task.id}: ${ex.message}")
                        }
                    }
                }
                println("[Executor] Task ${task.id} failed after ${task.retries} retries.")
            } else {
                task.currentRetryCount++
                println("[Executor] Scheduling retry ${task.currentRetryCount} for task: ${task.id} after delay=${task.retryDelay}ms")
                scope.launch {
                    delay(task.retryDelay)
                    mutex.withLock { task.resetForRetry() }
                    try {
                        enqueueCallback(task)
                    } catch (ex: Throwable) {
                        println("[Executor] enqueueCallback error when retrying ${task.id}: ${ex.message}")
                        mutex.withLock { task.markFailed(ex) }
                    }
                }
            }
        }
    }
}