package cn.netdiscovery.taskflow

import kotlinx.coroutines.CompletableDeferred
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CancellationException
import java.util.concurrent.CopyOnWriteArrayList

/**
 *
 * @FileName:
 *          cn.netdiscovery.taskflow.Task
 * @author: Tony Shen
 * @date: 2024/8/20 15:41
 * @version: V1.0 <描述当前版本功能>
 */
/**
 * 任务动作接口，用于封装不同输入和输出类型的任务逻辑
 */
interface TaskAction {
    suspend fun execute(input: Any?): Any?
}

class SmartGenericTaskAction<I, O>(
    private val inputType: Class<I>,                // 主输入类型
    private val elementType: Class<*>? = null,      // 如果是集合，指定元素类型（可选）
    private val action: suspend (I) -> O
) : TaskAction {

    @Suppress("UNCHECKED_CAST")
    override suspend fun execute(input: Any?): Any? {
        return try {
            when {
                input == null && inputType == Unit::class.java ->
                    action(Unit as I)

                inputType.isInstance(input) ->
                    action(input as I)

                input is Collection<*> && Collection::class.java.isAssignableFrom(inputType) -> {
                    validateElements(input)
                    action(input as I)
                }

                input != null && Collection::class.java.isAssignableFrom(inputType) -> {
                    validateElement(input)
                    action(listOf(input) as I)
                }

                else -> throw IllegalArgumentException(
                    "SmartGenericTaskAction: Cannot cast ${input?.javaClass} to expected type $inputType"
                )
            }
        } catch (e: Exception) {
            println("SmartGenericTaskAction error: ${e.message}")
            throw e
        }
    }

    private fun validateElements(collection: Collection<*>) {
        elementType?.let { et ->
            collection.forEach { element ->
                if (element == null || !et.isInstance(element)) {
                    throw IllegalArgumentException("Collection element is not of expected type $et")
                }
            }
        }
    }

    private fun validateElement(element: Any) {
        elementType?.let { et ->
            if (!et.isInstance(element)) {
                throw IllegalArgumentException(
                    "Element ${element::class.java} is not of expected type $et"
                )
            }
        }
    }
}


enum class TaskStatus {
    NOT_STARTED,
    IN_PROGRESS,
    COMPLETED,
    FAILED,
    TIMED_OUT,
    CANCELLED
}

enum class TaskType {
    IO,  // I/O 密集型任务
    CPU  // 计算密集型任务
}

data class TaskResult(
    val success: Boolean,
    val value: Any? = null,
    val error: Throwable? = null
)

class Task(
    val id: String,
    val taskName: String,
    var priority: Int = 0,
    val type: TaskType = TaskType.IO,
    val taskAction: TaskAction
) : Comparable<Task> {
    @Volatile
    var status: TaskStatus = TaskStatus.NOT_STARTED

    @Volatile
    var currentRetryCount: Int = 0
    var retries: Int = 3
    var retryDelay: Long = 1000L

    var successCallback: (suspend () -> Unit)? = null
    var failureCallback: (suspend () -> Unit)? = null
    var rollbackAction: (suspend () -> Unit)? = null

    val dependencies = ConcurrentHashMap<String, Task>()      // 强依赖（上游）
    val weakDependencies = ConcurrentHashMap<String, Task>()  // 弱依赖（上游）
    val dependents = ConcurrentHashMap<String, Task>()        // 下游

    @Volatile
    var indegree: Int = 0

    @Volatile
    var output: TaskResult? = null

    @Volatile
    var weakDependenciesCompleted: Boolean = false

    @Volatile
    var isCancelled: Boolean = false

    var weakDependencyTimeout: Long = 0L   // 控制弱依赖的等待时长

    // 强依赖超时（毫秒，0 表示无限等）
    var strongDependencyTimeout: Long = 0L // 控制强依赖的等待时长
    var executionTimeout: Long = 0L        // 控制任务本身的执行时长

    @Volatile
    var weakDependencyWaitStarted: Boolean = false

    var cachePolicy: CachePolicy = CachePolicy.NONE
    var cacheKey: String? = null
    var cacheTTL: Long = 0L // 毫秒；<=0 表示不过期

    /** 便捷方法：启用缓存 */
    fun enableCache(key: String, ttlMillis: Long = 0L, policy: CachePolicy = CachePolicy.READ_WRITE): Task {
        this.cacheKey = key
        this.cacheTTL = ttlMillis
        this.cachePolicy = policy
        return this
    }

    // 对外可 await 的执行结果
    var completion = CompletableDeferred<TaskResult>()

    val dependencyOrder = CopyOnWriteArrayList<String>()

    fun markCompleted(result: TaskResult) {
        status = if (result.success) TaskStatus.COMPLETED else TaskStatus.FAILED
        output = result
        if (!completion.isCompleted) {
            completion.complete(result)
        }
    }

    fun markFailed(e: Throwable) {
        status = TaskStatus.FAILED
        val res = TaskResult(false, error = e)
        output = res
        if (!completion.isCompleted) {
            completion.complete(res)
        }
    }

    // 添加重置状态方法
    fun resetForRetry() {
        status = TaskStatus.NOT_STARTED
        output = null
        completion = CompletableDeferred() // 重置完成状态
    }

    fun cancel() {
        isCancelled = true
        status = TaskStatus.CANCELLED
        if (!completion.isCompleted) {
            completion.completeExceptionally(CancellationException("Task $id cancelled"))
        }
    }

    fun dependsOn(vararg tasks: Task) {
        for (task in tasks) {
            dependencies[task.id] = task
            task.dependents[this.id] = this
            dependencyOrder.add(task.id)
        }
    }

    fun weakDependsOn(vararg tasks: Task) {
        for (task in tasks) {
            weakDependencies[task.id] = task
            task.dependents[this.id] = this
        }
    }

    fun updatePriority(newPriority: Int) {
        priority = newPriority
    }

    override fun compareTo(other: Task): Int {
        return other.priority - this.priority
    }
}