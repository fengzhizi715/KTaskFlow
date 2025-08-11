package cn.netdiscovery.taskflow

import kotlinx.coroutines.CompletableDeferred
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CancellationException

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
    private val action: suspend (I) -> O
) : TaskAction {
    @Suppress("UNCHECKED_CAST")
    override suspend fun execute(input: Any?): Any? {
        return try {
            when {
                // 输入为 null，且 I 是 Unit
                input == null && Unit::class.java.isAssignableFrom(getGenericTypeClass()) ->
                    action(Unit as I)

                // 输入已经是 I 类型
                getGenericTypeClass().isInstance(input) ->
                    action(input as I)

                // 输入是列表，且 I 是列表
                input is List<*> && List::class.java.isAssignableFrom(getGenericTypeClass()) ->
                    action(input as I)

                // 输入是单值，但 I 是列表
                input != null && List::class.java.isAssignableFrom(getGenericTypeClass()) ->
                    action(listOf(input) as I)

                // 其他无法匹配的情况
                else -> throw IllegalArgumentException(
                    "SmartGenericTaskAction: Cannot cast ${input?.javaClass} to expected type ${getGenericTypeClass()}"
                )
            }
        } catch (e: Exception) {
            println("SmartGenericTaskAction error: ${e.message}")
            throw e
        }
    }

    private fun getGenericTypeClass(): Class<*> {
        val type = (action.javaClass.genericInterfaces.firstOrNull()
            ?: action.javaClass.genericSuperclass)
        return when (type) {
            is java.lang.reflect.ParameterizedType -> type.actualTypeArguments[0] as? Class<*>
                ?: Any::class.java
            else -> Any::class.java
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

    var weakDependencyThreshold: Float = 1.0f
    var weakDependencyTimeout: Long = 0L   // 控制弱依赖的等待时长

    // 强依赖超时（毫秒，0 表示无限等）
    var strongDependencyTimeout: Long = 0L // 控制强依赖的等待时长
    var executionTimeout: Long = 0L        // 控制任务本身的执行时长

    @Volatile
    var weakDependencyWaitStarted: Boolean = false

    @Volatile
    var rollbackDone: Boolean = false

    // 对外可 await 的执行结果
    val completion = CompletableDeferred<TaskResult>()

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