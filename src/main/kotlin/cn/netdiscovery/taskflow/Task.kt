package cn.netdiscovery.taskflow

import java.util.concurrent.ConcurrentHashMap

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
    private val action: suspend (I) -> O,
    private val inputClass: Class<I>
) : TaskAction {
    @Suppress("UNCHECKED_CAST")
    override suspend fun execute(input: Any?): Any? {
        return try {
            if (inputClass.isInstance(input)) {
                action(input as I)
            } else {
                // 自动包裹输入为 List<I>（比如单个输入变成 list）
                val wrapped = listOfNotNull(input) as I
                action(wrapped)
            }
        } catch (e: Exception) {
            println("SmartGenericTaskAction error: ${e.message}")
            throw e
        }
    }
}

enum class TaskStatus {
    NOT_STARTED,
    IN_PROGRESS,
    COMPLETED,
    FAILED,
    TIMED_OUT
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
    var status: TaskStatus = TaskStatus.NOT_STARTED
    var currentRetryCount: Int = 0
    var retries: Int = 3
    var timeout: Long = 5000
    var retryDelay: Long = 1000

    var successCallback: (() -> Unit)? = null
    var failureCallback: (() -> Unit)? = null
    var rollbackAction: (() -> Unit)? = null

    val dependencies = ConcurrentHashMap<String, Task>()
    val weakDependencies = ConcurrentHashMap<String, Task>()
    val dependents = ConcurrentHashMap<String, Task>()
    var indegree: Int = 0

    // 存储任务输出
    @Volatile
    var output: TaskResult? = null

    @Volatile
    var weakDependenciesCompleted: Boolean = false

    @Volatile
    var isCancelled: Boolean = false

    fun cancel() {
        isCancelled = true
    }

    // 设置强依赖任务
    fun dependsOn(vararg tasks: Task) {
        for (task in tasks) {
            dependencies[task.id] = task
            task.dependents[this.id] = this
        }
    }

    // 设置弱依赖任务
    fun weakDependsOn(vararg tasks: Task) {
        for (task in tasks) {
            weakDependencies[task.id] = task
        }
    }

    fun updatePriority(newPriority: Int) {
        priority = newPriority
    }

    override fun compareTo(other: Task): Int {
        return other.priority - this.priority
    }
}