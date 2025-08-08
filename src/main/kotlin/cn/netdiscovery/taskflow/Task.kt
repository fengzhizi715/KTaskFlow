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

// 通用的 TaskAction 实现
class GenericTaskAction<I, O>(
    private val action: suspend (I) -> O
) : TaskAction {
    @Suppress("UNCHECKED_CAST")
    override suspend fun execute(input: Any?): Any? {
        return action(input as I)
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

    val dependencies = ConcurrentHashMap<String, Task>()
    val weakDependencies = ConcurrentHashMap<String, Task>()
    val dependents = ConcurrentHashMap<String, Task>()
    var indegree: Int = 0

    // 存储任务输出
    @Volatile
    var output: Any? = null

    @Volatile
    var weakDependenciesCompleted: Boolean = false

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