package cn.netdiscovery.taskflow

/**
 *
 * @FileName:
 *          cn.netdiscovery.taskflow.Task
 * @author: Tony Shen
 * @date: 2024/8/20 15:41
 * @version: V1.0 <描述当前版本功能>
 */
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

data class Task<I, O>(
    val id: String,
    val taskName: String,
    var priority: Int = 0,
    val type: TaskType = TaskType.IO,
    val taskAction: suspend (I) -> O,
    var input: I? = null,
    var output: O? = null
) : Comparable<Task<*, *>> {

    var status: TaskStatus = TaskStatus.NOT_STARTED
    var currentRetryCount: Int = 0
    var retries: Int = 3
    var timeout: Long = 5000
    var retryDelay: Long = 1000

    var successCallback: (() -> Unit)? = null
    var failureCallback: (() -> Unit)? = null

    val dependencies = mutableListOf<Task<*, *>>()
    val weakDependencies = mutableListOf<Task<*, *>>()
    val dependents = mutableListOf<Task<*, *>>()
    var indegree: Int = 0

    // 设置强依赖任务
    fun dependsOn(vararg tasks: Task<*, *>) {
        for (task in tasks) {
            dependencies.add(task)
            task.dependents.add(this)
        }
    }

    // 设置弱依赖任务
    fun weakDependsOn(vararg tasks: Task<*, *>) {
        for (task in tasks) {
            weakDependencies.add(task)
        }
    }

    // 更新优先级
    fun updatePriority(newPriority: Int) {
        priority = newPriority
    }

    override fun compareTo(other: Task<*, *>): Int {
        return other.priority - this.priority
    }
}