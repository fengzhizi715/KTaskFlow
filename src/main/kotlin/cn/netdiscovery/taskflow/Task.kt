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

data class Task(
    val id: String,
    val taskName: String,
    var priority: Int = 0, // 默认优先级为 0, 值越大，优先级越高
    val type: TaskType = TaskType.IO,  // 默认是 I/O 密集型任务
    val taskAction: suspend () -> Unit
) : Comparable<Task> {

    var status: TaskStatus = TaskStatus.NOT_STARTED
    var currentRetryCount: Int = 0
    var retries: Int = 3 // 默认重试次数
    var timeout: Long = 5000 // 默认超时时间
    var retryDelay: Long = 1000 // 重试延迟时间

    var successCallback: (() -> Unit)? = null
    var failureCallback: (() -> Unit)? = null

    val dependencies = mutableListOf<Task>()  // 强依赖任务
    val weakDependencies = mutableListOf<Task>()  // 弱依赖任务
    val dependents = mutableListOf<Task>()  // 依赖此任务的任务
    var indegree: Int = 0  // 入度

    // 设置强依赖任务
    fun dependsOn(vararg tasks: Task) {
        for (task in tasks) {
            dependencies.add(task)
            task.dependents.add(this)
            indegree++ // 每添加一个依赖，当前任务的入度加一
        }
    }

    // 设置弱依赖任务
    fun weakDependsOn(vararg tasks: Task) {
        for (task in tasks) {
            weakDependencies.add(task)
        }
    }

    // 修改优先级
    fun updatePriority(newPriority: Int) {
        priority = newPriority
    }

    // 排序规则：优先级高的排在前面
    override fun compareTo(other: Task): Int {
        return other.priority - this.priority  // 优先级越高，排序越前
    }
}