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

data class Task(
    val id: String,
    val taskName: String,
    val taskAction: suspend () -> Unit,
    val priority: Int = 0 // 默认优先级为 0, 值越大，优先级越高
) : Comparable<Task> {
    var status: TaskStatus = TaskStatus.NOT_STARTED
    var currentRetryCount: Int = 0
    var retries: Int = 3 // 默认重试次数
    var successCallback: (() -> Unit)? = null
    var failureCallback: (() -> Unit)? = null
    var timeout: Long = 5000 // 默认超时时间
    var retryDelay: Long = 1000 // 重试延迟时间

    // 任务的强依赖
    val dependencies = mutableListOf<Task>()

    // 任务的弱依赖
    val weakDependencies = mutableListOf<Task>()

    // 依赖此任务的其他任务
    val dependents = mutableListOf<Task>()

    // 入度，表示任务的依赖关系数
    var indegree: Int = 0

    // 设置任务的强依赖关系
    fun dependsOn(vararg tasks: Task) {
        for (task in tasks) {
            dependencies.add(task)
            task.dependents.add(this)
            indegree++ // 依赖的任务增加入度
        }
    }

    // 设置任务的弱依赖关系
    fun weakDependsOn(vararg tasks: Task) {
        for (task in tasks) {
            weakDependencies.add(task)
        }
    }

    // 比较任务优先级，支持任务优先级的调度
    override fun compareTo(other: Task): Int {
        // 优先级越高越优先调度，优先级高的任务排前
        return other.priority - this.priority
    }
}