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

class Task(
    val id: String,
    val taskName: String,
    val taskAction: suspend () -> Unit
) {
    var status: TaskStatus = TaskStatus.NOT_STARTED
    var retryCount: Int = 0
    var successCallback: (() -> Unit)? = null
    var failureCallback: (() -> Unit)? = null
    var timeout: Long = 5000 // 默认超时时间
    var retries: Int = 3 // 默认重试次数

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
}