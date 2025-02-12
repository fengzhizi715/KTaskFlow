package cn.netdiscovery.taskflow

/**
 *
 * @FileName:
 *          cn.netdiscovery.taskflow.DAG
 * @author: Tony Shen
 * @date: 2024/12/10 15:29
 * @version: V1.0 <描述当前版本功能>
 */
class DAG {
    val tasks = mutableMapOf<String, Task>()

    fun task(id: String, taskName: String, priority: Int = 0, taskAction: suspend () -> Unit): Task {
        val task = Task(id, taskName, priority, taskAction)
        if (tasks.containsKey(id)) {
            throw IllegalArgumentException("Task with id $id already exists.")
        }
        tasks[id] = task
        return task
    }
}