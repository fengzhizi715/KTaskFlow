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

    fun task(id: String, taskName: String, taskAction: suspend () -> Unit): Task {
        val task = Task(id, taskName, taskAction)
        tasks[id] = task
        return task
    }
}