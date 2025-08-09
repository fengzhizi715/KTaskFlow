package cn.netdiscovery.taskflow

import java.util.concurrent.ConcurrentHashMap

/**
 *
 * @FileName:
 *          cn.netdiscovery.taskflow.DAG
 * @author: Tony Shen
 * @date: 2024/12/10 15:29
 * @version: V1.0 <描述当前版本功能>
 */
class DAG {
    private val tasks = ConcurrentHashMap<String, Task>()

    fun task(id: String, name: String, priority: Int = 0, type: TaskType = TaskType.IO, action: TaskAction): Task {
        val t = Task(id, name, priority, type, action)
        tasks[id] = t
        return t
    }

    fun getTasks() = tasks

    fun getTaskById(id: String): Task? = tasks[id]

    // 动态添加任务（支持从调度器调用）
    fun addTask(task: Task) {
        tasks[task.id] = task
    }

    suspend fun getTaskResultAsync(task: Task): TaskResult {
        return task.completion.await()
    }

    suspend fun getTaskResultAsync(taskId: String): TaskResult {
        val task = tasks[taskId] ?: throw IllegalArgumentException("Task $taskId not found")
        return task.completion.await()
    }
}