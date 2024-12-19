package cn.netdiscovery.taskflow

import kotlinx.coroutines.*

/**
 *
 * @FileName:
 *          cn.netdiscovery.taskflow.TaskScheduler
 * @author: Tony Shen
 * @date: 2024/12/10 15:30
 * @version: V1.0 <描述当前版本功能>
 */
class TaskScheduler(private val dag: DAG, private val jobScope: CoroutineScope = CoroutineScope(Dispatchers.Default)) {

    // 执行任务
    private suspend fun execute(task: Task) {
        try {
            withTimeout(task.timeout) {
                task.status = TaskStatus.IN_PROGRESS
                task.taskAction()
                task.successCallback?.invoke()
                task.status = TaskStatus.COMPLETED
            }
        } catch (e: TimeoutCancellationException) {
            task.status = TaskStatus.TIMED_OUT
            task.failureCallback?.invoke()
            retry(task)
        } catch (e: Exception) {
            task.status = TaskStatus.FAILED
            task.failureCallback?.invoke()
            retry(task)
        }
    }

    // 重试机制
    private suspend fun retry(task: Task) {
        if (task.retryCount < task.retries) {
            task.retryCount++
            println("Retrying task: ${task.id}, attempt: ${task.retryCount}")
            execute(task)
        }
    }

    // 启动所有准备好的任务
    suspend fun start() {
        // Step 1: 初始化任务的入度，只考虑强依赖，不考虑弱依赖
        val readyTasks = mutableListOf<Task>()
        for (task in dag.tasks.values) {
            // 初始化入度，只计算强依赖（通过 dependsOn 添加的任务）
            task.indegree = task.dependencies.size  // 只计算强依赖
            if (task.indegree == 0) {
                readyTasks.add(task)
            }
        }

        // Step 2: 持续调度任务，直到所有任务都完成
        while (readyTasks.isNotEmpty()) {
            val tasksToExecute = readyTasks.toList()
            readyTasks.clear()

            val jobs = tasksToExecute.map { task ->
                jobScope.async {
                    println("task id = ${task.id}")

                    // 执行任务
                    if (task.weakDependencies.size==0) {
                        execute(task)

                        // 完成当前任务后，更新依赖关系
                        for (dependentTask in task.dependents) {
                            dependentTask.indegree--
                            if (dependentTask.indegree == 0) {
                                readyTasks.add(dependentTask)
                            }
                        }
                    } else if (task.weakDependencies.size > 0){

                        // 检查依赖是否完成
                        val dependencyCompleted = task.weakDependencies.any { it.status == TaskStatus.COMPLETED }

                        println("dependencyCompleted = $dependencyCompleted")

                        if (dependencyCompleted) {
                            execute(task)
                        } else {
                            readyTasks.add(task)
                        }
                    }
                }
            }

            // 等待当前批次的任务执行完毕
            jobs.awaitAll()
        }

        // 确保所有任务都执行完毕后结束
        println("All tasks have been executed.")
    }
}