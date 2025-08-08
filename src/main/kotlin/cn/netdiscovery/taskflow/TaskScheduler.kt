package cn.netdiscovery.taskflow

import kotlinx.coroutines.*
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.util.*
import java.util.concurrent.ConcurrentLinkedQueue

/**
 *
 * @FileName:
 *          cn.netdiscovery.taskflow.TaskScheduler
 * @author: Tony Shen
 * @date: 2024/12/10 15:30
 * @version: V1.0 <描述当前版本功能>
 */
class TaskScheduler(private val dag: DAG) {

    // 并发控制
    private val mutex = Mutex()

    // I/O 密集型任务的协程池
    private val ioTaskPool = CoroutineScope(Dispatchers.IO)

    // CPU 密集型任务的协程池
    private val cpuTaskPool = CoroutineScope(Dispatchers.Default)

    // 任务执行器
    private val taskExecutor = TaskExecutor()

    // 全局任务队列，使用线程安全队列
    private val readyTasks = ConcurrentLinkedQueue<Task<*, *>>()

    // 待执行的任务优先级队列
    private val taskQueue = PriorityQueue<Task<*, *>>()

    // 启动任务调度
    suspend fun start() {
        // 初始化任务的入度和依赖
        dag.getTasks().values.forEach { task ->
            task.indegree = task.dependencies.size
            if (task.indegree == 0) {
                readyTasks.add(task)
            }
        }

        while (readyTasks.isNotEmpty() || dag.getTasks().values.any { it.status == TaskStatus.IN_PROGRESS }) {
            // 将就绪任务添加到优先级队列
            while (readyTasks.isNotEmpty()) {
                val task = readyTasks.poll()
                if (task != null) {
                    taskQueue.add(task)
                }
            }

            // 按任务类型分组
            val ioTasks = mutableListOf<Task<*, *>>()
            val cpuTasks = mutableListOf<Task<*, *>>()

            while (taskQueue.isNotEmpty()) {
                val task = taskQueue.poll()

                // 检查弱依赖是否完成
                val weakDependenciesCompleted = task.weakDependencies.all { it.status == TaskStatus.COMPLETED }

                if (weakDependenciesCompleted) {
                    if (task.type == TaskType.IO) {
                        ioTasks.add(task)
                    } else {
                        cpuTasks.add(task)
                    }
                } else {
                    // 弱依赖未完成，重新加入就绪队列，等待下次调度
                    readyTasks.add(task)
                }
            }

            // 执行 I/O 密集型任务
            val ioJobs = ioTasks.map { task ->
                ioTaskPool.async {
                    println("Executing IO Task: ${task.id}")
                    executeAndNotify(task)
                }
            }

            // 执行 CPU 密集型任务
            val cpuJobs = cpuTasks.map { task ->
                cpuTaskPool.async {
                    println("Executing CPU Task: ${task.id}")
                    executeAndNotify(task)
                }
            }

            // 等待本次循环所有任务完成，然后进行下一次调度
            ioJobs.awaitAll()
            cpuJobs.awaitAll()
        }

        println("All tasks have been executed.")
    }

    // 执行任务并通知其依赖任务
    private suspend fun executeAndNotify(task: Task<*, *>) {
        taskExecutor.execute(task)

        // 任务完成后，更新其依赖任务的状态
        if (task.status == TaskStatus.COMPLETED) {
            mutex.withLock {
                for (dependentTask in task.dependents) {
                    dependentTask.indegree--
                    // 检查是否所有强依赖都已完成
                    if (dependentTask.indegree == 0) {
                        // 将任务添加到就绪队列
                        readyTasks.add(dependentTask)
                    }
                }
            }
        }
    }
}