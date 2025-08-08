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
    private val taskExecutor = TaskExecutor(mutex)

    // 全局任务队列，使用线程安全队列
    private val readyTasks = ConcurrentLinkedQueue<Task>()

    // 待执行的任务优先级队列
    private val taskQueue = PriorityQueue<Task>()

    // 动态添加任务
    suspend fun addAndSchedule(task: Task) {
        dag.addTask(task)

        mutex.withLock {
            task.indegree = task.dependencies.size
            val weakReady = task.weakDependencies.isEmpty() || task.weakDependenciesCompleted
            if (task.indegree == 0 && weakReady) {
                readyTasks.add(task)
            }
        }
    }

    // 启动任务调度
    suspend fun start() {
        dag.getTasks().values.forEach { task ->
            task.indegree = task.dependencies.size
            if (task.indegree == 0) {
                readyTasks.add(task)
            }
        }

        while (readyTasks.isNotEmpty() || dag.getTasks().values.any { it.status == TaskStatus.IN_PROGRESS }) {
            while (readyTasks.isNotEmpty()) {
                val task = readyTasks.poll()
                if (task != null) {
                    taskQueue.add(task)
                }
            }

            val ioTasks = mutableListOf<Task>()
            val cpuTasks = mutableListOf<Task>()

            while (taskQueue.isNotEmpty()) {
                val task = taskQueue.poll()

                if (task.weakDependencies.isEmpty() || task.weakDependenciesCompleted) {
                    if (task.type == TaskType.IO) {
                        ioTasks.add(task)
                    } else {
                        cpuTasks.add(task)
                    }
                } else {
                    readyTasks.add(task)
                }
            }

            val ioJobs = ioTasks.map { task ->
                ioTaskPool.async {
                    println("Executing IO Task: ${task.id}")
                    executeAndNotify(task)
                }
            }

            val cpuJobs = cpuTasks.map { task ->
                cpuTaskPool.async {
                    println("Executing CPU Task: ${task.id}")
                    executeAndNotify(task)
                }
            }

            ioJobs.awaitAll()
            cpuJobs.awaitAll()
        }

        println("All tasks have been executed.")
    }

    private suspend fun executeAndNotify(task: Task) {
        taskExecutor.execute(task)

        if (task.status == TaskStatus.COMPLETED) {
            mutex.withLock {
                // 更新所有依赖该任务的任务
                for (dependent in task.dependents.values) {
                    dependent.indegree--
                    // ✅ 缓存弱依赖完成状态
                    if (!dependent.weakDependenciesCompleted &&
                        dependent.weakDependencies.values.all { it.status == TaskStatus.COMPLETED }
                    ) {
                        dependent.weakDependenciesCompleted = true
                    }

                    if (dependent.indegree == 0) {
                        readyTasks.add(dependent)
                    }
                }
            }
        }
    }
}