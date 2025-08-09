package cn.netdiscovery.taskflow

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

/**
 *
 * @FileName:
 *          cn.netdiscovery.taskflow.TaskScheduler
 * @author: Tony Shen
 * @date: 2024/12/10 15:30
 * @version: V1.0 <描述当前版本功能>
 */
class TaskScheduler(private val dag: DAG) {
    private val readyChannel = Channel<Task>(Channel.UNLIMITED)
    private val mutex = Mutex()

    private val cpuTaskPool = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private val ioTaskPool = CoroutineScope(SupervisorJob() + Dispatchers.IO)

    private val taskExecutor = TaskExecutor(mutex)

    private val activeTasks = AtomicInteger(0)
    private val allTasksCompleted = CompletableDeferred<Unit>()

    // 维护任务ID与其执行协程 Job 的映射
    private val runningJobs = ConcurrentHashMap<String, Job>()

    suspend fun start() {
        cpuTaskPool.launch {
            for (task in readyChannel) {
                activeTasks.incrementAndGet()
                launchTask(task)
            }
        }
        enqueueInitialTasks()

        allTasksCompleted.await()  // 等所有任务完成
    }

    suspend fun cancelTask(taskId: String) {
        val task = dag.getTaskById(taskId) ?: return
        mutex.withLock {
            cancelTaskAndDependents(task)
        }
    }

    private fun cancelTaskAndDependents(task: Task) {
        if (task.isCancelled) return // 已取消无需重复

        println("Cancelling task ${task.id}")
        task.cancel()

        // 取消正在执行的协程
        runningJobs[task.id]?.cancel()

        // 递归取消所有依赖此任务的任务
        for (dependent in task.dependents.values) {
            cancelTaskAndDependents(dependent)
        }
    }

    private fun enqueueInitialTasks() {
        dag.getTasks().values.forEach { task ->
            if (task.indegree == 0) {
                if (task.weakDependencies.isEmpty()) {
                    cpuTaskPool.launch { readyChannel.send(task) }
                } else {
                    launchWeakDependencyWaitAndMaybeEnqueue(task)
                }
            }
        }
    }

    private fun launchWeakDependencyWaitAndMaybeEnqueue(task: Task) {
        ioTaskPool.launch {
            val startTime = System.currentTimeMillis()
            while (true) {
                val allCompleted = task.weakDependencies.values.all { it.status == TaskStatus.COMPLETED }
                val elapsed = System.currentTimeMillis() - startTime
                val timeout = task.timeout

                if (allCompleted) break
                if (timeout > 0 && elapsed > timeout) {
                    println("Weak dependency timeout for task ${task.id}, continuing execution.")
                    break
                }
                delay(50)
            }

            mutex.withLock {
                if (!task.isCancelled && task.indegree == 0) {
                    readyChannel.send(task)
                }
            }
        }
    }

    private suspend fun launchTask(task: Task) {
        if (task.isCancelled) {
            println("Task ${task.id} is cancelled before execution, skipping.")
            onTaskCompleted(task) // 依然通知完成，更新下游任务
            return
        }

        val pool = if (task.type == TaskType.CPU) cpuTaskPool else ioTaskPool

        val job = pool.launch {
            val input = collectDependencyOutputs(task)
            try {
                taskExecutor.execute(task, input)
                onTaskCompleted(task)
            } catch (e: CancellationException) {
                println("Task ${task.id} was cancelled during execution.")
            } catch (e: Exception) {
                println("Task ${task.id} failed: ${e.message}")
            } finally {
                if (activeTasks.decrementAndGet() == 0) {
                    allTasksCompleted.complete(Unit)
                }
                runningJobs.remove(task.id)
            }
        }

        runningJobs[task.id] = job
    }

    private suspend fun collectDependencyOutputs(task: Task): Any? {
        val outputs = mutableListOf<Any?>()
        for ((_, dep) in task.dependencies) {
            while (dep.status != TaskStatus.COMPLETED && !dep.isCancelled) {
                delay(10)
            }
            outputs.add(dep.output?.value)
        }
        return when {
            outputs.isEmpty() -> null
            outputs.size == 1 -> outputs.first()
            else -> outputs
        }
    }

    private suspend fun onTaskCompleted(task: Task) {
        mutex.withLock {
            for ((_, dependent) in task.dependents) {
                dependent.indegree--
                if (dependent.indegree == 0) {
                    if (dependent.weakDependencies.isEmpty()) {
                        readyChannel.send(dependent)
                    } else {
                        launchWeakDependencyWaitAndMaybeEnqueue(dependent)
                    }
                }
            }
        }
    }
}