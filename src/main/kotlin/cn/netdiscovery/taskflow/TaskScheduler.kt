package cn.netdiscovery.taskflow

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ClosedSendChannelException
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
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

    // 关闭标志（原子）
    private val isShuttingDown = AtomicBoolean(false)

    suspend fun start() {
        // 首次为所有任务设置 indegree（以防你之前没有设置）
        mutex.withLock {
            dag.getTasks().values.forEach { it.indegree = it.dependencies.size }
        }

        // consumer: 从 channel 取任务并派发
        cpuTaskPool.launch {
            for (task in readyChannel) {
                // 当 channel 被 close() 时，会跳出循环
                if (allTasksCompleted.isCompleted) break

                println("[Scheduler] got ready task ${task.id}, launching...")
                activeTasks.incrementAndGet()
                launchTask(task)
            }
        }

        // 把初始可执行任务放入 channel
        enqueueInitialTasks()

        // 等待所有任务完成（此处会在合适时机被 complete）
        allTasksCompleted.await()
        println("[Scheduler] all tasks completed, start() returning.")
    }

    /**
     * 优雅关闭：
     * - 标记正在关闭
     * - 关闭 readyChannel（使调度的 consumer 退出循环）
     * - 等待所有正在执行的任务结束（allTasksCompleted）
     */
    suspend fun shutdown() {
        // 设置关闭标志，阻止新增任务入队
        if (!isShuttingDown.compareAndSet(false, true)) {
            // 已经在关闭中 / 关闭完毕，直接等待完成
            allTasksCompleted.await()
            return
        }

        println("[Scheduler] shutdown initiated.")
        // 关闭 channel，停止接受新任务发送（send 会抛异常）
        readyChannel.close()

        // 如果此时没有 active job 和 runningJobs，complete allTasksCompleted 以便 start() 返回
        if (activeTasks.get() == 0 && runningJobs.isEmpty()) {
            if (!allTasksCompleted.isCompleted) allTasksCompleted.complete(Unit)
        }

        // 等待所有任务完成
        allTasksCompleted.await()
        println("[Scheduler] shutdown complete.")
    }

    suspend fun cancelTask(taskId: String) {
        val task = dag.getTaskById(taskId) ?: return
        mutex.withLock {
            cancelTaskAndDependents(task, HashSet())
        }
    }

    private fun cancelTaskAndDependents(task: Task, visited: MutableSet<String>) {
        if (!visited.add(task.id)) return
        if (task.isCancelled) return

        println("[Scheduler] Cancelling task ${task.id}")
        task.cancel()
        runningJobs[task.id]?.cancel()

        task.dependents.values.forEach { cancelTaskAndDependents(it, visited) }
    }

    // 动态添加任务并调度
    suspend fun addAndSchedule(task: Task) {
        // 如果正在关闭，不接受新任务
        if (isShuttingDown.get()) {
            println("[Scheduler] addAndSchedule rejected for ${task.id} because scheduler is shutting down.")
            return
        }

        mutex.withLock {
            dag.addTask(task)
            task.indegree = task.dependencies.size
            enqueueIfReady(task)
        }
    }

    private fun enqueueInitialTasks() {
        dag.getTasks().values.forEach { enqueueIfReady(it) }
    }

    private fun enqueueIfReady(task: Task) {
        val strongReady = task.indegree == 0
        val weakReady = task.weakDependencies.isEmpty() || task.weakDependenciesCompleted

        if (strongReady && weakReady && !task.isCancelled && !isShuttingDown.get()) {
            cpuTaskPool.launch {
                println("[Scheduler] enqueue task ${task.id} -> readyChannel")
                // 使用 send，注意：若 channel 已关闭会抛异常，这里可捕获或让上层处理
                try {
                    readyChannel.send(task)
                } catch (e: ClosedSendChannelException) {
                    println("[Scheduler] readyChannel closed, cannot enqueue ${task.id}")
                }
            }
        } else if (strongReady && task.weakDependencies.isNotEmpty() && !task.weakDependencyWaitStarted) {
            // 若强条件已满足但弱依赖未完成，则启动弱等待（幂等）
            task.weakDependencyWaitStarted = true
            launchWeakDependencyWaitAndMaybeEnqueue(task)
        }
    }

    private fun launchWeakDependencyWaitAndMaybeEnqueue(task: Task) {
        ioTaskPool.launch {
            val startTime = System.currentTimeMillis()
            val timeout = if (task.weakDependencyTimeout > 0L) task.weakDependencyTimeout else 0L

            while (true) {
                val allCompleted = task.weakDependencies.values.all { it.status == TaskStatus.COMPLETED }
                val elapsed = System.currentTimeMillis() - startTime

                if (allCompleted) {
                    println("[Scheduler] weak deps all completed for ${task.id}")
                    break
                }
                if (timeout > 0 && elapsed > timeout) {
                    println("[Scheduler] Weak dependency timeout for task ${task.id}, continuing execution.")
                    break
                }
                delay(50)
            }

            mutex.withLock {
                // 标记弱依赖“已满足”（可能是超时）
                task.weakDependenciesCompleted = true
                if (!task.isCancelled && task.indegree == 0 && !isShuttingDown.get()) {
                    println("[Scheduler] enqueue after weak wait: ${task.id}")
                    try {
                        readyChannel.send(task)
                    } catch (e: ClosedSendChannelException) {
                        println("[Scheduler] readyChannel closed, cannot enqueue ${task.id} after weak wait")
                    }
                } else {
                    println("[Scheduler] after weak wait: ${task.id} not enqueued (cancelled or indegree !=0 or shuttingDown)")
                }
            }
        }
    }

    private fun launchTask(task: Task) {
        if (task.isCancelled) {
            println("[Scheduler] Task ${task.id} is cancelled before execution, skipping.")
            // treat as completed to unblock dependents
            GlobalScope.launch { runBlocking { onTaskCompleted(task) } }
            return
        }

        val pool = if (task.type == TaskType.CPU) cpuTaskPool else ioTaskPool

        val job = pool.launch {
            println("[Scheduler] launching execution job for task ${task.id}")
            // 在真正执行前收集强依赖输出（阻塞等待，只针对强依赖）
            val input = collectDependencyOutputsWithLogs(task)
            try {
                taskExecutor.execute(task, input)
            } catch (ex: Exception) {
                println("[Scheduler] execution exception for ${task.id}: ${ex.message}")
            } finally {
                // 标记完成并通知下游
                try {
                    onTaskCompleted(task)
                } catch (ex: Exception) {
                    println("[Scheduler] onTaskCompleted error for ${task.id}: ${ex.message}")
                }

                // active/tasks 完成检测
                if (activeTasks.decrementAndGet() == 0) {
                    // 如果 channel 里没有任务且没有 running jobs，则可以完成 overall
                    if (readyChannel.isEmpty && runningJobs.isEmpty()) {
                        if (!allTasksCompleted.isCompleted) {
                            allTasksCompleted.complete(Unit)
                        }
                    }
                }
                runningJobs.remove(task.id)
                println("[Scheduler] job finished for ${task.id}")
            }
        }

        runningJobs[task.id] = job
    }

    private suspend fun collectDependencyOutputsWithLogs(task: Task): Any? {
        val outputs = mutableListOf<Any?>()
        for ((_, dep) in task.dependencies) {
            // 只等待强依赖完成
            while (dep.status != TaskStatus.COMPLETED && !dep.isCancelled) {
                // debug log
                println("[Scheduler] task ${task.id} waiting for strong dep ${dep.id} (status=${dep.status})")
                delay(10)
            }
            println("[Scheduler] dependency ${dep.id} completed for ${task.id}, output=${dep.output?.value}")
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
            // snapshot 防止并发修改遍历问题
            val dependentsSnapshot = task.dependents.values.toList()
            for (dependent in dependentsSnapshot) {
                dependent.indegree = (dependent.indegree - 1).coerceAtLeast(0)
                enqueueIfReady(dependent)
            }
        }
    }
}