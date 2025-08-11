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

    // consumer job 引用（用于 shutdown 时取消）
    @Volatile
    private var consumerJob: Job? = null

    /**
     * 非阻塞启动（立即返回，调度在后台运行）
     */
    fun startAsync() {
        // 已经启动则直接返回
        if (consumerJob != null && consumerJob!!.isActive) return

        // 首次为所有任务设置 indegree（以防未初始化）
        runBlocking {
            mutex.withLock {
                dag.getTasks().values.forEach { it.indegree = it.dependencies.size }
            }
        }

        // 启动 consumer 协程，在后台消费 readyChannel
        consumerJob = cpuTaskPool.launch {
            for (task in readyChannel) {
                // 如果调度器已经完成（shutdown 完成），退出循环
                if (allTasksCompleted.isCompleted) break

                println("[Scheduler] got ready task ${task.id}, launching...")
                activeTasks.incrementAndGet()
                launchTask(task)
            }
        }

        // 把初始可执行任务放入 channel（异步）
        enqueueInitialTasks()
    }

    /**
     * 阻塞式启动（向后兼容）：会阻塞直到所有任务执行完毕
     */
    suspend fun start() {
        startAsync()
        // 等待所有任务完成信号
        allTasksCompleted.await()
        println("[Scheduler] all tasks completed, start() returning.")
    }

    /**
     * 优雅关闭：
     * - 标记正在关闭（拒绝新增任务）
     * - 不立即关闭 readyChannel，允许已经在进行的弱等待/强等待将它们准备好的任务入队
     * - 等待所有在途任务完成，再关闭 readyChannel、关闭 pools 并 complete allTasksCompleted
     */
    suspend fun shutdown() {
        // 设置关闭标志，阻止新增任务入队
        if (!isShuttingDown.compareAndSet(false, true)) {
            // 已经在关闭中 / 关闭完毕，直接等待完成
            allTasksCompleted.await()
            return
        }

        println("[Scheduler] shutdown initiated.")
        // 不立刻 close readyChannel —— 允许正在等待弱依赖/强依赖的协程把任务入队
        // 等待所有在途（正在执行或已入队但未开始）任务完成
        while (true) {
            // 条件：没有正在执行的任务，并且 channel 中没有待处理的任务
            val running = runningJobs.isNotEmpty()
            val active = activeTasks.get() > 0
            val pendingInChannel = try {
                // Channel 提供 isEmpty 属性 in kotlinx.coroutines
                !readyChannel.isEmpty
            } catch (t: Throwable) {
                // 保险起见：如果不可查询，则认为可能有元素 -> 等待
                true
            }

            if (!running && !active && !pendingInChannel) {
                break
            }
            delay(50)
        }

        // 现在应该可以安全关闭 channel，让 consumer 退出
        try {
            readyChannel.close()
        } catch (_: Exception) {}

        // 等待 consumer/worker 彻底结束（consumer 会在 channel 关闭后结束）
        consumerJob?.cancelAndJoin()

        // 完成 overall 信号
        if (!allTasksCompleted.isCompleted) allTasksCompleted.complete(Unit)

        // 取消 pools 的子任务（可选）
        cpuTaskPool.coroutineContext.cancelChildren()
        ioTaskPool.coroutineContext.cancelChildren()

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
        // 确保等待方不会永远等待：把 completion 标记为取消（若还没完成）
        try {
            if (!task.completion.isCompleted) {
                task.markFailed(CancellationException("Task ${task.id} cancelled"))
            }
        } catch (_: Exception) {
        }

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
            // 重新计算 indegree：只统计尚未完成且未被取消的强依赖
            task.indegree = task.dependencies.values.count { it.status != TaskStatus.COMPLETED && !it.isCancelled }
            enqueueIfReady(task)
        }
    }

    private fun enqueueInitialTasks() {
        dag.getTasks().values.forEach { enqueueIfReady(it) }
    }

    private fun enqueueIfReady(task: Task) {
        val strongReady = task.indegree == 0
        val weakReady = task.weakDependencies.isEmpty() || task.weakDependenciesCompleted

        if (strongReady && weakReady && !task.isCancelled) {
            // 允许在 shuttingDown 过程中（只要 readyChannel 还没关闭）把已准备好的任务入队执行
            cpuTaskPool.launch {
                println("[Scheduler] enqueue task ${task.id} -> readyChannel")
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
                // 尝试入队：即便处于 shuttingDown 状态，也允许将已经准备好的任务加入队列，
                // 因为 shutdown 不立即 close channel（会等到在途任务完成再关闭）
                if (!task.isCancelled && task.indegree == 0) {
                    println("[Scheduler] enqueue after weak wait: ${task.id}")
                    try {
                        readyChannel.send(task)
                    } catch (e: ClosedSendChannelException) {
                        println("[Scheduler] readyChannel closed, cannot enqueue ${task.id} after weak wait")
                    }
                } else {
                    println("[Scheduler] after weak wait: ${task.id} not enqueued (cancelled or indegree !=0)")
                }
            }
        }
    }

    private fun launchTask(task: Task) {
        if (task.isCancelled) {
            println("[Scheduler] Task ${task.id} is cancelled before execution, skipping.")
            // 确保等待者不会挂起
            try {
                if (!task.completion.isCompleted) {
                    task.markFailed(CancellationException("Task ${task.id} cancelled before execution"))
                }
            } catch (_: Exception) {}
            // 仍然通知下游以让依赖链推进
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
            } catch (ex: CancellationException) {
                println("[Scheduler] Task ${task.id} was cancelled during execution.")
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
                    val channelHas = try { !readyChannel.isEmpty } catch (_: Throwable) { true }
                    if (!channelHas && runningJobs.isEmpty()) {
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
        val startTime = System.currentTimeMillis()
        val timeout = if (task.strongDependencyTimeout > 0L) task.strongDependencyTimeout else Long.MAX_VALUE

        for ((_, dep) in task.dependencies) {
            // 只等待强依赖完成，但受 strongDependencyTimeout 限制
            while (dep.status != TaskStatus.COMPLETED && !dep.isCancelled) {
                val elapsed = System.currentTimeMillis() - startTime
                if (elapsed > timeout) {
                    println("[Scheduler] Strong dependency wait timeout for ${task.id} on dep ${dep.id}")
                    break
                }
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

    /**
     * 获取单个任务的 CompletableDeferred<TaskResult>，外部可以 await() 单个任务结果
     */
    fun getTaskResultDeferred(taskId: String): CompletableDeferred<TaskResult>? {
        return dag.getTaskById(taskId)?.completion
    }
}