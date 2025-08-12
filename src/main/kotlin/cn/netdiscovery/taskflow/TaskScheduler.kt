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
class TaskScheduler(
    private val dag: DAG,
    private val scope: CoroutineScope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
) {
    private val readyChannel = Channel<Task>(Channel.UNLIMITED)
    private val mutex = Mutex()

    private val cpuTaskPool = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private val ioTaskPool = CoroutineScope(SupervisorJob() + Dispatchers.IO)

    // enqueuedTasks 用于防止重复入队（key: task.id）
    private val enqueuedTasks = ConcurrentHashMap.newKeySet<String>()

    // 把 enqueueIfReady 封装成给 TaskExecutor 的回调
    private val taskExecutor: TaskExecutor = TaskExecutor(mutex, scope) { task ->
        // enqueueCallback：在调度器上下文中尝试把任务放入 readyChannel（先检查状态）
        scope.launch {
            mutex.withLock {
                if (isShuttingDown.get() || task.isCancelled) {
                    println("[Scheduler] enqueueCallback: skipping enqueue for ${task.id} (cancelled/shuttingDown)")
                    return@withLock
                }

                if (task.indegree == 0) {
                    val added = enqueuedTasks.add(task.id)
                    if (added) {
                        try {
                            readyChannel.send(task)
                        } catch (e: ClosedSendChannelException) {
                            println("[Scheduler] readyChannel closed, cannot enqueue ${task.id} (from enqueueCallback)")
                        }
                    } else {
                        println("[Scheduler] enqueueCallback: task ${task.id} already enqueued, skipping")
                    }
                } else {
                    println("[Scheduler] enqueueCallback: skipping enqueue for ${task.id} (indegree != 0)")
                }
            }
        }
    }

    private val activeTasks = AtomicInteger(0)
    private val allTasksCompleted = CompletableDeferred<Unit>()

    private val runningJobs = ConcurrentHashMap<String, Job>()
    private val isShuttingDown = AtomicBoolean(false)

    @Volatile
    private var consumerJob: Job? = null

    /**
     * 非阻塞启动
     */
    fun startAsync() {
        if (consumerJob != null && consumerJob!!.isActive) return

        // 首次为所有任务设置 indegree（以防未初始化）
        runBlocking {
            mutex.withLock {
                dag.getTasks().values.forEach { it.indegree = it.dependencies.size }
            }
        }

        // consumer 运行在 cpuTaskPool（可被 shutdown 取消）
        consumerJob = cpuTaskPool.launch {
            for (task in readyChannel) {
                if (allTasksCompleted.isCompleted) break
                // 再次快速检查：跳过已经取消或已完成的任务
                if (task.isCancelled || task.status == TaskStatus.COMPLETED) {
                    // 清理 enqueued 标记（若因为某些原因任务已经完成但还在 channel 中）
                    enqueuedTasks.remove(task.id)
                    continue
                }
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
        allTasksCompleted.await()
        println("[Scheduler] all tasks completed, start() returning.")
    }

    /**
     * 优雅关闭：
     */
    suspend fun shutdown() {
        if (!isShuttingDown.compareAndSet(false, true)) {
            allTasksCompleted.await()
            return
        }

        println("[Scheduler] shutdown initiated.")
        // 等待所有在途条件：没有 running jobs、activeTasks==0、channel 为空且所有任务处于终态
        while (true) {
            val running = runningJobs.isNotEmpty()
            val active = activeTasks.get() > 0
            val pendingInChannel = try {
                !readyChannel.isEmpty
            } catch (t: Throwable) {
                true
            }
            val allFinished = mutex.withLock { allTasksAreInFinalState() }

            if (!running && !active && !pendingInChannel && allFinished) {
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

        if (!allTasksCompleted.isCompleted) allTasksCompleted.complete(Unit)

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
        // 清理 enqueued 标记，防止被再次入队
        enqueuedTasks.remove(task.id)

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

    /**
     * 判断是否可以入队（此方法会在一个协程中被调用并持有 mutex）
     *
     * 关键：入队前用 enqueuedTasks.add(task.id) 做幂等检查，防止重复入队。
     */
    private fun enqueueIfReady(task: Task) {
        scope.launch {
            mutex.withLock {
                // 提前启动弱依赖等待（只要存在弱依赖且还未启动）
                if (task.weakDependencies.isNotEmpty() && !task.weakDependencyWaitStarted && !task.isCancelled && !isShuttingDown.get()) {
                    task.weakDependencyWaitStarted = true
                    launchWeakDependencyWaitAndMaybeEnqueue(task)
                }

                val strongReady = task.indegree == 0
                val weakReady = task.weakDependencies.isEmpty() || task.weakDependenciesCompleted

                if (strongReady && weakReady && !task.isCancelled && !isShuttingDown.get()) {
                    // 幂等入队：确保一个任务只被真正 send 一次
                    val added = enqueuedTasks.add(task.id)
                    if (added) {
                        try {
                            println("[Scheduler] enqueue task ${task.id} -> readyChannel")
                            readyChannel.send(task)
                        } catch (e: ClosedSendChannelException) {
                            println("[Scheduler] readyChannel closed, cannot enqueue ${task.id}")
                        }
                    } else {
                        println("[Scheduler] enqueueIfReady: task ${task.id} already enqueued, skipping")
                    }
                } else if (strongReady && task.weakDependencies.isNotEmpty() && !task.weakDependencyWaitStarted) {
                    // 若强条件已满足但弱依赖未完成且未启动弱等待，则启动（幂等由上面提前启动分支保障）
                    task.weakDependencyWaitStarted = true
                    launchWeakDependencyWaitAndMaybeEnqueue(task)
                }
            }
        }
    }

    private fun launchWeakDependencyWaitAndMaybeEnqueue(task: Task) {
        ioTaskPool.launch {// 这里使用 ioTaskPool ，是因为只是做轮询和延时等待，不占 CPU，放 cpuTaskPool 会浪费资源
            val startTime = System.currentTimeMillis()
            val timeout = if (task.weakDependencyTimeout > 0L) task.weakDependencyTimeout else 0L

            while (true) {
                // 如果任务在等待过程中被取消，立即停止等待
                if (task.isCancelled) {
                    println("[Scheduler] weak wait aborted because task ${task.id} was cancelled.")
                    return@launch
                }

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
                task.weakDependenciesCompleted = true
                // 只有在未取消并且 indegree==0 的情况下才尝试入队
                if (!task.isCancelled && task.indegree == 0 && !isShuttingDown.get()) {
                    val added = enqueuedTasks.add(task.id)
                    if (added) {
                        try {
                            println("[Scheduler] enqueue after weak wait: ${task.id}")
                            readyChannel.send(task)
                        } catch (e: ClosedSendChannelException) {
                            println("[Scheduler] readyChannel closed, cannot enqueue ${task.id} after weak wait")
                        }
                    } else {
                        println("[Scheduler] enqueue after weak wait: task ${task.id} already enqueued, skipping")
                    }
                } else {
                    println("[Scheduler] after weak wait: ${task.id} not enqueued (cancelled/shuttingDown/indegree !=0)")
                }
            }
        }
    }

    private fun launchTask(task: Task) {

        // 在任务执行前添加状态检查
        if (task.status == TaskStatus.FAILED || task.status == TaskStatus.CANCELLED || task.status == TaskStatus.TIMED_OUT) {
            println("[Scheduler] Task ${task.id} in ${task.status} state, skipping execution.")
            // 清理 enqueued 标记（任务不会再被执行，允许重试/重加时重新入队）
            enqueuedTasks.remove(task.id)
            return
        }

        if (task.isCancelled) {
            println("[Scheduler] Task ${task.id} is cancelled before execution, skipping.")
            try {
                if (!task.completion.isCompleted) {
                    task.markFailed(CancellationException("Task ${task.id} cancelled before execution"))
                }
            } catch (_: Exception) {}
            // 清理 enqueued 标记
            enqueuedTasks.remove(task.id)
            // 通知下游以让依赖链推进
            scope.launch {
                try { onTaskCompleted(task) } catch (ex: Exception) { println("[Scheduler] onTaskCompleted error for ${task.id}: ${ex.message}") }
            }
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
                // 在任务执行完成（无论成功/失败/超时/取消）时清理 enqueued 标记，允许该任务在后续被重新入队（例如重试）
                enqueuedTasks.remove(task.id)

                // 标记完成并通知下游
                try {
                    onTaskCompleted(task)
                } catch (ex: Exception) {
                    println("[Scheduler] onTaskCompleted error for ${task.id}: ${ex.message}")
                }

                // active/tasks 完成检测
                if (activeTasks.decrementAndGet() == 0) {
                    val channelHas = try { !readyChannel.isEmpty } catch (_: Throwable) { true }
                    if (!channelHas && runningJobs.isEmpty()) {
                        if (!allTasksCompleted.isCompleted) {
                            // 额外检查：确认所有任务处于终态
                            val allFinal = mutex.withLock { allTasksAreInFinalState() }
                            if (allFinal && !allTasksCompleted.isCompleted) {
                                allTasksCompleted.complete(Unit)
                            }
                        }
                    }
                }
                runningJobs.remove(task.id)
                println("[Scheduler] job finished for ${task.id}")
            }
        }

        runningJobs[task.id] = job
    }

    /**
     * 收集强依赖输出：如果上游最终失败（FAILED/CANCELLED/TIMED_OUT）且不会再成功，则尽快返回。
     */
    private suspend fun collectDependencyOutputsWithLogs(task: Task): Any? {
        val outputs = mutableListOf<Any?>()
        val startTime = System.currentTimeMillis()
        val timeout = if (task.strongDependencyTimeout > 0L) task.strongDependencyTimeout else Long.MAX_VALUE

        for ((_, dep) in task.dependencies) {
            // 只等待强依赖完成，但受 strongDependencyTimeout 限制
            while (dep.status != TaskStatus.COMPLETED && !dep.isCancelled) {
                // 如果上游已经进入无法恢复的终态，立即跳出等待
                if (dep.status == TaskStatus.FAILED || dep.status == TaskStatus.TIMED_OUT) {
                    println("[Scheduler] dependency ${dep.id} is in terminal state ${dep.status}, won't wait further for ${task.id}")
                    break
                }
                val elapsed = System.currentTimeMillis() - startTime
                if (elapsed > timeout) {
                    println("[Scheduler] Strong dependency wait timeout for ${task.id} on dep ${dep.id}")
                    break
                }
                println("[Scheduler] task ${task.id} waiting for strong dep ${dep.id} (status=${dep.status})")
                delay(10)
            }
            println("[Scheduler] dependency ${dep.id} completed/terminal for ${task.id}, output=${dep.output?.value}")
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
                // 跳过已完成或已取消的下游，避免重复入队
                if (dependent.status == TaskStatus.COMPLETED || dependent.isCancelled) {
                    continue
                }

                // 只对“强依赖”执行 indegree--（修复：不要对弱依赖进行 indegree 减少）
                val isStrongDep = dependent.dependencies.containsKey(task.id)
                val isWeakDep = dependent.weakDependencies.containsKey(task.id)

                if (isStrongDep) {
                    dependent.indegree = (dependent.indegree - 1).coerceAtLeast(0)
                }

                // 如果这是一个弱依赖的完成，检查该 dependent 的所有弱依赖是否都完成
                if (isWeakDep) {
                    val allWeakCompleted = dependent.weakDependencies.values.all { it.status == TaskStatus.COMPLETED }
                    if (allWeakCompleted) {
                        dependent.weakDependenciesCompleted = true
                    }
                }

                // 尝试入队（enqueueIfReady 会做幂等检查和弱依赖/强依赖判断）
                enqueueIfReady(dependent)
            }
        }
    }

    fun getTaskResultDeferred(taskId: String): CompletableDeferred<TaskResult>? {
        return dag.getTaskById(taskId)?.completion
    }

    // ----------------- 辅助方法 -----------------
    private fun allTasksAreInFinalState(): Boolean {
        val tasks = dag.getTasks().values
        return tasks.all { it.status == TaskStatus.COMPLETED
                || it.status == TaskStatus.FAILED
                || it.status == TaskStatus.CANCELLED
                || it.status == TaskStatus.TIMED_OUT }
    }
}