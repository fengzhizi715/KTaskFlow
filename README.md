# KTaskFlow

基于 Kotlin 协程实现的通用任务编排框架，支持有向无环图（DAG）依赖调度，适用于复杂业务流程调度。

---

## 主要特性

- 支持**强依赖**和**弱依赖**，弱依赖可配置超时后自动放行
- 任务类型区分 I/O 密集型和 CPU 密集型，自动调度到不同线程池
- 支持任务取消及取消传播，防止不必要任务执行
- 内置任务超时控制和失败重试机制，支持回滚操作
- 支持异步通知，通过 `CompletableDeferred` 优雅等待任务结果
- 任务优先级调度，基于优先级队列动态调度
- 使用 Kotlin 协程 Channel 实现事件驱动任务调度，避免传统轮询带来的 CPU 空转

---

## 快速开始

```kotlin
val dag = DAG().apply {
    val taskA = task("A", "Load Config", 1, TaskType.IO, SmartGenericTaskAction<Unit, String> {
        println("Task A running")
        delay(300)
        "config done"
    })
    val taskB = task("B", "Initialize DB", 1, TaskType.CPU, SmartGenericTaskAction<String, String> {
        println("Task B received input: $it")
        delay(500)
        "db ready"
    })
    taskB.dependsOn(taskA)
}

val scheduler = TaskScheduler(dag)

runBlocking {
    scheduler.start()
    val result = dag.getTaskResultAsync("B")
    println("Task B result: ${result.value}")
}
```

## 使用说明
* 创建 DAG 并定义任务，支持自定义 TaskAction，使用泛型封装输入输出类型
* 设置任务之间的依赖关系，支持强依赖 .dependsOn() 和弱依赖 .weakDependsOn()
* 使用 TaskScheduler 启动调度，异步执行任务
* 通过 DAG.getTaskResultAsync(taskId) 等待任务完成并获取结果
* 可调用 TaskScheduler.cancelTask(taskId) 取消任务及其后续依赖任务


## 常用示例