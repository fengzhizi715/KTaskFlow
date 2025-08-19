package cn.netdiscovery.taskflow

/**
 *
 * @FileName:
 *          cn.netdiscovery.taskflow.DSL
 * @author: Tony Shen
 * @date: 2024/12/10 15:41
 * @version: V1.0 <描述当前版本功能>
 */
// DSL marker
@DslMarker
annotation class DagDsl

@DagDsl
class DAGDsl {
    private val dag = DAG()

    /**
     * DSL 风格的任务定义（避免与 DAG.task(...) 冲突）
     */
    fun defineTask(id: String, init: TaskDsl.() -> Unit): Task {
        val tb = TaskDsl(id, dag)
        tb.init()
        return tb.build()
    }

    internal fun build(): DAG = dag
}

/**
 * Task 构建器：支持
 *  - name / priority / type
 *  - action {} (Unit / 单值 / 列表重载)
 *  - dependsOn / weakDependsOn
 *  - cache(...)
 */
@DagDsl
class TaskDsl(private val id: String, private val dag: DAG) {
    var name: String = id
    var priority: Int = 0
    var type: TaskType = TaskType.IO

    private var actionImpl: TaskAction? = null

    private val strongDeps = mutableListOf<Task>()
    private val weakDeps = mutableListOf<Task>()

    // cache 配置占位（不引入新类型）
    private var cacheKey: String? = null
    private var cacheTtlMillis: Long = 0L
    private var cachePolicy: CachePolicy? = null

    /**
     * 无输入（Unit）版本
     * usage: action { delay(100); "value" }
     */
    fun <O> action(block: suspend () -> O): TaskDsl = apply {
        // SmartGenericTaskAction expects suspend (I)->O, I = Unit
        actionImpl = SmartGenericTaskAction(Unit::class.java, null) { _: Unit -> block() }
    }

    /**
     * 单值输入（显式输入类型）
     * usage: action(String::class.java) { s -> ... }
     */
    fun <I : Any, O> action(inputClass: Class<I>, block: suspend (I) -> O): TaskDsl = apply {
        actionImpl = SmartGenericTaskAction(inputClass, null, block)
    }

    /**
     * 列表输入（元素类型指定）
     * usage: actionList(String::class.java) { list -> ... }
     */
    fun <E : Any, O> actionList(elementClass: Class<E>, block: suspend (List<E>) -> O): TaskDsl = apply {
        // List::class.java as Class<List<E>> 用于传递集合类型信息（类型擦除限制下的折中）
        actionImpl = SmartGenericTaskAction(List::class.java as Class<List<E>>, elementClass, block as suspend (List<E>) -> O)
    }

    /** 缓存配置（直接保存，build 时调用 Task.enableCache(...)） */
    fun cache(key: String, ttlMillis: Long, policy: CachePolicy): TaskDsl = apply {
        cacheKey = key
        cacheTtlMillis = ttlMillis
        cachePolicy = policy
    }

    /** 强依赖，支持 infix 在构建块里写： dependsOn t1 */
    infix fun dependsOn(task: Task): TaskDsl = apply { strongDeps += task }

    /** 弱依赖 */
    infix fun weakDependsOn(task: Task): TaskDsl = apply { weakDeps += task }

    internal fun build(): Task {
        val act = actionImpl ?: error("Task '$id' must define action { }")
        val t = Task(id = id, taskName = name, priority = priority, type = type, taskAction = act)
        // 注册到 DAG
        dag.addTask(t)

        // 按声明顺序建立依赖（dependsOn 添加时顺序已保持）
        strongDeps.forEach { dep -> t.dependsOn(dep) }
        weakDeps.forEach { dep -> t.weakDependsOn(dep) }

        // 应用缓存（如果有）
        cachePolicy?.let { pol ->
            t.enableCache(cacheKey!!, cacheTtlMillis, pol)
        }

        return t
    }
}

/** DSL 入口 */
fun dag(init: DAGDsl.() -> Unit): DAG {
    val dsl = DAGDsl()
    dsl.init()
    return dsl.build()
}

/**
 * 可选的语法糖：让 Task 也可以在 DSL 外部用 infix 方式写依赖。
 * （名称与 Task 类的成员同名不会冲突；这是扩展函数）
 */
infix fun Task.dependsOnTask(other: Task): Task {
    this.dependsOn(other)
    return this
}

infix fun Task.weakDependsOnTask(other: Task): Task {
    this.weakDependsOn(other)
    return this
}
// ===== 一些语法糖：infix & + =====

/**
 * 生成 dot 文件，使用 Graphviz 打开
 */
fun generateDotFile(dag: DAG): String {
    val sb = StringBuilder()
    sb.append("digraph G {\n")
    sb.append("  rankdir=LR; // 从左到右的布局\n")

    // 为不同类型的任务定义不同样式
    sb.append("  node [shape=box, style=filled];\n")
    sb.append("  node [fillcolor=lightblue] [label=\"IO\"];\n")
    sb.append("  node [fillcolor=lightgreen] [label=\"CPU\"];\n")

    // 生成任务的节点
    for (task in dag.getTasks().values) {
        val color = if (task.type == TaskType.IO) "lightblue" else "lightgreen"
        sb.append("  ${task.id} [label=\"${task.taskName}\", fillcolor=$color];\n")
    }

    // 生成依赖关系
    for (task in dag.getTasks().values) {
        // 强依赖
        for (dep in task.dependencies) {
            sb.append("  ${dep.key} -> ${task.id} [label=\"strong\"];\n")
        }

        // 弱依赖
        for (weakDep in task.weakDependencies) {
            sb.append("  ${weakDep.key} -> ${task.id} [style=dashed, label=\"weak\"];\n")
        }
    }

    sb.append("}\n")
    return sb.toString()
}

/**
 * 生成 Mermaid flowchart
 */
fun generateMermaidFlowchart(dag: DAG): String {
    val sb = StringBuilder()
    sb.append("flowchart LR\n")

    // 节点定义：不同类型任务不同颜色
    for (task in dag.getTasks().values) {
        val color = if (task.type == TaskType.IO) "#ADD8E6" else "#90EE90" // lightblue / lightgreen
        sb.append("    ${task.id}[\"${task.taskName}\"]:::${task.type.name}\n")
    }

    // 依赖关系
    for (task in dag.getTasks().values) {
        // 强依赖（实线）
        for (dep in task.dependencies.values) {
            sb.append("    ${dep.id} --> ${task.id}\n")
        }
        // 弱依赖（虚线）
        for (dep in task.weakDependencies.values) {
            sb.append("    ${dep.id} -.-> ${task.id}\n")
        }
    }

    // 样式定义
    sb.append("\n    classDef IO fill:#ADD8E6,stroke:#333,stroke-width:1px;\n")
    sb.append("    classDef CPU fill:#90EE90,stroke:#333,stroke-width:1px;\n")

    return sb.toString()
}