package cn.netdiscovery.taskflow

/**
 *
 * @FileName:
 *          cn.netdiscovery.taskflow.DSL
 * @author: Tony Shen
 * @date: 2024/12/10 15:41
 * @version: V1.0 <描述当前版本功能>
 */
fun dag(init: DAG.() -> Unit): DAG {
    val dag = DAG()
    dag.init()
    return dag
}

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