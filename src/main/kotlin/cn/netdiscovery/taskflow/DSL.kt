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
