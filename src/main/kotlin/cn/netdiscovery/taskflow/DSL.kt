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

    // 生成任务的节点
    for (task in dag.getTasks().values) {
        // 输出任务节点
        sb.append("  ${task.id} [label=\"${task.taskName}\"];\n")

        // 生成强依赖关系（不包括弱依赖）
        for (dep in task.dependencies) {
            sb.append("  ${dep.id} -> ${task.id} [label=\"strong\"];\n")
        }

        // 如果需要，可以为弱依赖添加特殊标注，但不生成连接线
        for (weakDep in task.weakDependencies) {
            sb.append("  ${weakDep.id} -> ${task.id} [style=dotted, label=\"weak\"];\n")
        }
    }

    sb.append("}\n")
    return sb.toString()
}
