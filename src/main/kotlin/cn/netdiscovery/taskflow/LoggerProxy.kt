package cn.netdiscovery.taskflow

/**
 *
 * @FileName:
 *          cn.netdiscovery.taskflow.LoggerProxy
 * @author: Tony Shen
 * @date: 2025/2/14 10:36
 * @version: V1.0 <描述当前版本功能>
 */

interface Logger {
    fun i(msg: String, tag: String? = "ktaskflow")
    fun v(msg: String, tag: String? = "ktaskflow")
    fun d(msg: String, tag: String? = "ktaskflow")
    fun w(msg: String, tag: String? = "ktaskflow", tr: Throwable?)
    fun e(msg: String, tag: String? = "ktaskflow", tr: Throwable?)
}

object DefaultLogger: Logger {
    override fun i(msg: String, tag: String?) {
        println("$tag $msg")
    }

    override fun v(msg: String, tag: String?) {
        println("$tag $msg")
    }

    override fun d(msg: String, tag: String?) {
        println("$tag $msg")
    }

    override fun w(msg: String, tag: String?, tr: Throwable?) {
        tr?.printStackTrace()
        println("$tag $msg")
    }

    override fun e(msg: String, tag: String?, tr: Throwable?) {
        tr?.printStackTrace()
        System.err.println("$tag $msg")
    }
}

object LoggerProxy {

    private lateinit var mLogger: Logger

    fun initLogger(logger: Logger) {
        mLogger = logger
    }

    fun getLogger(): Logger {
        return if (this::mLogger.isInitialized) {
            mLogger
        } else {
            DefaultLogger
        }
    }
}