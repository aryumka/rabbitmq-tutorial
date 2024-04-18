package rpc

import com.rabbitmq.client.ConnectionFactory

class RpcServer {
  private val RPC_QUEUE_NAME = "rpc_queue"

  private fun fib(n: Int): Int {
    if (n == 0) return 0
    return if (n == 1) 1 else fib(n - 1) + fib(n - 2)
  }

  fun send(argv: Array<String>) {
    val factory = ConnectionFactory()
    factory.host = "localhost"

    factory.newConnection().use { connection ->
      connection.createChannel().use { channel ->
        channel.queueDeclare(RPC_QUEUE_NAME, false, false, false, null);
        channel.queuePurge(RPC_QUEUE_NAME)

        channel.basicQos(1)
        val message = getMessage(argv)

        println(" [x] Sent '$message'")
      }
    }
  }

  private fun getRouting(strings: Array<String>): String {
    return if (strings.isEmpty()) "anonymous.info" else strings[0]
  }

  fun getMessage(argv: Array<String>): String =
    if (argv.size < 2) "Hello World!" else argv.sliceArray(1 until argv.size).joinToString(" ")

  companion object {
    @JvmStatic
    fun main(argv: Array<String>) {
      RpcServer().send(argv)
    }
  }
}

