package topics

import com.rabbitmq.client.ConnectionFactory

object EmitLogTopic {
  private val EXCHANGE_NAME = "topic_logs"

  @JvmStatic
  fun main(argv: Array<String>) {
    val factory = ConnectionFactory()
    factory.host = "localhost"

    factory.newConnection().use { connection ->
      connection.createChannel().use { channel ->
        // topic exchange를 사용하여 routing key가 일치하는 큐로 메시지를 전달한다.
        // Deliver messages to queues based on the routing key.
        channel.exchangeDeclare(EXCHANGE_NAME, "topic")
        val routingKey: String = getRouting(argv)
        val message = getMessage(argv)

        channel.basicPublish(
          EXCHANGE_NAME,
          routingKey,
          null,
          message.toByteArray()
        )
        println(" [x] Sent '$message'")
      }
    }
  }

  private fun getRouting(strings: Array<String>): String {
    return if (strings.isEmpty()) "anonymous.info" else strings[0]
  }

  fun getMessage(argv: Array<String>): String =
    if (argv.size < 2) "Hello World!" else argv.sliceArray(1 until argv.size).joinToString(" ")
}
