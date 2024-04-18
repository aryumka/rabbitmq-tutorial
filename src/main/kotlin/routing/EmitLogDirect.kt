package routing

import com.rabbitmq.client.ConnectionFactory

object EmitLogDirect {
  private val EXCHANGE_NAME = "logs"

  @JvmStatic
  fun main(argv: Array<String>) {
    val factory = ConnectionFactory()
    factory.host = "localhost"

    factory.newConnection().use { connection ->
      connection.createChannel().use { channel ->
        // direct exchange를 사용하여 binding key에 routing key가 일치하는 큐로 메시지를 전달한다.
        channel.exchangeDeclare(EXCHANGE_NAME, "direct")

        val severity = getSeverity(argv)
        val message = getMessage(argv)

        channel.basicPublish(
          EXCHANGE_NAME,
          severity,
          null,
          message.toByteArray()
        )
        println(" [x] Sent '$message'")
      }
    }
  }

  fun getSeverity(argv: Array<String>): String =
    if (argv.isEmpty()) "info" else argv[0]

  fun getMessage(argv: Array<String>): String =
    if (argv.size < 2) "Hello World!" else argv.sliceArray(1 until argv.size).joinToString(" ")
}
