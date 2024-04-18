package routing

import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.DeliverCallback
import kotlin.system.exitProcess

object ReceiveLogsDirect {
  private val EXCHANGE_NAME = "logs"

  fun main(argv: Array<String>) {
    val factory = ConnectionFactory()
    factory.host = "localhost"

    val connection = factory.newConnection()
    val channel = connection.createChannel()

    channel.exchangeDeclare(EXCHANGE_NAME, "direct")

    val queueName = channel.queueDeclare().queue

    if (argv.isEmpty()) {
      System.err.println("Usage: ReceiveLogsDirect [info] [warning] [error]");
      exitProcess(1);
    }

    // routing key에 해당하는 메시지만 받기 위해 큐를 바인딩한다.
    // To receive messages that have a specific routing key, a queue is bound to the exchange.
    for (severity in argv) {
      channel.queueBind(queueName, EXCHANGE_NAME, severity)
    }

    println(" [*] Waiting for messages. To exit press Ctrl+C")

    val deliverCallback = DeliverCallback { _, delivery ->
      val message = String(delivery.body, charset("UTF-8"))
      println(" [x] Received ${delivery.envelope.routingKey} '$message'")
    }

    channel.basicConsume(queueName, true, deliverCallback) { _ -> }
  }
}
