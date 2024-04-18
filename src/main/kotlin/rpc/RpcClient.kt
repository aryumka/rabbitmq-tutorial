package rpc

import com.rabbitmq.client.Channel
import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.DeliverCallback
import kotlin.system.exitProcess

class RpcClient {
  private val EXCHANGE_NAME = "topic_logs"
  private val channel: Channel

  constructor() {
    val factory = ConnectionFactory()
    factory.host = "localhost"

    val connection = factory.newConnection()
    channel = connection.createChannel()
  }

  fun receive(argv: Array<String>) {

    channel.exchangeDeclare(EXCHANGE_NAME, "topic")

    val queueName = channel.queueDeclare().queue

    if (argv.isEmpty()) {
      System.err.println("Usage: ReceiveLogsTopic [binding_key]...")
      exitProcess(1);
    }

    // topic에 해당하는 메시지만 받기 위해 큐를 바인딩한다.
    // *는 하나의 단어를 대체한다. ex) *.info, *.error
    // #은 여러 단어를 대체한다. ex) #.info, *.#
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

  companion object {
    @JvmStatic
    fun main(argv: Array<String>) {
      RpcClient().receive(argv)
    }
  }
}

