package helloWorld

import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.DeliverCallback

object Receiver {
  // 어떤 큐로부터 메시지를 받을지를 정의한다.
  // define which queue to receive messages from
  private val QUEUE_NAME = "hello"

  @JvmStatic
  fun main(args: Array<String>) {
    val factory = ConnectionFactory()
    factory.host = "localhost"

    val connection = factory.newConnection()
    val channel = connection.createChannel()

    channel.queueDeclare(QUEUE_NAME, false, false, false, null)
    println(" [*] Waiting for messages. To exit press Ctrl+C")

    // DeliverCallback: 서버로부터 푸쉬된 메시지를 수신할 때 호출되는 콜백이다. 실제로 이 메시지를 사용할 때까지 buffer된다.
    // DeliverCallback: A callback that is called when a message is pushed from the server. It is buffered until it is actually used.
    val deliverCallback = DeliverCallback { _, delivery ->
      val message = String(delivery.body, charset("UTF-8"))
      println(" [x] Received '$message'")
    }

    channel.basicConsume(QUEUE_NAME, true, deliverCallback) { _: String? -> }
  }
}
