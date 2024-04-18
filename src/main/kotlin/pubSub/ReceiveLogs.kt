package pubSub

import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.DeliverCallback

object ReceiveLogs {
  private val EXCHANGE_NAME = "logs"

  @JvmStatic
  fun main(argv: Array<String>) {
    val factory = ConnectionFactory()
    factory.host = "localhost"

    val connection = factory.newConnection()
    val channel = connection.createChannel()

    // exchangeDeclare는 멱등성을 가진다.
    // exchangeDeclare is idempotent.
    channel.exchangeDeclare(EXCHANGE_NAME, "fanout")

    // log를 받을 컨슈머는 과거의 로그가 아닌 최신 로그만 수신하여야 하므로 매 시작마다 새로운 큐를 생성해야 한다.
    // A consumer should only receive logs created after it started up.
    // 또 컨슈머가 구독을 끊을 경우 큐를 삭제할 수 있어야 한다.
    // Also, when a consumer disconnects, the queue should be deleted.
    // 따라서 큐 이름을 랜덤하게 생성하도록 지정한다.
    // So we need to create a fresh, anonymous queue for each consumer.
    val queueName = channel.queueDeclare().queue

    // 위에서 생성한 큐를 선언된 exchange에 바인딩한다.
    // Bind the queue to the exchange.
    channel.queueBind(queueName, EXCHANGE_NAME, "")

    println(" [*] Waiting for messages. To exit press Ctrl+C")

    val deliverCallback = DeliverCallback { _, delivery ->
      val message = String(delivery.body, charset("UTF-8"))
      println(" [x] Received '$message'")
    }

    channel.basicConsume(queueName, true, deliverCallback) { _ -> }
  }
}
