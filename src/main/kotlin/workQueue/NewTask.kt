package workQueue

import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.MessageProperties

object NewTask {
  private val QUEUE_NAME = "task_queue"

  @JvmStatic
  fun main(argv: Array<String>) {
    val factory = ConnectionFactory()
    factory.host = "localhost"

    factory.newConnection().use { connection ->
      connection.createChannel().use { channel ->
        val durable = true
        // durable을 true로 설정하면 RabbitMQ가 재시작되어도 큐가 유지된다.
        // If durable is set to true, the queue will survive a broker restart.
        channel.queueDeclare(QUEUE_NAME, durable, false, false, null)
        // 프로그램 실행 인자를 메시지로 보낸다.
        val message = argv.joinToString(" ")

        channel.basicPublish(
          "",
          QUEUE_NAME,
          // 메시지를 디스크에 영속화한다.
          // If the message is persistent, it will be written to disk.
          // 하지만 메시지 무손실을 보장하지 않는다. RabbitMQ가 메시지를 디스크에 저장하기 전에 죽으면 메시지는 손실될 수 있다.
          // However, it does not guarantee that the message won't be lost. If RabbitMQ server stops, the message will be lost.
          // 메시지를 무손실로 보장하려면 publisher confirms를 사용해야 한다. https://www.rabbitmq.com/docs/confirms
          // To ensure that the message is not lost, you need to use publisher confirms.
          MessageProperties.PERSISTENT_TEXT_PLAIN,
          message.toByteArray()
        )
        println(" [x] Sent '$message'")
      }
    }
  }
}
