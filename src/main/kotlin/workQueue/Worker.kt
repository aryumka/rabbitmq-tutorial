package workQueue

import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.DeliverCallback

object Worker {
  private val QUEUE_NAME = "task_queue"

  @JvmStatic
  fun main(argv: Array<String>) {
    val factory = ConnectionFactory()
    factory.host = "localhost"

    val connection = factory.newConnection()
    val channel = connection.createChannel()

    val durable = true
    channel.queueDeclare(QUEUE_NAME, durable, false, false, null)
    println(" [*] Waiting for messages. To exit press Ctrl+C")

    // prefetchCount: 한 컨슈머가 처리하는 메시지의 개수. 여기서는 1로 설정한다. 디폴트 개수는 250이다.
    // prefetchCount: The number of messages that the consumer can process. Here, it is set to 1. The default number is 250.
    // 한 번에 하나의 메시지만 받도록 한다. Round-robin 방식이 아닌 fair dispatch를 위해 prefetchCount를 1로 설정한다.
    // Set to receive only one message at a time. Set prefetchCount to 1 for fair dispatch, not round-robin.
    channel.basicQos(1)

    val deliverCallback = DeliverCallback { _, delivery ->
      val message = String(delivery.body, charset("UTF-8"))
      println(" [x] Received '$message'")
      try {
        doWork(message)
      } catch (e: InterruptedException) {
        e.printStackTrace()
      } finally {
        println(" [x] Done")
        // 만약 basicAck을 호출하지 않으면 unAck 상태의 메시지들은 재전송되지 못하고 메모리 이슈가 발생한다.
        // If basicAck is not called, unAck messages cannot be resent and memory issues occur.
        channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
      }
      // autoAck를 true로 설정하면 RabbitMQ에 메시지를 전달한 후 바로 삭제한다. 디폴트는 true이다.
      // If autoAck is set to true, RabbitMQ deletes the message immediately after delivering it. The default is true.
      // autoAck를 false로 설정하면 컨슈머가 메시지 처리를 완료했을 때 메시지가 삭제되고 unAck상태의 메시지들은 재전송된다.
      // If autoAck is set to false, the message is deleted when the consumer completes processing, and unAck messages are resent.
      // 메시지 처리가 완료되면 RabbitMQ에게 메시지 처리가 완료되었음을 알린다.
      // When message processing is complete, notify RabbitMQ that message processing is complete.
    }
    val autoAck = false
    channel.basicConsume(QUEUE_NAME, autoAck, deliverCallback) { consumerTag -> }
  }

  // 시간이 걸리는 작업 수행을 모방하기 위해 메시지의 각 . 개수마다 1초를 기다린다.
  // To mimic time-consuming work, wait 1 second for each . in the message.
  fun doWork(task: String) {
    for (char in task.toCharArray()) {
      if (char == '.') {
        Thread.sleep(1000)
      }
    }
  }
}
