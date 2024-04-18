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

    // prefetchCount: 한 컨슈머가 처리하는 메시지의 개수. 여기서는 1로 설정한다. 디폴트
    // 한 번에 하나의 메시지만 받도록 한다. Round-robin 방식으로 메시지를 분배한다.
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
        // 이러한 이슈를 디버깅하기 위해서는
        // sudo rabbitmqctl list_queues name messages_ready messages_unacknowledged
        // rabbitmqctl.bat list_queues name messages_ready messages_unacknowledged -- windows
        // 또는 RabbitMQ Management UI에서 확인할 수 있다.
        channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
      }
      // autoAck를 true로 설정하면 RabbitMQ에 메시지를 전달한 후 바로 삭제한다. 디폴트는 true이다.
      // autoAck를 false로 설정하면 컨슈머가 메시지 처리를 완료했을 때 메시지가 삭제되고 unAck상태의 메시지들은 재전송된다.
      // 메시지 처리가 완료되면 RabbitMQ에게 메시지 처리가 완료되었음을 알린다.
    }
    val autoAck = false
    channel.basicConsume(QUEUE_NAME, autoAck, deliverCallback) { consumerTag -> }
  }

  //시간이 걸리는 작업 수행을 모방하기 위해 메시지의 각 . 개수마다 1초를 기다린다.
  fun doWork(task: String) {
    for (char in task.toCharArray()) {
      if (char == '.') {
        Thread.sleep(1000)
      }
    }
  }
}
