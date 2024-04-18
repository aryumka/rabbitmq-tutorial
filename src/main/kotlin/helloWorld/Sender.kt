package helloWorld

import com.rabbitmq.client.ConnectionFactory

object Sender {
  private val QUEUE_NAME = "hello"

  @JvmStatic
  fun main(args: Array<String>) {
    val factory = ConnectionFactory()
    factory.host = "localhost"

    // Connection: 소켓 연결이 추상화된 객체이다. RabbitMQ는 물리적으로 단일 소켓을 통한 TCP 연결을 사용한다.
    // Connection: An abstracted object for socket connections. RabbitMQ uses a single socket for TCP connections.

    // Connection과 Channel은 모두 java.lang.AutoCloseable을 상속받고 있다. 따라서 use를 사용하면 블록이 리턴될 때 close를 호출해준다.
    // Connection and Channel both inherit java.lang.AutoCloseable. Therefore, using 'use' will call close when the block returns.
    factory.newConnection().use { connection ->
      // Channel: Connection을 공유하는 논리적인 개념의 다중화된 경량 연결이다. 실제 api가 메시지를 보내고 받는 작업을 수행한다. Connection의 생명주기에 종속적이다.
      // Channel: A lightweight connection that is a logical concept that shares a Connection. The actual API sends and receives messages through Channel. It depends on the lifecycle of the Connection.
      connection.createChannel().use { channel ->
        // 큐를 선언한다. 이미 존재하는 큐를 선언하면 무시된다. 큐 생성은 멱등성(idempotent)을 가진다.
        // Declare a queue. If a queue that already exists is declared, it is ignored. Queue creation is idempotent.
        channel.queueDeclare(QUEUE_NAME, false, false, false, null)
        val message = "Hello World!"
        // 메시지 내용은 byte array여야 한다.
        // The message content must be a byte array.
        channel.basicPublish("", QUEUE_NAME, null, message.toByteArray())
        println(" [x] Sent '$message'")
      }
    }
  }
}
