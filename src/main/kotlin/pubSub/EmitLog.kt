package pubSub

import com.rabbitmq.client.ConnectionFactory

object EmitLog {
  private val EXCHANGE_NAME = "logs"

  fun main(argv: Array<String>) {
    val factory = ConnectionFactory()
    factory.host = "localhost"

    factory.newConnection().use { connection ->
      connection.createChannel().use { channel ->
        // 지정된 이름으로 fanout 타입의 exchange를 생성한다.
        // fanout: The fanout exchange broadcasts all the messages it receives to all the queues it knows.
        // fanout: 수신한 모든 메시지를 바인딩된 모든 큐에 브로드캐스팅한다.
        // 커넥션을 만든 후 exchange를 선언해야 존재하지 않는 exchange로 메시지를 보내는 것을 막을 수 있다.
        // Declaring an exchange after the connection is established prevents sending messages to a non-existing exchange.
        // 해당 exchange에 바인딩된 큐가 없으면 메시지는 소멸한다.
        // If there are no queues bound to the exchange, the message will be lost.
        channel.exchangeDeclare(EXCHANGE_NAME, "fanout")

        // 프로그램 실행 인자를 메시지로 보낸다.
        // Send the message from the program arguments.
        val message = if (argv.isEmpty()) "info: Hello World!" else argv.joinToString(" ")

        channel.basicPublish(
          // exchange name: 위에 정의한 fanout exchange를 사용하도록 지정한다.
          // exchange name: Use the fanout exchange defined above.
          EXCHANGE_NAME,
          // routing key: routingKey에 지정된 이름의 큐에 메시지를 발행하지만 브로드캐스팅 시에는 모든 큐에 전송이 필요하므로 빈 값으로 설정한다.
          // routing key: The message is published to the queue with the name specified in routingKey, but when broadcasting, it is sent to all queues, so it is set to an empty value.
          "",
          null,
          message.toByteArray()
        )
        println(" [x] Sent '$message'")
      }
    }
  }
}
