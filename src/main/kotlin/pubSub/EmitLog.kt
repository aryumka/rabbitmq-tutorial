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
        // fanout: 수신한 모든 메시지를 바인딩된 모든 큐에 브로드캐스팅한다.
        // 커넥션을 만든 후 exchange를 선언해야 존재하지 않는 exchange로 메시지를 보내는 것을 막을 수 있다.
        // 해당 exchange에 바인딩된 큐가 없으면 메시지는 소멸한다.
        channel.exchangeDeclare(EXCHANGE_NAME, "fanout")

        // 프로그램 실행 인자를 메시지로 보낸다.
        val message = if (argv.isEmpty()) "info: Hello World!" else argv.joinToString(" ")

        channel.basicPublish(
          // exchange name: 위에 정의한 fanout exchange를 사용하도록 지정한다.
          EXCHANGE_NAME,
          // routing key: routingKey에 지정된 이름의 큐에 메시지를 발행하지만 브로드캐스팅 시에는 모든 큐에 전송이 필요하므로 빈 값으로 설정한다.
          "",
          null,
          message.toByteArray()
        )
        println(" [x] Sent '$message'")
      }
    }
  }
}
