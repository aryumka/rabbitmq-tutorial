package rpc

import com.rabbitmq.client.*
import java.util.*
import java.util.concurrent.CompletableFuture
import kotlin.system.exitProcess

class RpcClient: AutoCloseable {
  private val connection: Connection
  private val channel: Channel
  private val requestQueueName = "rpc_queue"

  init {
    val factory = ConnectionFactory()
    factory.host = "localhost"

    connection = factory.newConnection()
    channel = connection.createChannel()
  }

  //RPC 요청을 보내고 응답을 받는다.
  //Send an RPC request and receive a response.
  fun call(message: String): String {
    // 응답과 요청을 매핑하기 위한 고유한 ID를 생성한다.
    // Create a unique ID to map responses and requests.
    val corrId: String = UUID.randomUUID().toString()

    // 현재 클라이언트에서 응답을 받는 큐이므로 익명으로 생성.
    // server-named, exclusive, autodelete, non-durable queue 이므로 클라이언트가 죽으면 큐가 삭제된다.
    // server-named, exclusive, autodelete, non-durable queue so it will be deleted when the client disconnects.
    // 요청을 보낸 후 죽으면 응답을 어떻게 받아야 할까?
    // What if the client dies after sending the request?
    val replyQueueName = channel.queueDeclare().queue

    val props = AMQP.BasicProperties.Builder()
      // 응답을 받기 위한 고유한 ID를 지정한다.
      // Specify a unique ID to receive responses.
      .correlationId(corrId)
      // 응답을 받을 큐를 지정한다.
      // Specify the queue to receive responses.
      .replyTo(replyQueueName)
      .build()

    // 응답을 받기 위한 ID와 큐를 지정하여 rpc_queue를 통해 서버에 요청 메시지를 발행한다.
    channel.basicPublish("", requestQueueName, props, message.toByteArray())

    // 분리된 쓰레드에서 응답을 받으므로 CompletableFuture를 사용한다.
    // Use CompletableFuture because the response is received in a separate thread.
    val response = CompletableFuture<String>()

    val consumerTag = channel.basicConsume(replyQueueName, true,
      { _, delivery: Delivery ->
        // 응답 메시지의 고유한 ID와 요청 메시지의 고유한 ID가 일치하는지 확인한다.
        // Check if the unique ID of the response message matches the unique ID of the request message.
        if (delivery.properties.correlationId == corrId) {
          response.complete(String(delivery.body))
        }
      }
    ) { _ -> }

    val result = response.get()
    channel.basicCancel(consumerTag)
    return result
  }

  override fun close() {
    connection.close()
  }
}
fun main(argv: Array<String>) {
  RpcClient().use { fibonacciRpc ->
    for (i in 0..31) {
      val message = i.toString()
      println(" [x] Requesting fib($message)")
      val response: String = fibonacciRpc.call(message)
      println(" [.] Got '$response'")
    }
  }
}

