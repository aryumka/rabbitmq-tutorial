package publisherConfirm

import com.rabbitmq.client.ConfirmCallback
import com.rabbitmq.client.Connection
import com.rabbitmq.client.ConnectionFactory
import java.time.Duration
import java.util.*
import java.util.concurrent.ConcurrentNavigableMap
import java.util.concurrent.ConcurrentSkipListMap
import java.util.function.BooleanSupplier

object PublisherConfirms {
  const val MESSAGE_COUNT = 50000
  fun createConnection(): Connection {
    val cf = ConnectionFactory()
    cf.host = "localhost"
    cf.username = "guest"
    cf.password = "guest"
    return cf.newConnection()
  }

  @JvmStatic
  fun main(args: Array<String>) {
    publishMessagesIndividually()
    publishMessagesInBatch()
    handlePublishConfirmsAsynchronously()
  }

  // Publisher confirms를 사용하여 메시지를 개별적으로 발행한다.
  fun publishMessagesIndividually() {
    createConnection().use { connection ->
      val channel = connection.createChannel()
      val queue = UUID.randomUUID().toString()
      channel.queueDeclare(queue, false, false, true, null)
      // Publisher confirm은 채널 단위로 단 한번 활성화된다(메시지 단위가 아님).
      channel.confirmSelect()
      val start = System.nanoTime()
      for (i in 0 until MESSAGE_COUNT) {
        val body = i.toString()
        channel.basicPublish("", queue, null, body.toByteArray())
        channel.waitForConfirmsOrDie(5000)
      }
      val end = System.nanoTime()
      println("Published $MESSAGE_COUNT messages individually in ${Duration.ofNanos(end - start).toMillis()} ms")
    }
  }

  // Publisher confirms를 사용하여 메시지를 100개 씩 batch로 묶어서 발행한다. 발행과 확인이 각 batch에 대해 수행된다.
  fun publishMessagesInBatch() {
    createConnection().use { connection ->
      val channel = connection.createChannel()
      val queue = UUID.randomUUID().toString()
      channel.queueDeclare(queue, false, false, true, null)
      channel.confirmSelect()
      val batchSize = 100
      var outstandingMessageCount = 0
      val start = System.nanoTime()
      for (i in 0 until MESSAGE_COUNT) {
        val body = i.toString()
        channel.basicPublish("", queue, null, body.toByteArray())
        outstandingMessageCount++
        if (outstandingMessageCount == batchSize) {
          channel.waitForConfirmsOrDie(5000)
          outstandingMessageCount = 0
        }
      }
      if (outstandingMessageCount > 0) {
        channel.waitForConfirmsOrDie(5000)
      }
      val end = System.nanoTime()
      println("Published $MESSAGE_COUNT messages in batch in ${Duration.ofNanos(end - start).toMillis()} ms")
    }
  }

  // Publisher confirms를 사용하여 메시지를 비동기적으로 처리한다.
  fun handlePublishConfirmsAsynchronously() {
    createConnection().use { connection ->
      val channel = connection.createChannel()
      val queue = UUID.randomUUID().toString()
      channel.queueDeclare(queue, false, false, true, null)
      channel.confirmSelect()
      // outstandingConfirms는 확인되지 않은 메시지를 추적하는 데 사용된다.
      // ConcurrentSkipListMap은 시퀀스 번호 순서로 메시지를 저장할 수 있고 동시성을 지원한다. 메시지의 시퀀스 번호를 키, 메시지의 내용은 값으로 사용된다.
      val outstandingConfirms: ConcurrentNavigableMap<Long, String> =
        ConcurrentSkipListMap()

      // 메시지가 확인되면 outstandingConfirms에서 해당 메시지를 제거한다.
      val cleanOutstandingConfirms =
        ConfirmCallback { sequenceNumber: Long, multiple: Boolean ->
          if (multiple) {
            val confirmed = outstandingConfirms.headMap(
              sequenceNumber, true
            )
            confirmed.clear()
          } else {
            outstandingConfirms.remove(sequenceNumber)
          }
        }

      // ConfirmCallback을 사용하여 확인되지 않은 메시지를 추적하고, nack을 처리한다.
      channel.addConfirmListener(
        cleanOutstandingConfirms
      ) { sequenceNumber: Long, multiple: Boolean ->
        val body = outstandingConfirms[sequenceNumber]
        System.err.format(
          "Message with body %s has been nack-ed. Sequence number: %d, multiple: %b%n",
          body, sequenceNumber, multiple
        )
        cleanOutstandingConfirms.handle(sequenceNumber, multiple)
      }
      val start = System.nanoTime()
      for (i in 0 until MESSAGE_COUNT) {
        val body = i.toString()
        // 다음 발행 시퀀스 번호를 키로 사용하여 메시지를 outstandingConfirms에 저장한다.
        outstandingConfirms[channel.nextPublishSeqNo] = body
        channel.basicPublish("", queue, null, body.toByteArray())
      }

      // 모든 메시지가 확인될 때까지 대기한다. 60초 동안 확인되지 않으면 메시지와 함께 IllegalStateException을 발생시킨다.
      check(
        waitUntil(
          Duration.ofSeconds(60)
        ) { outstandingConfirms.isEmpty() }
      ) { "All messages could not be confirmed in 60 seconds" }
      val end = System.nanoTime()
      println("Published $MESSAGE_COUNT messages and handled confirms asynchronously in ${Duration.ofNanos(end - start).toMillis()} ms")
    }
  }

  fun waitUntil(timeout: Duration, condition: BooleanSupplier): Boolean {
    var waited = 0
    while (!condition.asBoolean && waited < timeout.toMillis()) {
      Thread.sleep(100L)
      waited += 100
    }
    return condition.asBoolean
  }
}
