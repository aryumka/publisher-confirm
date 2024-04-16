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

  fun publishMessagesIndividually() {
    createConnection().use { connection ->
      val ch = connection.createChannel()
      val queue = UUID.randomUUID().toString()
      ch.queueDeclare(queue, false, false, true, null)
      // Publisher confirm은 채널 단위로 단 한번 활성화된다(메시지 단위가 아님).
      ch.confirmSelect()
      val start = System.nanoTime()
      for (i in 0 until MESSAGE_COUNT) {
        val body = i.toString()
        ch.basicPublish("", queue, null, body.toByteArray())
        ch.waitForConfirmsOrDie(5000)
      }
      val end = System.nanoTime()
      println("Published $MESSAGE_COUNT messages individually in ${Duration.ofNanos(end - start).toMillis()} ms")
    }
  }

  fun publishMessagesInBatch() {
    createConnection().use { connection ->
      val ch = connection.createChannel()
      val queue = UUID.randomUUID().toString()
      ch.queueDeclare(queue, false, false, true, null)
      ch.confirmSelect()
      val batchSize = 100
      var outstandingMessageCount = 0
      val start = System.nanoTime()
      for (i in 0 until MESSAGE_COUNT) {
        val body = i.toString()
        ch.basicPublish("", queue, null, body.toByteArray())
        outstandingMessageCount++
        if (outstandingMessageCount == batchSize) {
          ch.waitForConfirmsOrDie(5000)
          outstandingMessageCount = 0
        }
      }
      if (outstandingMessageCount > 0) {
        ch.waitForConfirmsOrDie(5000)
      }
      val end = System.nanoTime()
      System.out.format(
        "Published %,d messages in batch in %,d ms%n",
        MESSAGE_COUNT,
        Duration.ofNanos(end - start).toMillis()
      )
    }
  }

  fun handlePublishConfirmsAsynchronously() {
    createConnection().use { connection ->
      val ch = connection.createChannel()
      val queue = UUID.randomUUID().toString()
      ch.queueDeclare(queue, false, false, true, null)
      ch.confirmSelect()
      val outstandingConfirms: ConcurrentNavigableMap<Long, String> =
        ConcurrentSkipListMap()
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
      ch.addConfirmListener(
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
        outstandingConfirms[ch.nextPublishSeqNo] = body
        ch.basicPublish("", queue, null, body.toByteArray())
      }
      check(
        waitUntil(
          Duration.ofSeconds(60)
        ) { outstandingConfirms.isEmpty() }
      ) { "All messages could not be confirmed in 60 seconds" }
      val end = System.nanoTime()
      System.out.format(
        "Published %,d messages and handled confirms asynchronously in %,d ms%n",
        MESSAGE_COUNT,
        Duration.ofNanos(end - start).toMillis()
      )
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
