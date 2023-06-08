package com.labs1904.hwe.consumers

import com.labs1904.hwe.producers.ProducerFromFile.logger
import com.labs1904.hwe.util.Util
import net.liftweb.json.DefaultFormats
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.slf4j.LoggerFactory

import java.time.Duration
import java.util.Arrays

object Consumer {
  implicit val formats: DefaultFormats.type = DefaultFormats
  private val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    logger.info("Consumer$ starting...")
    val propertiesOption = for (bootstrapServers <- sys.env.get("HWE_BOOTSTRAP");
                                username <- sys.env.get("HWE_USERNAME");
                                password <- sys.env.get("HWE_PASSWORD"))
        yield Util.getConsumerProperties(bootstrapServers, username, password)

    propertiesOption match {
      case Some(properties) => {
        // Create the KafkaConsumer
        val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](properties)

        // Subscribe to the topic
        consumer.subscribe(Arrays.asList("connection-test"))

        var tries = 2
        while (tries > 0) {
          // poll for new data
          val duration: Duration = Duration.ofMillis(100)
          val records: ConsumerRecords[String, String] = consumer.poll(duration)
          if (records.count() > 0 || tries == 1) tries -= 1

          records.forEach((record: ConsumerRecord[String, String]) => {
            // Retrieve the message from each record
            val message = record.value()
            logger.info(s"Message Received: $message")
          })
        }
      }
      case None => {
        logger.warn("Unable to get properties, did you set HWE_BOOTSTRAP, HWE_USERNAME, and HWE_PASSWORD env vars?")
      }
    }
    logger.info("Done!")
  }
}
