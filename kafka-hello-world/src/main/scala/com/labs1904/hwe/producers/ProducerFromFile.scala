package com.labs1904.hwe.producers

import com.labs1904.hwe.util.Util
import org.apache.kafka.clients.admin.{AdminClient, NewTopic}
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.slf4j.LoggerFactory

import java.io.{BufferedReader, File, FileReader}
import java.util
import java.util.{Collections, Properties}

object ProducerFromFile {
  lazy val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    logger.info("ProducerFromFile starting...")
    val propertiesOption = for (bootstrapServers <- sys.env.get("HWE_BOOTSTRAP");
      username <- sys.env.get("HWE_USERNAME");
      password <- sys.env.get("HWE_PASSWORD"))
    yield Util.getProperties(bootstrapServers, username, password)
    logger.debug("bootstrap: {}", propertiesOption)

    propertiesOption match {
      case Some(properties) => {
        // Create Kafka Producer
        val admin = AdminClient.create(properties)
        val topics = admin.listTopics().names().get()
        //val topicToCreate = "reviews"
        //createTopicIfDoesNotExist(admin, topics, topicToCreate)
        //writeFileToKafka(properties, "/Users/Kit/Code/amazon_reviews_us_Toys_v1_00_1000000.tsv", topicToCreate)

        val topicToCreate = "word-count"
        createTopicIfDoesNotExist(admin, topics, topicToCreate)
        writeFileToKafka(properties, "/Users/Kit/Code/alice-in-wonderland.txt", topic = topicToCreate)
        println(topics)
      }
      case None => {
        logger.warn("Unable to get properties, did you set HWE_BOOTSTRAP, HWE_USERNAME, and HWE_PASSWORD env vars?")
      }
    }
    logger.info("Done!")
  }

  private def createTopicIfDoesNotExist(admin: AdminClient, topics: util.Set[String], topicToCreate: String): Unit = {
    if (!topics.contains(topicToCreate)) {
      val newTopic = admin.createTopics(Collections.singletonList(new NewTopic(topicToCreate, 3, 2.toShort)))
      newTopic.all().get()
      println(s"Created $topicToCreate topic")
    }
  }

  def writeFileToKafka(properties: Properties, filename: String, topic: String): Unit = {
    val producer = new KafkaProducer[String, String](properties)
    val file = new File(filename)
    val reader = new BufferedReader(new FileReader(file))
    try {
      var line = reader.readLine() // skip the header
      line = reader.readLine()
      while (line != null) {
        if (line.trim.nonEmpty) {
          val record = new ProducerRecord[String, String](topic, line)
          producer.send(record, new Callback() {
            override def onCompletion(recordMetadata: RecordMetadata, e: Exception): Unit = {
              if (e == null) {
                println(
                  s"""
                     |Sent Record: ${record.value()}
                     |Topic: ${recordMetadata.topic()}
                     |Partition: ${recordMetadata.partition()}
                     |Offset: ${recordMetadata.offset()}
                     |Timestamp: ${recordMetadata.timestamp()}
                   """.stripMargin)
              } else println("Error while producing", e)
            }
          })
        }
        line = reader.readLine()
      }
    } finally {
      reader.close()
      producer.close()
    }
  }
}
