package com.labs1904.hwe.util

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}

import java.nio.file.Paths
import java.util.{Properties, UUID}

object Util {
  val TRUST_STORE: String = {
    val url = getClass.getResource("/kafka.client.truststore.jks")
    if (url == null) throw new Exception("Unable to find kafka.client.truststore.jks in resources, is the project opened correctly?")
    else Paths.get(url.toURI).toString
  }

  def getProperties(bootstrapServer: String, username: String, password: String): Properties = {
    // Set Properties to be used for Kafka Producer
    val properties = new Properties
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer)
    properties.setProperty(ProducerConfig.ACKS_CONFIG, "1")
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

    properties.put("security.protocol", "SASL_SSL")
    properties.put("sasl.mechanism", "SCRAM-SHA-512")
    properties.put("ssl.truststore.location", TRUST_STORE)
    properties.put("sasl.jaas.config", getScramAuthString(username, password))

    properties
  }

  def getConsumerProperties(bootstrapServer: String, username: String, password: String): Properties = {
    // Set Properties to be used for Kafka Consumer
    val properties = new Properties
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer)
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString)
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    properties.put("security.protocol", "SASL_SSL")
    properties.put("sasl.mechanism", "SCRAM-SHA-512")
    properties.put("ssl.truststore.location", TRUST_STORE)
    properties.put("sasl.jaas.config", getScramAuthString(username, password))

    properties
  }

  def getScramAuthString(username: String, password: String): String = {
   s"""org.apache.kafka.common.security.scram.ScramLoginModule required
   username=\"$username\"
   password=\"$password\";"""
  }
}
