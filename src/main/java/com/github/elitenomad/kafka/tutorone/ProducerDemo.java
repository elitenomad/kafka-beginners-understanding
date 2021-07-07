package com.github.elitenomad.kafka.tutorone;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerDemo {
  public static void main(String[] args) {
    System.out.println("Hello World!!");

    // Create producer properties
    Properties properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    // Create the producer
    KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

    // Create a Producer Record
    ProducerRecord<String, String> record = 
          new ProducerRecord<String,String>("coded_topic", "nice to start with problem!!!");
    // Send data
    producer.send(record);

    // flush producer
    producer.flush();

    // Close producer
    producer.close();
  }
}
