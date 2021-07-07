package com.github.elitenomad.kafka.tutorone;

import java.util.Properties;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemoWithCallbacks {
    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallbacks.class);
        // Create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);


        // Send data
        for(int i = 0 ; i < 10; i++) {
            // Create a Producer Record
            ProducerRecord<String, String> record =
                    new ProducerRecord<String,String>("framed_topic", "nice to start with problem!!!" + Integer.toString(i));
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if(exception == null){
                        logger.info(
                                "New Meta data \n" +
                                        "TOPIC: " + metadata.topic() +
                                        "Partition: " + metadata.partition() +
                                        "Offset :" + metadata.offset() +
                                        "timeStamp :" + metadata.timestamp()
                        );
                    }else {
                        logger.error("error :", exception);
                    }
                }
            });
        }


        // flush producer
        producer.flush();

        // Close producer
        producer.close();
    }
}
