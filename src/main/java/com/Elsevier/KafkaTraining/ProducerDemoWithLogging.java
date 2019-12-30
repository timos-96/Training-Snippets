package com.Elsevier.KafkaTraining;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithLogging {

    // Create a kafka producer with 1. Configured Properties and 2. A custom message
    public static void main(String[] args) {

        final Logger logger = LoggerFactory.getLogger(ProducerDemoWithLogging.class);

        Properties properties = new Properties();
        String bootstrapServers = "localhost:9092";
        String keySerializer = StringSerializer.class.getName();
        String valueSerializer = StringSerializer.class.getName();
        properties.setProperty("bootstrap.servers", bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer);
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer);

        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);

        final ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "Hello World");

        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if (e == null){
                    logger.info("Topic: " + recordMetadata.topic() + "\n" +
                            "Partition: " + recordMetadata.partition() + "\n" +
                            "Offset: " + recordMetadata.offset() + "\n");
                }
                else{
                    logger.info("Error");
                }
            }
        });

        producer.flush();

        producer.close();
    }
}
