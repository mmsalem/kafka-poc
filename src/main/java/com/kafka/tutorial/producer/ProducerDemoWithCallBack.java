package com.kafka.tutorial.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallBack {
    static Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallBack.class);

    public static void main(String[] args) {

        System.out.println("Hello World");

//        create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localHost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

//        create producer
        KafkaProducer<String, String> producer =
                new KafkaProducer<String, String>(properties);

//        send data -Asynchronous
        ProducerRecord<String, String> record =
                new ProducerRecord<String, String>("first_topic", "Hello_wold:From My Application");
        producer.send(record, new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {

                if(e ==null){
                    logger.info("this is after callback");
                    logger.info(""+recordMetadata.partition());
                }
            }
        });
        producer.flush();
        producer.close();
    }
}
