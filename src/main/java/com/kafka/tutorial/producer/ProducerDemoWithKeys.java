package com.kafka.tutorial.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithKeys {

    private static Logger logger = LoggerFactory.getLogger(ProducerDemoWithKeys.class);

    //use case 2
    // by providing a key we guaranty same key always to same partition.

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

        String topic ="first_topic";
        String message ="greetings";
        for (int i = 0; i <10; i++) {

            String key ="id_"+i;

            //        send data -Asynchronous
            ProducerRecord<String, String> record =
                    new ProducerRecord<String, String>(topic,key, message+i);

            logger.info("key=======>"+key);

            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {

                    if(e ==null){
                        logger.info("this is after callback");
                        logger.info("OFFSET=======>  "+recordMetadata.offset());
                        logger.info("Partition======>  "+recordMetadata.partition());
                    }
                }
            });
            producer.flush();
        }
        producer.close();
    }
}
