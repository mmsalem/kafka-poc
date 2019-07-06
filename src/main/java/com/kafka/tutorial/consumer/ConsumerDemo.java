package com.kafka.tutorial.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;

public class ConsumerDemo {

private static Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getName());

    public static void main(String[] args) {

        String consumerGroupId ="cg-1";
        String topic ="first_topic";
        //Consumer Config Properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,consumerGroupId);
        //Potential Values Are (earliest,latest,NON)
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        //Create Consumer

        KafkaConsumer<String,String> kafkaConsumer =
                new KafkaConsumer<String,String>(properties);

        //Subscribe Consumer To A Topic
//        kafkaConsumer.subscribe(Collections.singleton(topic));
        kafkaConsumer.subscribe(Arrays.asList(topic));

        //Poll Data From Topic
        //this is will read partition by partition
        while (true){
           ConsumerRecords<String,String> consumerRecords =
                   kafkaConsumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String,String> record:consumerRecords) {
                logger.info("Partition :- "+record.partition()+", Offset:- "+record.offset());
            logger.info("Key"+record.key() +", value :- "+record.value());
            }
        }
    }
}
