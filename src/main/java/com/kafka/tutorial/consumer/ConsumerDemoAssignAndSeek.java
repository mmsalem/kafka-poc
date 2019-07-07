package com.kafka.tutorial.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoAssignAndSeek {

private static Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignAndSeek.class.getName());

    public static void main(String[] args) {

        String topic ="first_topic";
        //Consumer Config Properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        //Potential Values Are (earliest,latest,NON)
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        //Create Consumer

        KafkaConsumer<String,String> kafkaConsumer =
                new KafkaConsumer<String,String>(properties);

        //assign and seek are mostly used to replay data and fetch message
        TopicPartition readFrom = new TopicPartition("first_topic",0);
        kafkaConsumer.assign(Arrays.asList(readFrom));
        kafkaConsumer.seek(readFrom,15);

        int numberOfmessages =5;
        int numberOfMessagesReadSoFar =0;
        boolean keepOnReading = true;

        //Poll Data From Topic
        //this is will read partition by partition
        while (keepOnReading){
           ConsumerRecords<String,String> consumerRecords =
                   kafkaConsumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String,String> record:consumerRecords) {
                numberOfMessagesReadSoFar +=1;
                logger.info("Partition :- "+record.partition()+", Offset:- "+record.offset());
            logger.info("Key"+record.key() +", value :- "+record.value());
            if(numberOfMessagesReadSoFar == numberOfmessages){
                keepOnReading = false;
                break;
            }
            }
        }
    }
}