package com.example.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class SimpleProducerSync {
    public static final Logger logger = LoggerFactory.getLogger(SimpleProducerSync.class.getName());
    public static void main(String[] args) {

        // 토픽 명 설정
        String topicName = "simple-topic";

        // KafkaProducer Configuration setting
        // test key: null, value: "hello world"

        Properties props = new Properties();

        // bootstrap.servers, key.serializer.class, value.serializer.class property set
        // 접속할 서버의 ip를 적는다.
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.0.29:9092");

        // key serializer 객체 타입을 설정
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // value serializer 객체 타입을 설정
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // KafkaProducer 객체 생성
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props); // 환경 설정을 해당 카프카 프로듀서가 반영

        // ProducerRecord 객체 생성.(토픽과 메세지를 설정할 수 있음)
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, "hello world");

        // KafkaProducer message send(return Future)
        try {
            RecordMetadata recordMetadata = kafkaProducer.send(producerRecord).get();
            logger.info("\n ####### record metadata received ####### \n" +
                    "partition: " + recordMetadata.partition() + "\n" +
                    "offset: " + recordMetadata.offset() + "\n" +
                    "timestamp: " + recordMetadata.timestamp());

        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } finally { // 반드시 kafka close 해준다.
            kafkaProducer.close();
        }
    }
}
