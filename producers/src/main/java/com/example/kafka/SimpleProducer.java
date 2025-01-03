package com.example.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class SimpleProducer {
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
        kafkaProducer.send(producerRecord);

        // KafkaProducer end
        // 배치가 차야 메세지를 보내기 때문에 send 시 바로 메세지가 전달되지 않기 때문에 브로커에 있는 메세지를 내보내는 작업
        kafkaProducer.flush();
        kafkaProducer.close();
    }
}
