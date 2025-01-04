package com.example.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerAsyncWithKey {
    public static final Logger logger = LoggerFactory.getLogger(ProducerAsyncWithKey.class.getName());
    public static void main(String[] args) {

        // 토픽 명 설정
        String topicName = "multipart-topic";

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

        for(int seq=0; seq<20; seq++) {
            // ProducerRecord 객체 생성.(토픽과 메세지를 설정할 수 있음)
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, String.valueOf(seq), "hello world" + seq);

            // KafkaProducer message send(return Future)
            kafkaProducer.send(producerRecord, new Callback() {
                @Override // sender thread에서 실행
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null) {
                        logger.info("\n ####### record metadata received ####### \n" +
                                "partition: " + metadata.partition() + "\n" +
                                "offset: " + metadata.offset() + "\n" +
                                "timestamp: " + metadata.timestamp());
                    } else {
                        logger.error(exception.getMessage());
                    }
                }
            });
        }

        // thread 대기(비동기 방식은 그냥 종료될 수도 있기 때문에)
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        kafkaProducer.close();
    }
}
