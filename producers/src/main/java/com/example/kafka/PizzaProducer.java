package com.example.kafka;

import com.github.javafaker.Faker;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;

public class PizzaProducer {
    public static final Logger logger = LoggerFactory.getLogger(PizzaProducer.class.getName());

    // 카프카 프로듀서, 토픽 이름, 메세지 전송 반복 횟수(-1은 무한번), 한건 보낸뒤 쉬는 시간, n건 돌리고 쉬는 시간, n건 돌리는 카운트, 동기 비동기 여부
    public static void sendPizzaMessage(KafkaProducer<String, String> kafkaProducer,
                                        String topicName, int iterCount,
                                        int interIntervalMillis, int intervalMillis,
                                        int intervalCount, boolean sync){

        PizzaMessage pizzaMessage = new PizzaMessage();
        int iterSeq = 0;
        long seed = 2022;
        Random random = new Random(seed);
        Faker faker = Faker.instance(random);

        while (iterSeq++ != iterCount){
            HashMap<String, String> pMessage = pizzaMessage.produce_msg(faker, random, iterSeq);
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, pMessage.get("key"), pMessage.get("message"));

            // 메세지 전송
            sendMessage(kafkaProducer, producerRecord, pMessage, sync);

            // intervalCount가 0보다 크고, n의 배수만큼 반복했을때
            if((intervalCount>0) && (iterSeq % intervalCount == 0)){
                try {
                    logger.info("########## Interval Count: "+ intervalCount +
                            " intervalMillis: "+intervalMillis + "#########");
                    Thread.sleep(intervalMillis); // intervalMillis만큼 스레드 sleep
                } catch (InterruptedException e) {
                    logger.error(e.getMessage());
                }
            }

            // 한건 보낸뒤 쉬는 시간이 0보다 크면, 매 전송마다 그만큼 쉰다.
            if(interIntervalMillis > 0){
                try {
                    logger.info("interIntervalMillis : "+interIntervalMillis);
                    Thread.sleep(interIntervalMillis); // intervalMillis만큼 스레드 sleep
                } catch (InterruptedException e) {
                    logger.error(e.getMessage());
                }
            }
        }
    }

    // 메세지 send하는 실질 로직
    public static void sendMessage(KafkaProducer<String, String> kafkaProducer,
                                   ProducerRecord<String, String> producerRecord,
                                   HashMap<String, String> pMessage, boolean sync){

        if(!sync) { // async 인 경우
            kafkaProducer.send(producerRecord, (metadata, exception) -> {
                if (exception == null) {
                    logger.info("async message: " + pMessage.get("key") +
                            " partition: " + metadata.partition() +
                            " offset: " + metadata.offset());
                } else {
                    logger.error(exception.getMessage());
                }
            });
        }else { // sync 인 경우
            try {
                RecordMetadata metadata = kafkaProducer.send(producerRecord).get();
                logger.info("sync message: "+ pMessage.get("key") +
                        " partition: " + metadata.partition() +
                        " offset: " + metadata.offset());

            } catch (InterruptedException e) {
                logger.error(e.getMessage());
            } catch (ExecutionException e) {
                logger.error(e.getMessage());
            }
        }
    }

    public static void main(String[] args) {

        // 토픽 명 설정
        String topicName = "pizza-topic";

        Properties props = new Properties();

        // bootstrap.servers, key.serializer.class, value.serializer.class property set
        // 접속할 서버의 ip를 적는다.
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.0.29:9092");

        // key serializer 객체 타입을 설정
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // value serializer 객체 타입을 설정
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // acks 설정
//        props.setProperty(ProducerConfig.ACKS_CONFIG, "0");

        // KafkaProducer 객체 생성
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props); // 환경 설정을 해당 카프카 프로듀서가 반영

        // 무한번 메세지 보내고, 한건 보내고 10ms 쉬고, 100건 보낼때 마다 100ms 쉬고, 동기식으로 전송
        sendPizzaMessage(kafkaProducer, topicName,
                -1, 10, 100, 100, true);
        kafkaProducer.close();
    }
}
