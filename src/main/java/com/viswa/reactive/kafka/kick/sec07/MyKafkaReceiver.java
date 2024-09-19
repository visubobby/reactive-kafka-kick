package com.viswa.reactive.kafka.kick.sec07;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.List;
import java.util.Map;


public class MyKafkaReceiver {

    private static final Logger log = LoggerFactory.getLogger(MyKafkaReceiver.class);

    public static void main(String[] args) {
        var consumerConfig = Map.<String,Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.GROUP_ID_CONFIG, "demo-group-1234",
                ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "1",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest",
                ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000,  // 10 seconds
                ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 3000,  // 3 seconds
                ConsumerConfig.METADATA_MAX_AGE_CONFIG, "30000"   // 30 seconds
        );
        var options  = ReceiverOptions.create(consumerConfig)
                .addAssignListener(c -> {
                    c.forEach(r -> log.info("assigned {}", r.position()));
                   // c.forEach(r -> r.seek(r.position() - 2));
                    c.stream()
                            .filter(r -> r.topicPartition().partition() ==2)
                            .findFirst()
                            .ifPresent(r -> r.seek(r.position()-2));
                })
                .subscription(List.of("order-events"));

        KafkaReceiver.create(options).receive()
                .doOnNext( r -> log.info("key: {}, value: {}", r.key(), r.value()))
                .doOnNext(r -> r.receiverOffset().acknowledge())
                .subscribe();
    }
}
