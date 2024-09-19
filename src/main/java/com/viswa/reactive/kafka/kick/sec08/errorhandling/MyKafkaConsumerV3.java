package com.viswa.reactive.kafka.kick.sec08.errorhandling;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

//Retry based on Exception
public class MyKafkaConsumerV3 {

    private static final Logger log = LoggerFactory.getLogger(MyKafkaConsumerV3.class);

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
                .subscription(List.of("order-events"));

        KafkaReceiver.create(options)
                .receive()
                .log()
                .concatMap(MyKafkaConsumerV3::process)
                .subscribe();

    }

    private static Mono<Void> process(ReceiverRecord<Object, Object> receiverRecord){
        return Mono.just(receiverRecord)
                .doOnNext(r -> {
                    if(r.key().equals("5"))
                        throw new RuntimeException("DB is down");
                    var index = ThreadLocalRandom.current().nextInt(1,20);
                    log.info("key: {}, index:{}, value: {}", r.key(), index, r.value().toString().toCharArray()[index]);
                    r.receiverOffset().acknowledge();
                })
                .retryWhen(retrySpec())
                .doOnError(ex -> log.error(ex.getMessage()))
                .onErrorResume(IndexOutOfBoundsException.class, ex -> Mono.fromRunnable(() -> receiverRecord.receiverOffset().acknowledge()))
                .then();
    }

    private static Retry retrySpec(){
        return Retry.fixedDelay(3, Duration.ofSeconds(1))
                .filter(IndexOutOfBoundsException.class::isInstance)
                .onRetryExhaustedThrow((spec, signal) -> signal.failure());
    }
}
