package com.viswa.reactive.kafka.kick.sec08.errorhandling;


import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

import java.time.Duration;
import java.util.Map;

public class MyKafkaProducer {

    private static final Logger log = LoggerFactory.getLogger(MyKafkaProducer.class);

    public static void main(String[] args) {

        var producerConfig = Map.<String,Object>of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class
        );

        var options = SenderOptions.<String,String>create(producerConfig);

        var flux = Flux.interval(Duration.ofMillis(100))
                        .take(100).map(i -> new ProducerRecord<>("order-events",i.toString(),"order-"+i))
                .map(pr -> SenderRecord.create(pr, pr.key()));

        KafkaSender.create(options)
                .send(flux)
                .doOnNext(result -> log.info("Message with correlation id :{} sent successfully", result.correlationMetadata()))
                .subscribe();

    }
}
