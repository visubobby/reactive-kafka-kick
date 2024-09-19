package com.viswa.reactive.kafka.kick.sec05;

import org.apache.kafka.clients.consumer.KafkaConsumer;

public class KafkaConsumerGroup {

    private static class Consumer1 {
        public static void main(String[] args) {
            MyKafkaReceiver.start("1");
        }
    }

    private static class Consumer2 {
        public static void main(String[] args) {
            MyKafkaReceiver.start("2");
        }
    }

    private static class Consumer3 {
        public static void main(String[] args) {
            MyKafkaReceiver.start("3");
        }
    }
}
