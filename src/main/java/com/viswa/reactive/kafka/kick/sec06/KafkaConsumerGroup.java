package com.viswa.reactive.kafka.kick.sec06;

public class KafkaConsumerGroup {

    private static class Consumer1 {
        public static void main(String[] args) {
            MyKafkaReceiver.start("1");
        }
        //0, 1, 2
    }

    private static class Consumer2 {
        public static void main(String[] args) {
            MyKafkaReceiver.start("2");
        }
        // 1
    }

    private static class Consumer3 {
        public static void main(String[] args) {
            MyKafkaReceiver.start("3");
        }
        // 2
    }
}
