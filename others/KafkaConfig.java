@Configuration
public class KafkaConfig {

    @Bean
    public ReceiverOptions<String, String> kafkaReceiverOptions() {
        Map<String, Object> kafkaProps = new HashMap<>();
        kafkaProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, "my-group");
        kafkaProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        ReceiverOptions<String, String> basicReceiverOptions = ReceiverOptions.create(kafkaProps);
        return basicReceiverOptions.subscription(Collections.singleton("my-topic"));
    }

    @Bean
    public KafkaReceiver<String, String> kafkaReceiver(ReceiverOptions<String, String> kafkaReceiverOptions) {
        return KafkaReceiver.create(kafkaReceiverOptions);
    }
}
