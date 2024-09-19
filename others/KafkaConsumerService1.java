@Service
public class KafkaConsumerService {

    private final KafkaReceiver<String, String> kafkaReceiver;
    private final MessageRepository messageRepository;

    public KafkaConsumerService(KafkaReceiver<String, String> kafkaReceiver, MessageRepository messageRepository) {
        this.kafkaReceiver = kafkaReceiver;
        this.messageRepository = messageRepository;
    }

    @PostConstruct
    public void consumeKafkaMessages() {
        kafkaReceiver.receive()
            .flatMap(consumerRecord -> {
                String messageContent = consumerRecord.value();
                Message message = new Message(null, messageContent);
                return messageRepository.save(message)
                    .doOnSuccess(savedMessage -> consumerRecord.receiverOffset().acknowledge());
            })
            .subscribe();
    }
}
