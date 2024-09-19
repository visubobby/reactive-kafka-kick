import org.springframework.r2dbc.connection.TransactionAwareConnectionFactoryProxy;
import org.springframework.transaction.annotation.Transactional;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import javax.annotation.PostConstruct;
import java.time.Duration;

@Service
public class KafkaConsumerService {

    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerService.class);

    private final KafkaReceiver<String, String> kafkaReceiver;
    private final MessageRepository messageRepository;
    private final MetadataRepository metadataRepository;
    private final DatabaseClient databaseClient;
    private final CircuitBreaker circuitBreaker;

    public KafkaConsumerService(KafkaReceiver<String, String> kafkaReceiver, 
                                MessageRepository messageRepository, 
                                MetadataRepository metadataRepository, 
                                TransactionAwareConnectionFactoryProxy connectionFactory) {
        this.kafkaReceiver = kafkaReceiver;
        this.messageRepository = messageRepository;
        this.metadataRepository = metadataRepository;

        // Circuit Breaker Config
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .waitDurationInOpenState(Duration.ofSeconds(10))
                .build();
        this.circuitBreaker = CircuitBreaker.of("kafkaCircuitBreaker", circuitBreakerConfig);
    }

    @PostConstruct
    public void consumeKafkaMessages() {
        kafkaReceiver.receive()
                .flatMap(consumerRecord -> {
                    String messageContent = consumerRecord.value();
                    
                    // Insert data into both tables within a transaction
                    return insertMessageAndMetadata(messageContent)
                            .doOnSuccess(result -> consumerRecord.receiverOffset().acknowledge())
                            .doOnError(error -> logger.error("Error in transaction: {}", error.getMessage()))
                            .onErrorResume(error -> {
                                // Log the error and continue
                                logger.error("Error processing message: {}", error.getMessage());
                                return Mono.empty();
                            })
                            .retryWhen(Retry.backoff(3, Duration.ofSeconds(1)))
                            .transformDeferred(CircuitBreakerOperator.of(circuitBreaker));
                })
                .doOnError(error -> logger.error("Kafka stream error: {}", error.getMessage()))
                .onErrorContinue((throwable, o) -> {
                    // Continue the stream after an error
                    logger.error("Continuing despite error: {}", throwable.getMessage());
                })
                .subscribe();
    }

    @Transactional
    public Mono<Void> insertMessageAndMetadata(String messageContent) {
        Message message = new Message(null, messageContent);
        
        return messageRepository.save(message)
                .flatMap(savedMessage -> {
                    Metadata metadata = new Metadata(null, savedMessage.getId(), "Additional data about " + savedMessage.getId());
                    return metadataRepository.save(metadata);
                })
                .then();
    }
}
