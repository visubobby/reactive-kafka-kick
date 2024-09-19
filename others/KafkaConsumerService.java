import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.reactor.circuitbreaker.operator.CircuitBreakerOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.util.retry.Retry;

import javax.annotation.PostConstruct;
import java.time.Duration;

@Service
public class KafkaConsumerService {

    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerService.class);

    private final KafkaReceiver<String, String> kafkaReceiver;
    private final MessageRepository messageRepository;
    private final CircuitBreaker circuitBreaker;

    public KafkaConsumerService(KafkaReceiver<String, String> kafkaReceiver, MessageRepository messageRepository) {
        this.kafkaReceiver = kafkaReceiver;
        this.messageRepository = messageRepository;

        // Configure the Circuit Breaker
        CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)  // Circuit breaker opens when 50% of requests fail
                .waitDurationInOpenState(Duration.ofSeconds(10))  // Time to wait before trying again
                .build();
        this.circuitBreaker = CircuitBreaker.of("kafkaCircuitBreaker", circuitBreakerConfig);
    }

    @PostConstruct
    public void consumeKafkaMessages() {
        kafkaReceiver.receive()
                .flatMap(consumerRecord -> {
                    String messageContent = consumerRecord.value();
                    Message message = new Message(null, messageContent);

                    // Circuit breaker and error handling with retry
                    return messageRepository.save(message)
                            .doOnSuccess(savedMessage -> consumerRecord.receiverOffset().acknowledge())
                            .doOnError(error -> logger.error("Failed to save message: {}", error.getMessage()))
                            .onErrorResume(error -> {
                                // Handle the error by logging and continuing the stream
                                logger.error("Error processing message: {}", error.getMessage());
                                return Flux.empty();
                            })
                            .retryWhen(Retry.backoff(3, Duration.ofSeconds(1)))  // Retry up to 3 times
                            .transformDeferred(CircuitBreakerOperator.of(circuitBreaker));  // Apply circuit breaker
                })
                .doOnError(error -> logger.error("Kafka processing stream encountered an error: {}", error.getMessage()))
                .onErrorContinue((throwable, o) -> {
                    // Continue processing even after an error
                    logger.error("Continuing stream despite error: {}", throwable.getMessage());
                })
                .subscribe();
    }
}
