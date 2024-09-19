Key Changes and Explanation:
Transactional Boundary:

We use the @Transactional annotation on the insertMessageAndMetadata method to ensure that both the Message and Metadata tables are updated as part of a single transaction.
If either insertion fails, the entire transaction is rolled back.
Reactive Transactions:

Spring Data R2DBC automatically handles reactive transactions. By using @Transactional, it ensures that the operations within the insertMessageAndMetadata method are executed as a single unit of work.
The Mono<Void>.then() ensures that after inserting into the messageRepository, the insertion into metadataRepository happens next.
Retry and Error Handling:

As before, errors are logged and the stream continues processing subsequent Kafka messages. The retry mechanism retries failed transactions up to 3 times.
The circuitBreaker ensures resilience during failures, preventing system overload.
This setup ensures that both tables (e.g., messages and metadata) are updated within a single transaction. If anything fails during the process, the transaction is rolled back, and the error is handled without breaking the Kafka consumer.