@Configuration
public class R2dbcConfig extends AbstractR2dbcConfiguration {

    @Bean
    public ConnectionFactory connectionFactory() {
        return ConnectionFactories.get(ConnectionFactoryOptions.builder()
            .option(ConnectionFactoryOptions.DRIVER, "oracle")
            .option(ConnectionFactoryOptions.HOST, "localhost")
            .option(ConnectionFactoryOptions.PORT, 1521)
            .option(ConnectionFactoryOptions.DATABASE, "yourDB")
            .option(ConnectionFactoryOptions.USER, "yourUsername")
            .option(ConnectionFactoryOptions.PASSWORD, "yourPassword")
            .build());
    }
}
