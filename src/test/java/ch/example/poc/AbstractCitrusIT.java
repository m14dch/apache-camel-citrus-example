package ch.example.poc;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.citrusframework.TestCaseRunner;
import org.citrusframework.annotations.CitrusResource;
import org.citrusframework.config.CitrusSpringConfig;
import org.citrusframework.http.server.HttpServer;
import org.citrusframework.junit.jupiter.spring.CitrusSpringSupport;
import org.citrusframework.kafka.endpoint.KafkaEndpoint;
import org.citrusframework.kafka.endpoint.KafkaEndpointConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@CitrusSpringSupport
@Testcontainers
@ActiveProfiles("test")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@ContextConfiguration(classes = {
        Application.class, CitrusSpringConfig.class, AbstractCitrusIT.TestEndpoints.class
})
abstract class AbstractCitrusIT {

    private static final ConfluentKafkaContainer KAFKA = new ConfluentKafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.9.2"));

    static {
        KAFKA.start();
        try (AdminClient admin = AdminClient.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()))) {
            admin.createTopics(List.of(
                    new NewTopic("instruments.in", 1, (short) 1),
                    new NewTopic("instruments.out", 1, (short) 1)))
                .all().get();
        } catch (Exception e) {
            throw new IllegalStateException("Unable to create Kafka topics", e);
        }
    }

    @CitrusResource
    protected TestCaseRunner runner;

    @Autowired
    protected KafkaEndpoint instrumentsIn;

    @Autowired
    protected KafkaEndpoint instrumentsOut;

    @Autowired
    protected HttpServer refDataService;

    @Value("${app.refdata.username}")
    private String refDataUsername;

    @Value("${app.refdata.password}")
    private String refDataPassword;

    @Value("${app.retry.max-redeliveries}")
    protected int maxRedeliveries;

    @DynamicPropertySource
    static void applicationProperties(DynamicPropertyRegistry registry) {
        registry.add("camel.component.kafka.brokers", KAFKA::getBootstrapServers);
    }

    protected String expectedAuthorization() {
        String credentials = refDataUsername + ":" + refDataPassword;
        return "Basic " + Base64.getEncoder().encodeToString(
                credentials.getBytes(StandardCharsets.UTF_8));
    }

    @TestConfiguration
    static class TestEndpoints {

        @Bean
        KafkaEndpoint instrumentsIn() {
            KafkaEndpointConfiguration configuration = new KafkaEndpointConfiguration();
            configuration.setServer(KAFKA.getBootstrapServers());
            configuration.setTopic("instruments.in");
            return new KafkaEndpoint(configuration);
        }

        @Bean
        KafkaEndpoint instrumentsOut() {
            KafkaEndpointConfiguration configuration = new KafkaEndpointConfiguration();
            configuration.setServer(KAFKA.getBootstrapServers());
            configuration.setTopic("instruments.out");
            configuration.setConsumerGroup("citrus");
            configuration.setOffsetReset("earliest");
            configuration.setTimeout(15000);
            return new KafkaEndpoint(configuration);
        }

        @Bean
        HttpServer refDataService(@Value("${test.refdata.port}") int port) {
            HttpServer server = new HttpServer();
            server.setPort(port);
            server.setAutoStart(true);
            server.setDefaultTimeout(15000);
            return server;
        }
    }
}
