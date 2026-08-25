package ch.example.poc;

import org.apache.camel.Exchange;
import org.apache.camel.ExchangePropertyKey;
import org.apache.camel.LoggingLevel;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaConstants;
import org.springframework.stereotype.Component;

@Component
public class InstrumentEnrichmentRoute extends RouteBuilder {

    private static final String LOGGER = "ch.example.poc.integration";

    @Override
    public void configure() {
        onException(Exception.class)
            .maximumRedeliveries("{{app.retry.max-redeliveries}}")
            .redeliveryDelay("{{app.retry.delay-ms}}")
            .backOffMultiplier("{{app.retry.backoff-multiplier}}")
            .retryAttemptedLogLevel(LoggingLevel.WARN)
            .handled(true)
            .log(LoggingLevel.ERROR, LOGGER,
                    "Enrichment failed for ${variable.isin}: ${exception.message}")
            .setVariable("errorMessage").simple("${exception.message}")
            .setBody(constant("{}"))
            .to("jslt:jslt/error-response.jslt?contentCache=true")
            .to("direct:publishResult")
            .end();

        from("kafka:{{app.kafka.in-topic}}?groupId={{app.kafka.consumer-group}}"
                + "&autoOffsetReset=earliest")
            .routeId("instrument-enrichment")
            .setVariable("messageId").jsonpath("$.messageId")
            .setVariable("isin").jsonpath("$.isin")
            .setVariable("name").jsonpath("$.name")
            .setProperty(ExchangePropertyKey.CORRELATION_ID.getName())
                .variable("messageId")
            .log(LoggingLevel.INFO, LOGGER,
                    "Received instrument isin=${variable.isin}, name=${variable.name}")
            .to("jslt:jslt/refdata-request.jslt?contentCache=true")
            .to("direct:callRefData")
            .to("jslt:jslt/refdata-response.jslt?contentCache=true")
            .log(LoggingLevel.INFO, LOGGER,
                    "Enrichment completed for isin=${variable.isin}")
            .to("direct:publishResult");

        from("direct:callRefData")
            .routeId("refdata-call")
            .removeHeaders("*")
            .setHeader(Exchange.HTTP_METHOD, constant("POST"))
            .setHeader(Exchange.CONTENT_TYPE, constant("application/json"))
            .log(LoggingLevel.INFO, LOGGER,
                    "Calling reference data service for isin=${variable.isin}")
            .toD("{{app.refdata.url}}"
                    + "?authMethod=Basic"
                    + "&authUsername={{app.refdata.username}}"
                    + "&authPassword={{app.refdata.password}}"
                    + "&authenticationPreemptive=true"
                    + "&connectTimeout={{app.refdata.connect-timeout-ms}}"
                    + "&responseTimeout={{app.refdata.socket-timeout-ms}}")
            .convertBodyTo(String.class)
            .log(LoggingLevel.INFO, LOGGER,
                    "Reference data lookup succeeded for isin=${variable.isin}, "
                            + "httpStatus=${header.CamelHttpResponseCode}");

        from("direct:publishResult")
            .routeId("publish-result")
            .setHeader(KafkaConstants.KEY).simple("${variable.isin}")
            .log(LoggingLevel.INFO, LOGGER,
                    "Publishing result for isin=${variable.isin} to {{app.kafka.out-topic}}")
            .to("kafka:{{app.kafka.out-topic}}")
            .log(LoggingLevel.INFO, LOGGER,
                    "Published result for isin=${variable.isin} to {{app.kafka.out-topic}}");
    }
}
