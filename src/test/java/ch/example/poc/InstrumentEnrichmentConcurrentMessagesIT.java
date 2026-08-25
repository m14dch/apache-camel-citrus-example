package ch.example.poc;

import static org.citrusframework.actions.ReceiveMessageAction.Builder.receive;
import static org.citrusframework.actions.SendMessageAction.Builder.send;
import static org.citrusframework.container.Parallel.Builder.parallel;
import static org.citrusframework.http.actions.HttpActionBuilder.http;

import java.util.List;

import org.citrusframework.TestActionBuilder;
import org.citrusframework.kafka.message.KafkaMessageHeaders;
import org.citrusframework.message.MessageType;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;

class InstrumentEnrichmentConcurrentMessagesIT extends AbstractCitrusIT {

    private static final List<Instrument> INSTRUMENTS = List.of(
            new Instrument("concurrent-1", "DE0008404005", "Allianz SE", "EUR"),
            new Instrument("concurrent-2", "FR0000120271", "TotalEnergies SE", "EUR"),
            new Instrument("concurrent-3", "NL0000009538", "Philips NV", "EUR")
    );

    private static final String REFDATA_RESPONSE = """
            {
              "valuation": {
                "price": 100.00,
                "currency": "EUR",
                "asOf": "2026-08-12"
              },
              "rating": "A",
              "source": "REFDATA-PROVIDER"
            }
            """;

    @Test
    void processesMessagesSentConcurrentlyWithOneKafkaConsumer() {
        runner.when(
            parallel().actions(
                INSTRUMENTS.stream()
                    .map(instrument -> send()
                        .endpoint(instrumentsIn)
                        .message()
                            .name("concurrent-input-" + instrument.messageId())
                            .body(instrument.inboundMessage()))
                    .toArray(TestActionBuilder[]::new)
            )
        );

        // One Camel Kafka consumer handles these requests sequentially. Their arrival order is
        // intentionally unspecified because the producer actions above execute concurrently.
        for (int index = 1; index <= INSTRUMENTS.size(); index++) {
            expectAnyReferenceDataRequest(index);
            respondSuccessfully(index);
        }

        // The detailed happy-path test proves field mapping. Here we verify that every concurrently
        // submitted message completes successfully without assuming which one completes first.
        for (int index = 1; index <= INSTRUMENTS.size(); index++) {
            expectAnyEnrichedResult(index);
        }
    }

    private void expectAnyReferenceDataRequest(int sequence) {
        runner.and(
            http()
                .server(refDataService)
                .receive()
                    .post("/api/v1/reference-data")
                    .message()
                        .name("concurrent-request-" + sequence)
                        .type(MessageType.JSON)
                        .header("Authorization", expectedAuthorization())
                        .body("""
                                {
                                  "identifier": {"type": "ISIN", "value": "@ignore@"},
                                  "displayName": "@ignore@",
                                  "baseCurrency": "EUR",
                                  "assetClass": "EQUITY"
                                }
                                """)
        );
    }

    private void respondSuccessfully(int sequence) {
        runner.and(
            http()
                .server(refDataService)
                .send()
                    .response(HttpStatus.OK)
                    .message()
                        .name("concurrent-response-" + sequence)
                        .contentType("application/json")
                        .body(REFDATA_RESPONSE)
        );
    }

    private void expectAnyEnrichedResult(int sequence) {
        runner.then(
            receive()
                .endpoint(instrumentsOut)
                .message()
                    .name("concurrent-output-" + sequence)
                    .type(MessageType.JSON)
                    .header(KafkaMessageHeaders.MESSAGE_KEY, "@ignore@")
                    .body("""
                            {
                              "messageId": "@startsWith('concurrent-')@",
                              "isin": "@ignore@",
                              "name": "@ignore@",
                              "status": "ENRICHED",
                              "price": 100.00,
                              "priceCurrency": "EUR",
                              "priceAsOf": "2026-08-12",
                              "rating": "A",
                              "source": "REFDATA-PROVIDER"
                            }
                            """)
        );
    }

    private record Instrument(String messageId, String isin, String name, String currency) {

        String inboundMessage() {
            return """
                    {
                      "messageId": "%s",
                      "isin": "%s",
                      "name": "%s",
                      "currency": "%s",
                      "issuer": "%s",
                      "instrumentType": "EQUITY",
                      "maturityDate": null,
                      "receivedAt": "2026-08-12T09:30:00Z"
                    }
                    """.formatted(messageId, isin, name, currency, name);
        }
    }
}
