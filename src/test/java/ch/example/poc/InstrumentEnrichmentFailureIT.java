package ch.example.poc;

import static org.citrusframework.actions.ReceiveMessageAction.Builder.receive;
import static org.citrusframework.actions.SendMessageAction.Builder.send;
import static org.citrusframework.http.actions.HttpActionBuilder.http;

import org.citrusframework.kafka.message.KafkaMessageHeaders;
import org.citrusframework.message.MessageType;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;

class InstrumentEnrichmentFailureIT extends AbstractCitrusIT {

    private static final String REQUEST = """
            {
              "identifier": {"type": "ISIN", "value": "GB0002634946"},
              "displayName": "BAE Systems plc",
              "baseCurrency": "GBP",
              "assetClass": "EQUITY"
            }
            """;

    private static final String INBOUND = """
            {
              "messageId": "milestone-6-failure",
              "isin": "GB0002634946",
              "name": "BAE Systems plc",
              "currency": "GBP",
              "issuer": "BAE Systems plc",
              "instrumentType": "EQUITY",
              "maturityDate": null,
              "receivedAt": "2026-08-12T09:25:00Z"
            }
            """;

    private static final String EXPECTED_OUTPUT = """
            {
              "messageId": "milestone-6-failure",
              "isin": "GB0002634946",
              "status": "FAILED",
              "errorCode": "REFDATA_UNAVAILABLE",
              "errorMessage": "@contains('statusCode: 503')@"
            }
            """;

    @Test
    void publishesFailureAfterRetriesAreExhausted() {
        runner.when(
            send()
                .endpoint(instrumentsIn)
                .message()
                    .body(INBOUND)
        );

        for (int attempt = 0; attempt <= maxRedeliveries; attempt++) {
            runner.and(
                http()
                    .server(refDataService)
                    .receive()
                        .post("/api/v1/reference-data")
                        .message()
                            .name("failure-request-" + attempt)
                            .type(MessageType.JSON)
                            .header("Authorization", expectedAuthorization())
                            .body(REQUEST)
            );
            runner.and(
                http()
                    .server(refDataService)
                    .send()
                        .response(HttpStatus.SERVICE_UNAVAILABLE)
                        .message()
                            .name("failure-response-" + attempt)
            );
        }

        runner.then(
            receive()
                .endpoint(instrumentsOut)
                .message()
                    .type(MessageType.JSON)
                    .header(KafkaMessageHeaders.MESSAGE_KEY, "GB0002634946")
                    .body(EXPECTED_OUTPUT)
        );
    }
}
