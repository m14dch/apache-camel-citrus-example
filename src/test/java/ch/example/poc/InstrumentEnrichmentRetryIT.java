package ch.example.poc;

import static org.citrusframework.actions.ReceiveMessageAction.Builder.receive;
import static org.citrusframework.actions.SendMessageAction.Builder.send;
import static org.citrusframework.http.actions.HttpActionBuilder.http;

import org.citrusframework.kafka.message.KafkaMessageHeaders;
import org.citrusframework.message.MessageType;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;

class InstrumentEnrichmentRetryIT extends AbstractCitrusIT {

    private static final String REQUEST = """
            {
              "identifier": {"type": "ISIN", "value": "US0378331005"},
              "displayName": "Apple Inc.",
              "baseCurrency": "USD",
              "assetClass": "EQUITY"
            }
            """;

    private static final String RESPONSE = """
            {
              "identifier": "US0378331005",
              "valuation": {
                "price": 231.42,
                "currency": "USD",
                "asOf": "2026-08-12"
              },
              "rating": "AA+",
              "source": "REFDATA-PROVIDER"
            }
            """;

    private static final String INBOUND = """
            {
              "messageId": "milestone-5-retry",
              "isin": "US0378331005",
              "name": "Apple Inc.",
              "currency": "USD",
              "issuer": "Apple Inc.",
              "instrumentType": "EQUITY",
              "maturityDate": null,
              "receivedAt": "2026-08-12T09:20:00Z"
            }
            """;

    private static final String EXPECTED_OUTPUT = """
            {
              "messageId": "milestone-5-retry",
              "isin": "US0378331005",
              "name": "Apple Inc.",
              "status": "ENRICHED",
              "price": 231.42,
              "priceCurrency": "USD",
              "priceAsOf": "2026-08-12",
              "rating": "AA+",
              "source": "REFDATA-PROVIDER"
            }
            """;

    @Test
    void retriesTwiceThenPublishesEnrichedResult() {
        runner.when(
            send()
                .endpoint(instrumentsIn)
                .message()
                    .body(INBOUND)
        );

        expectRequest(1);
        respond(1, HttpStatus.SERVICE_UNAVAILABLE, null);
        expectRequest(2);
        respond(2, HttpStatus.SERVICE_UNAVAILABLE, null);
        expectRequest(3);
        respond(3, HttpStatus.OK, RESPONSE);

        runner.then(
            receive()
                .endpoint(instrumentsOut)
                .message()
                    .type(MessageType.JSON)
                    .header(KafkaMessageHeaders.MESSAGE_KEY, "US0378331005")
                    .body(EXPECTED_OUTPUT)
        );
    }

    private void expectRequest(int attempt) {
        runner.and(
            http()
                .server(refDataService)
                .receive()
                    .post("/api/v1/reference-data")
                    .message()
                        .name("retry-request-" + attempt)
                        .type(MessageType.JSON)
                        .header("Authorization", expectedAuthorization())
                        .body(REQUEST)
        );
    }

    private void respond(int attempt, HttpStatus status, String body) {
        var response = http()
                .server(refDataService)
                .send()
                    .response(status);
        response.message()
                .name("retry-response-" + attempt);
        if (body != null) {
            response.message()
                    .contentType("application/json")
                    .body(body);
        }
        runner.and(response);
    }
}
