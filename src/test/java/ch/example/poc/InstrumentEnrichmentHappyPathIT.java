package ch.example.poc;

import static org.citrusframework.actions.ReceiveMessageAction.Builder.receive;
import static org.citrusframework.actions.SendMessageAction.Builder.send;
import static org.citrusframework.http.actions.HttpActionBuilder.http;

import org.citrusframework.kafka.message.KafkaMessageHeaders;
import org.citrusframework.message.MessageType;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;

class InstrumentEnrichmentHappyPathIT extends AbstractCitrusIT {

    private static final String INBOUND = """
            {
              "messageId": "milestone-3",
              "isin": "CH0012032048",
              "name": "Roche Holding AG",
              "currency": "CHF",
              "issuer": "Roche Holding AG",
              "instrumentType": "EQUITY",
              "maturityDate": null,
              "receivedAt": "2026-08-12T09:15:00Z"
            }
            """;

    private static final String REFDATA_REQUEST = """
            {
              "identifier": {"type": "ISIN", "value": "CH0012032048"},
              "displayName": "Roche Holding AG",
              "baseCurrency": "CHF",
              "assetClass": "EQUITY"
            }
            """;

    private static final String REFDATA_RESPONSE = """
            {
              "identifier": "CH0012032048",
              "valuation": {
                "price": 248.35,
                "currency": "CHF",
                "asOf": "2026-08-12"
              },
              "rating": "AA-",
              "source": "REFDATA-PROVIDER"
            }
            """;

    private static final String EXPECTED_OUTPUT = """
            {
              "messageId": "milestone-3",
              "isin": "CH0012032048",
              "name": "Roche Holding AG",
              "status": "ENRICHED",
              "price": 248.35,
              "priceCurrency": "CHF",
              "priceAsOf": "2026-08-12",
              "rating": "AA-",
              "source": "REFDATA-PROVIDER"
            }
            """;

    @Test
    void enrichesInstrumentAndPublishesResult() {
        runner.when(
            send()
                .endpoint(instrumentsIn)
                .message()
                    .body(INBOUND)
        );

        runner.and(
            http()
                .server(refDataService)
                .receive()
                    .post("/api/v1/reference-data")
                    .message()
                        .name("happy-path-request")
                        .type(MessageType.JSON)
                        .header("Content-Type", "@startsWith('application/json')@")
                        .header("Authorization", expectedAuthorization())
                        .body(REFDATA_REQUEST)
        );

        runner.and(
            http()
                .server(refDataService)
                .send()
                    .response(HttpStatus.OK)
                    .message()
                        .name("happy-path-response")
                        .contentType("application/json")
                        .body(REFDATA_RESPONSE)
        );

        runner.then(
            receive()
                .endpoint(instrumentsOut)
                .message()
                    .type(MessageType.JSON)
                    .header(KafkaMessageHeaders.MESSAGE_KEY, "CH0012032048")
                    .body(EXPECTED_OUTPUT)
        );
    }
}
