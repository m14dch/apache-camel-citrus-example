package ch.example.poc;

import static org.citrusframework.actions.ReceiveMessageAction.Builder.receive;
import static org.citrusframework.actions.SendMessageAction.Builder.send;
import static org.citrusframework.http.actions.HttpActionBuilder.http;

import java.util.stream.Stream;

import org.citrusframework.kafka.message.KafkaMessageHeaders;
import org.citrusframework.message.MessageType;
import org.citrusframework.spi.Resource;
import org.citrusframework.spi.Resources;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.http.HttpStatus;

class InstrumentEnrichmentBusinessScenariosIT extends AbstractCitrusIT {

    @ParameterizedTest(name = "{0}")
    @MethodSource("businessScenarios")
    void enrichesBusinessScenario(BusinessScenario scenario) {
        runner.when(
            send()
                .endpoint(instrumentsIn)
                .message()
                    .name(scenario.messageName("input"))
                    .body(scenario.resource("input.json"))
        );

        runner.and(
            http()
                .server(refDataService)
                .receive()
                    .post("/api/v1/reference-data")
                    .message()
                        .name(scenario.messageName("request"))
                        .type(MessageType.JSON)
                        .header("Authorization", expectedAuthorization())
                        .body(scenario.resource("expected-request.json"))
        );

        runner.and(
            http()
                .server(refDataService)
                .send()
                    .response(HttpStatus.OK)
                    .message()
                        .name(scenario.messageName("response"))
                        .contentType("application/json")
                        .body(scenario.resource("enrichment.json"))
        );

        runner.then(
            receive()
                .endpoint(instrumentsOut)
                .message()
                    .name(scenario.messageName("output"))
                    .type(MessageType.JSON)
                    .header(KafkaMessageHeaders.MESSAGE_KEY, scenario.isin())
                    .body(scenario.resource("expected-output.json"))
        );
    }

    static Stream<BusinessScenario> businessScenarios() {
        return Stream.of(
                new BusinessScenario("Allianz", "allianz", "DE0008404005"),
                new BusinessScenario("TotalEnergies", "totalenergies", "FR0000120271"),
                new BusinessScenario("Philips", "philips", "NL0000009538")
        );
    }

    private record BusinessScenario(String name, String directory, String isin) {

        Resource resource(String fileName) {
            return Resources.fromClasspath("scenarios/" + directory + "/" + fileName);
        }

        String messageName(String stage) {
            return directory + "-" + stage;
        }

        @Override
        public String toString() {
            return name;
        }
    }
}
