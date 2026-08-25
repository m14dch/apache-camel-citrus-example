# Instrument Enrichment PoC

This small Camel Spring Boot application demonstrates end-to-end testing of a Kafka-to-HTTP
integration with Citrus and Testcontainers. It transforms incoming instrument JSON with JSLT,
calls a basic-authenticated reference-data service, and publishes either enriched or failed JSON
to Kafka.

## Architecture

```text
instruments.in
      |
      v
Camel route -- JSLT request --> HTTPS reference-data service
      |                                |
      |                         enrichment JSON
      |                                |
      +<-- JSLT success/error mapping --+
      |
      v
instruments.out (Kafka key = ISIN)
```

The production application consists of one `RouteBuilder`, three JSLT resources, and the Spring
Boot entry point. There are no processors, mapping beans, DTOs, or Java message-manipulation
classes.

## Build and run

Requirements:

- Java 21 or newer
- Maven 3.6.3 or newer
- Docker for integration tests

Maven itself must run on Java 21, not merely target it. In IntelliJ, set **Build Tools → Maven →
Runner → JRE** to the project Java 21 SDK. The build enforces this requirement before compilation.

Run the complete build, including all five container-backed integration-test classes:

```shell
mvn clean verify
```

`mvn test` remains container-free because integration tests use the `*IT` suffix and run through
Maven Failsafe.

For a short presentation, run `mvn clean verify` and follow one `messageId` through the JSON log
entries. The happy path visibly progresses through receipt, reference-data call, HTTP success,
enrichment, and Kafka publication. The retry test shows two correlated `503` warnings before its
success, while the failure test shows all four attempts and the final failed publication.

Run the application against externally supplied services:

```shell
KAFKA_BROKERS=localhost:9092 \
REFDATA_URL=https://refdata.example.ch/api/v1/reference-data \
REFDATA_USER=poc-user \
REFDATA_PASSWORD=poc-password \
mvn spring-boot:run
```

Topics, service URL, credentials, HTTP timeouts, and retry settings are externalized in
`application.yml`. The checked-in credential defaults are deliberately non-secret demo values.

Application logs use Spring Boot's Logstash JSON format. Camel propagates the inbound business
`messageId` through its MDC, including across HTTP retries and the asynchronous Kafka producer.
Every message-related entry therefore has a top-level `messageId`; Camel's generated transport ID
is retained separately as `camelMessageId`.

## Test coverage

- Happy path: validates the complete transformed request, preemptive Basic authorization,
  response merge, output JSON, and Kafka key.
- Retry path: returns 503 twice and then 200, proving that Camel issues exactly three calls before
  publishing an enriched result.
- Failure path: returns 503 four times, proving `max-redeliveries + 1` calls and the final JSLT
  error message.
- Concurrent input: submits three Kafka messages in parallel and proves that the single Camel
  consumer enriches and publishes all three without relying on their submission order.
- Business scenarios: runs three named parameterized cases backed by independent JSON resources.
  Each case checks its complete input, request, enrichment, Kafka key, and output with no ignored
  fields, proving that data is never mixed between messages.

One Kafka container is started per test JVM. The two topics are created explicitly to avoid
auto-creation races and first-message loss. Citrus Kafka and HTTP endpoints are Spring-managed and
shared across the test classes so the Kafka consumer keeps its offsets and the HTTP port is bound
only once. Plain Testcontainers plus `@DynamicPropertySource` keeps the infrastructure wiring
visible. Citrus `citrus-testcontainers` annotations such as `@KafkaContainerSupport` are a possible
alternative if less explicit wiring is preferred later.

Apache HttpClient automatic request recovery is disabled. This ensures that the calls observed by
the retry tests come exclusively from Camel's error handler and use the configured delay and
backoff multiplier.

### Docker Desktop troubleshooting

Testcontainers should use a host-visible Docker socket. If Ryuk fails while trying to mount
Docker Desktop's private `docker.raw.sock`, remove that stale setting from
`~/.testcontainers.properties` or use:

```properties
docker.client.strategy=org.testcontainers.dockerclient.UnixSocketClientProviderStrategy
docker.host=unix:///var/run/docker.sock
```

This file is workstation configuration and must not be committed. Also avoid running two copies
of the integration suite concurrently because the demo Citrus server intentionally uses fixed
port `18088`.

## Design decisions and known gaps

Retained inbound fields use Camel exchange variables. The JSLT component exposes them through
`$variables` without enabling `allowContextMapAll`; exchange properties would require that option,
which exposes the full Camel context to a template. The values are scalar strings lifted with
JSONPath. JSLT produces every application-generated JSON structure and handles escaping, including
quotes that may occur in exception messages.

Production configuration defaults to an HTTPS URL. Tests intentionally use the Citrus server over
plain HTTP on port 18088, so they exercise Basic authentication but not TLS. A follow-up should add
a self-signed keystore to the Citrus server and a matching truststore for the Camel HTTP client.

The single broad exception policy retries every failure, including HTTP 4xx responses. A production
route should use narrower exception policies and retry only I/O failures and retryable 5xx status
codes.

The project pins Spring Boot 3.5.16, Camel 4.14.8 LTS, Citrus 4.10.2, and the current Testcontainers
1.x line compatible with the required `org.testcontainers:kafka` artifact. BOMs govern their
transitive dependencies. Camel's current HTTP component calls its response wait option
`responseTimeout`; it is fed by the externally documented `app.refdata.socket-timeout-ms` property.

## License

This example is available under the [MIT License](LICENSE).
