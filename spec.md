# PoC Spec — Instrument Enrichment with Camel Spring Boot, Citrus and Testcontainers

> **How to use this document.** You are implementing this proof of concept. Build it exactly as
> specified. Where this spec says "decide", make the simplest choice that satisfies the acceptance
> criteria and document it in the README. Ask before adding anything not listed here.

## 0. Reference documentation

Apache Camel publishes an LLM-friendly documentation index:

- **Entry point: <https://camel.apache.org/llms.txt>**
- Every documentation page is also available as markdown by appending `.md` to its URL, e.g.
  `https://camel.apache.org/components/next/kafka-component.html.md`,
  `https://camel.apache.org/components/next/languages/jsonpath-language.html.md`

Consult these instead of relying on memory for component options, DSL method names, and language
syntax. Verify every endpoint URI option you write against the component page.

Other references:

- Citrus framework: <https://citrusframework.org>
- Testcontainers Java: <https://java.testcontainers.org>

---

## 1. Goal

Demonstrate, in the smallest credible application, that a Camel Spring Boot integration touching
Kafka and an external HTTPS service can be tested end-to-end with Citrus and Testcontainers —
including the retry behaviour and the failure path.

The PoC is a **testing showcase**. The business logic is deliberately trivial. Reviewers should be
able to read one route and three tests and understand the whole thing in ten minutes.

## 2. Business scenario

1. A message describing a financial instrument arrives on a Kafka topic as JSON.
2. The route extracts several fields and maps them into a **different** JSON structure.
3. That JSON is POSTed to an external reference-data service over **HTTPS with basic auth**.
4. The service responds with enrichment data (e.g. valuation and rating).
5. The route merges the enrichment with fields from the original message and publishes a JSON
   response message to a second Kafka topic.

### 2.1 Inbound message — topic `instruments.in`

```json
{
  "messageId": "8f1c2f6e-2c4f-4f2a-9a5f-2b0c9b6f0a11",
  "isin": "CH0012032048",
  "name": "Roche Holding AG",
  "currency": "CHF",
  "issuer": "Roche Holding AG",
  "instrumentType": "EQUITY",
  "maturityDate": null,
  "receivedAt": "2026-08-12T09:15:00Z"
}
```

### 2.2 Request to the external service — `POST /api/v1/reference-data`

Note the deliberate structural difference: renamed fields, nested object, dropped fields.

```json
{
  "identifier": {
    "type": "ISIN",
    "value": "CH0012032048"
  },
  "displayName": "Roche Holding AG",
  "baseCurrency": "CHF",
  "assetClass": "EQUITY"
}
```

### 2.3 Response from the external service

```json
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
```

### 2.4 Outbound message — topic `instruments.out`

Merges original fields (`messageId`, `isin`, `name`) with enrichment fields.

```json
{
  "messageId": "8f1c2f6e-2c4f-4f2a-9a5f-2b0c9b6f0a11",
  "isin": "CH0012032048",
  "name": "Roche Holding AG",
  "status": "ENRICHED",
  "price": 248.35,
  "priceCurrency": "CHF",
  "priceAsOf": "2026-08-12",
  "rating": "AA-",
  "source": "REFDATA-PROVIDER"
}
```

### 2.5 Outbound error message — same topic `instruments.out`

Published when the external call still fails after all retries.

```json
{
  "messageId": "8f1c2f6e-2c4f-4f2a-9a5f-2b0c9b6f0a11",
  "isin": "CH0012032048",
  "status": "FAILED",
  "errorCode": "REFDATA_UNAVAILABLE",
  "errorMessage": "HTTP operation failed invoking ... with statusCode: 503"
}
```

The Kafka message key is the ISIN for both success and error messages.

---

## 3. Constraints and principles

**Non-negotiable:**

1. **Camel DSL over Java code.** No `Processor` implementations, no `AggregationStrategy` classes,
   no bean methods that manipulate the message. If a step feels like it needs Java, restructure the
   route instead.
2. **All JSON-to-JSON mapping is done with JSLT templates** (`camel-jslt`), held as separate
   template resources — not as string concatenation in the route and not with `simple` templating.
   `jsonpath` is used only to lift a handful of scalar values onto the exchange so the second
   template can reach them.
3. **Java DSL** (`RouteBuilder`), not YAML or XML.
4. **KISS.** One route file, three JSLT templates. No layering, no service classes, no DTOs, no
   MapStruct, no `@ConfigurationProperties` unless it removes more code than it adds. No Lombok.
5. **No secrets in code or in the repository.** Basic auth credentials come from configuration with
   environment-variable overrides.
6. **Everything externalised as properties** — topics, URL, credentials, timeouts, retry counts.
   Tests override properties; they never modify the route (no `AdviceWith`, no route mocking).

**Scope boundaries — do NOT implement:**

- Dead letter topic, error topic, or DLQ handling
- Schema registry / Avro / Protobuf — plain JSON strings only
- Idempotency, deduplication, ordering guarantees, exactly-once semantics
- OAuth, mTLS, token caching
- Database, persistence, caching
- Metrics dashboards, tracing backends (a `management.endpoints` health check is enough)
- Docker packaging of the application itself, Kubernetes manifests
- A second DSL variant

---

## 4. Technology stack

| Concern | Choice |
|---|---|
| Build | Maven |
| Java | 21 |
| Framework | Spring Boot 3.x |
| Integration | Apache Camel 4.x (`camel-spring-boot-bom`) |
| Test framework | JUnit 5 |
| Integration testing | Citrus (`citrus-bom`) |
| Infrastructure | Testcontainers (`testcontainers-bom`) |

**Version selection:** use the current release of each line and let the BOMs govern transitive
versions. Do not mix Camel versions across modules. Verify the Camel/Spring Boot compatibility
matrix at <https://camel.apache.org/camel-spring-boot/next/spring-boot.html.md> before pinning.

### 4.1 Runtime dependencies

- `camel-spring-boot-starter`
- `camel-kafka-starter`
- `camel-http-starter` (Apache HttpClient based — supports basic auth and HTTPS)
- `camel-jslt-starter` (JSON-to-JSON transformation — the core of this PoC)
- `camel-jackson-starter` (JSON type conversion; JSLT relies on it to expose exchange data)
- `camel-jsonpath-starter` (lifting a few scalars onto the exchange)
- `spring-boot-starter-actuator` (health endpoint only)

### 4.2 Test dependencies

- `spring-boot-starter-test`
- `citrus-junit5`
- `citrus-spring` (Spring integration for Citrus)
- `citrus-kafka`
- `citrus-http`
- `citrus-validation-json`
- `org.testcontainers:kafka`
- `org.testcontainers:junit-jupiter`

> Optional, evaluate but do not assume: Citrus also ships `citrus-testcontainers` with annotations
> such as `@KafkaContainerSupport` that start the broker and wire endpoints automatically. Prefer
> plain Testcontainers + `@DynamicPropertySource` for the first milestone — it is more familiar to
> reviewers and keeps the wiring visible. Note the alternative in the README.

---

## 5. Project structure

```
instrument-enrichment-poc/
├── pom.xml
├── README.md
└── src
    ├── main
    │   ├── java/ch/example/poc/
    │   │   ├── Application.java
    │   │   └── InstrumentEnrichmentRoute.java
    │   └── resources/
    │       ├── application.yml
    │       └── jslt/
    │           ├── refdata-request.jslt
    │           ├── refdata-response.jslt
    │           └── error-response.jslt
    └── test
        ├── java/ch/example/poc/
        │   ├── AbstractCitrusIT.java
        │   ├── InstrumentEnrichmentHappyPathIT.java
        │   ├── InstrumentEnrichmentRetryIT.java
        │   └── InstrumentEnrichmentFailureIT.java
        └── resources/
            └── application-test.yml
```

Three production classes total. If a fourth appears, justify it.

---

## 6. Application implementation

### 6.1 Configuration — `application.yml`

```yaml
camel:
  springboot:
    name: instrument-enrichment-poc
    main-run-controller: true
  component:
    kafka:
      brokers: ${KAFKA_BROKERS:localhost:9092}

app:
  kafka:
    in-topic: instruments.in
    out-topic: instruments.out
    consumer-group: instrument-enrichment
  refdata:
    # scheme is part of the property so tests can point at plain http
    url: ${REFDATA_URL:https://refdata.example.ch/api/v1/reference-data}
    username: ${REFDATA_USER:poc-user}
    password: ${REFDATA_PASSWORD:poc-password}
    connect-timeout-ms: 2000
    socket-timeout-ms: 5000
  retry:
    max-redeliveries: 3
    delay-ms: 300
    backoff-multiplier: 2
```

### 6.2 Route — `InstrumentEnrichmentRoute.java`

Single `RouteBuilder`. Implement in this shape; adjust option names to match the component docs.

**Error handling block**

```java
onException(Exception.class)
    .maximumRedeliveries("{{app.retry.max-redeliveries}}")
    .redeliveryDelay("{{app.retry.delay-ms}}")
    .backOffMultiplier("{{app.retry.backoff-multiplier}}")
    .retryAttemptedLogLevel(LoggingLevel.WARN)
    .handled(true)
    .log(LoggingLevel.ERROR,
         "Enrichment failed for ${variable.isin}: ${exception.message}")
    .setVariable("errorMessage").simple("${exception.message}")
    // deterministic input for the template — the body at failure time is undefined
    .setBody(constant("{}"))
    .to("jslt:jslt/error-response.jslt?contentCache=true")
    .to("direct:publishResult");
```

**Main route**

```java
from("kafka:{{app.kafka.in-topic}}?groupId={{app.kafka.consumer-group}}"
        + "&autoOffsetReset=earliest")
    .routeId("instrument-enrichment")
    .log("Received instrument message: ${body}")

    // 1. retain the fields the response template needs, as exchange variables
    .setVariable("messageId").jsonpath("$.messageId")
    .setVariable("isin").jsonpath("$.isin")
    .setVariable("name").jsonpath("$.name")

    // 2. map inbound JSON to the external service's request JSON
    .to("jslt:jslt/refdata-request.jslt?contentCache=true")

    // 3. call the external service
    .to("direct:callRefData")

    // 4. map the response JSON to the outbound message, merging in the retained fields
    .to("jslt:jslt/refdata-response.jslt?contentCache=true")

    .to("direct:publishResult");

from("direct:callRefData")
    .routeId("refdata-call")
    // never let Kafka headers leak into the HTTP request
    .removeHeaders("*")
    .setHeader(Exchange.HTTP_METHOD, constant("POST"))
    .setHeader(Exchange.CONTENT_TYPE, constant("application/json"))
    .toD("{{app.refdata.url}}"
        + "?authMethod=Basic"
        + "&authUsername={{app.refdata.username}}"
        + "&authPassword={{app.refdata.password}}"
        + "&authenticationPreemptive=true"
        + "&connectTimeout={{app.refdata.connect-timeout-ms}}"
        + "&socketTimeout={{app.refdata.socket-timeout-ms}}")
    .convertBodyTo(String.class)
    .log("Reference data response: ${body}");

from("direct:publishResult")
    .routeId("publish-result")
    .setHeader(KafkaConstants.KEY).simple("${variable.isin}")
    .to("kafka:{{app.kafka.out-topic}}");
```

### 6.3 JSLT templates

`src/main/resources/jslt/refdata-request.jslt` — inbound message to service request:

```
{
  "identifier": {
    "type": "ISIN",
    "value": .isin
  },
  "displayName": .name,
  "baseCurrency": .currency,
  "assetClass": .instrumentType
}
```

`src/main/resources/jslt/refdata-response.jslt` — service response merged with retained fields:

```
{
  "messageId": $variables.messageId,
  "isin": $variables.isin,
  "name": $variables.name,
  "status": "ENRICHED",
  "price": .valuation.price,
  "priceCurrency": .valuation.currency,
  "priceAsOf": .valuation.asOf,
  "rating": .rating,
  "source": .source
}
```

`src/main/resources/jslt/error-response.jslt` — retries exhausted:

```
{
  "messageId": $variables.messageId,
  "isin": $variables.isin,
  "status": "FAILED",
  "errorCode": "REFDATA_UNAVAILABLE",
  "errorMessage": $variables.errorMessage
}
```

### 6.4 Implementation notes and known pitfalls

**JSLT specifics** — verify against
<https://camel.apache.org/components/next/jslt-component.html.md>:

- **Exchange data reaches the template as `$headers.x`, `$variables.x` and
  `$exchange.properties.x`.** Use **variables** for the retained fields, as specified above.
  Exchange properties would work too, but only with `allowContextMapAll=true` on the endpoint,
  which the component documentation flags as a security risk because it exposes the full
  `CamelContext` API to the template. Variables need no such flag — this is the reason for the
  choice, and it belongs in the README.
- Values that Jackson cannot convert to JSON are silently dropped from the template context. Keep
  the retained variables as plain strings, set via `jsonpath`.
- The route uses `to("jslt:...")`, i.e. the JSLT **component**, not a language expression. The
  resource URI resolves from the classpath by default and accepts explicit `classpath:`, `file:`,
  `http:`, `ref:` and `bean:` prefixes. `contentCache` defaults to `true`, so the template is read
  once — set it to `false` if you want live template edits during development.
- JSLT handles JSON escaping for you — this is a real advantage over string templating and is worth
  one line in the README, since the error message from an exception can contain quotes.
- On the error path the body at failure time is not guaranteed to be valid JSON, so the route sets
  `{}` as the body before applying the error template. The template ignores its input entirely.
- **Templates are independently testable.** Optionally add one plain JUnit test per template that
  feeds a fixture JSON through `Parser.compileString(...)` and asserts the output. Cheap, fast, and
  it isolates mapping bugs from wiring bugs. Not required for acceptance.

**Route and HTTP specifics:**

- **`toD` vs `to`.** The property placeholder in the URL contains the full scheme, so the endpoint
  is resolved at build time and `to` also works. Use whichever resolves cleanly; if the URL
  property is only known at runtime, `toD` is required.
- **`removeHeaders("*")` before the HTTP call** is deliberate — Kafka consumer headers otherwise
  become HTTP request headers. Exchange properties survive this, which is exactly why the retained
  fields live there.
- **`authenticationPreemptive=true`** sends the `Authorization` header on the first request instead
  of waiting for a 401 challenge. The tests assert on that header, so it must be set.
- **Retry on 4xx is wrong in production** but is accepted here for simplicity. Add one README line
  noting that a real implementation would retry only on `IOException` and 5xx, e.g. by narrowing
  the `onException` clauses.

---

## 7. Test implementation

### 7.1 Base class — `AbstractCitrusIT`

Responsibilities:

- `@SpringBootTest(webEnvironment = WebEnvironment.NONE)` with the `test` profile
- `@Testcontainers` with a **static** Kafka container (one per JVM, shared by all test classes —
  use the singleton pattern so it is not restarted between classes)
- `@DynamicPropertySource` that sets `camel.component.kafka.brokers` from
  `kafka.getBootstrapServers()` and `app.refdata.url` from the Citrus HTTP server port
- `@CitrusSupport` and `@CitrusResource TestCaseRunner runner`
- Citrus endpoint declarations shared by all tests:

```java
@CitrusEndpoint
@KafkaEndpointConfig(topic = "instruments.in")
private KafkaEndpoint instrumentsIn;

@CitrusEndpoint
@KafkaEndpointConfig(topic = "instruments.out", consumerGroup = "citrus")
private KafkaEndpoint instrumentsOut;

@CitrusEndpoint
@HttpServerConfig(port = 18088, autoStart = true)
private HttpServer refDataService;
```

**On HTTPS in tests:** the route's scheme comes from `app.refdata.url`, so tests point it at
`http://localhost:18088/...` while the default configuration uses `https://`. This keeps the PoC
simple and still exercises basic auth. State this explicitly in the README as a known gap, and list
"add a Citrus HTTP server with a self-signed keystore and a truststore for the Camel HTTP client"
as the follow-up.

**Topic creation:** decide between `KafkaContainer` auto-topic-creation and creating the two topics
explicitly at startup. Explicit creation is more deterministic — prefer it if auto-creation causes
first-message loss.

### 7.2 Test 1 — happy path (`InstrumentEnrichmentHappyPathIT`)

```java
@Test
void enrichesInstrumentAndPublishesResult() {
    runner.when(send().endpoint(instrumentsIn)
        .message().body(<inbound JSON from §2.1>));

    runner.and(http().server(refDataService)
        .receive()
        .post("/api/v1/reference-data")
        .message()
            .contentType("application/json")
            .header("Authorization", "@startsWith('Basic ')@")
            .body("""
                {"identifier":{"type":"ISIN","value":"CH0012032048"},
                 "displayName":"Roche Holding AG",
                 "baseCurrency":"CHF",
                 "assetClass":"EQUITY"}"""));

    runner.and(http().server(refDataService)
        .send()
        .response(HttpStatus.OK)
        .message().contentType("application/json")
        .body(<response JSON from §2.3>));

    runner.then(receive().endpoint(instrumentsOut)
        .message().body(<outbound JSON from §2.4>)
        .validate(json().ignore("$.receivedAt")));
}
```

This single test proves the two things that matter: the request body was correctly assembled from
multiple inbound fields, and the outbound message correctly merges inbound and response data.

**Assert the decoded basic auth credentials**, not just the `Basic ` prefix — decode the header in
the assertion or use a Citrus validation matcher against the expected encoded value.

### 7.3 Test 2 — retry succeeds (`InstrumentEnrichmentRetryIT`)

- Send one inbound message.
- Citrus HTTP server receives the request and responds `503`.
- Citrus receives the **same request again** and responds `503`.
- Citrus receives it a third time and responds `200` with the enrichment body.
- Assert the successful `ENRICHED` message on `instruments.out`.

This is the test that justifies the whole setup — it verifies redelivery count and backoff
behaviour, which route-level unit tests with mock endpoints cannot.

Give the Kafka receive a generous timeout: the backoff (300ms, ×2) plus container latency adds up.

### 7.4 Test 3 — retries exhausted (`InstrumentEnrichmentFailureIT`)

- Send one inbound message.
- Citrus HTTP server responds `503` to every request (expect `max-redeliveries + 1` calls).
- Assert the `FAILED` message on `instruments.out` with `errorCode: REFDATA_UNAVAILABLE`, using
  `@contains(...)@` or `@ignore@` for the volatile `errorMessage` text.

### 7.5 Test conventions

- Name integration tests `*IT` and bind them to the `maven-failsafe-plugin`; keep `mvn test` fast
  and container-free.
- No `Thread.sleep` anywhere. Use Citrus receive timeouts and Testcontainers wait strategies.
- Each test uses a distinct `messageId`/ISIN so a leftover message from a previous test cannot
  satisfy an assertion.
- Consider a unique Kafka consumer group per test class to avoid offset interference.

---

## 8. Milestones

| # | Deliverable | Done when |
|---|---|---|
| 1 | Skeleton | `mvn verify` passes with an empty route and no tests |
| 2 | Kafka passthrough | Route consumes `instruments.in` and republishes unchanged to `instruments.out`; one Citrus test with the Kafka container proves it |
| 3 | Request transformation + HTTP call | Happy path test (§7.2) green |
| 4 | Response merge | Outbound message matches §2.4 exactly |
| 5 | Retry | Retry test (§7.3) green |
| 6 | Failure path | Failure test (§7.4) green |
| 7 | README | Architecture sketch, how to run, known gaps list |

Do not start a milestone before the previous one is green. Commit at each milestone.

---

## 9. Acceptance criteria

1. `mvn clean verify` passes from a clean checkout with only Docker available — no manual setup,
   no locally installed Kafka, no running external service.
2. Zero `Processor`, `AggregationStrategy`, or bean classes containing message logic.
3. Every JSON structure produced by the application comes from a JSLT template — no JSON assembled
   in Java or in `simple` expressions.
4. All three tests pass ten consecutive runs without flakiness.
5. No credentials, hostnames, or ports hardcoded in Java source.
6. The route file is under 80 lines.
7. README documents: how to run, why the retained fields are exchange variables rather than
   exchange properties, the HTTP-instead-of-HTTPS test gap, and the retry-on-4xx simplification.
