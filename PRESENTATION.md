# Presenting the Instrument Enrichment PoC

## Audience and objective

This 30-minute walkthrough is for developers who know Apache Camel and currently test routes with
`AdviceWith`, mock endpoints, and datasets, but are new to Citrus and BDD-style test sequencing.

The objective is to show that Citrus complements focused route tests by testing the assembled
application at its real Kafka and HTTP boundaries.

> **Core message:** `AdviceWith` proves route fragments in isolation. Citrus proves that the
> assembled application behaves correctly at its real boundaries.

## Before the session

1. Start Docker Desktop.
2. Ensure Maven runs on Java 21 or newer:

   ```shell
   mvn --version
   ```

3. If necessary, configure Testcontainers to use a host-visible Docker socket in
   `~/.testcontainers.properties`:

   ```properties
   docker.client.strategy=org.testcontainers.dockerclient.UnixSocketClientProviderStrategy
   docker.host=unix:///var/run/docker.sock
   ```

4. Warm the dependencies and verify the demo:

   ```shell
   mvn -Ddocker.host=unix:///var/run/docker.sock clean verify
   ```

5. Increase the terminal font size and close unrelated editor tabs.

Do not run another integration-test suite concurrently. The demo Citrus HTTP server uses fixed
port `18088`.

## 0–4 minutes — Establish the testing gap

Show this flow:

```text
Kafka instruments.in
        |
        v
   Camel route
        |
        +-- JSLT request mapping
        |
        +-- authenticated HTTP call
        |
        +-- JSLT response mapping
        |
        v
Kafka instruments.out
```

Describe the existing route-test approach fairly:

- `AdviceWith` changes route inputs or outputs to `direct:` and mock endpoints.
- Datasets efficiently exercise transformations and branches.
- These tests are fast and excellent for isolated route logic.

Then identify the remaining questions:

- Does the Spring application start with the intended endpoint configuration?
- Is the real Kafka message key correct?
- Is the HTTP method, path, content type, body, and Basic authorization header correct?
- Does Camel—not the HTTP client—perform the configured retries?
- Does the error result reach Kafka after retries are exhausted?

Use this comparison:

| Focused route tests | Citrus application tests |
|---|---|
| Route DSL and transformations | Actual Kafka serialization and headers |
| Individual route fragments | Complete Spring and Camel wiring |
| Mock endpoint expectations | Real HTTP request and response behavior |
| Fast branch coverage | Retry and failure behavior across a boundary |

Say explicitly:

> This is an additional test layer, not a proposal to remove focused `AdviceWith` tests.

## 4–8 minutes — Read the production route

Open:

```text
src/main/java/ch/example/poc/InstrumentEnrichmentRoute.java
```

Walk through these points only:

1. Kafka is the real route input.
2. `messageId`, ISIN, and name are retained before the body changes.
3. JSLT creates the reference-data request.
4. The HTTP component applies Basic auth, timeouts, and status handling.
5. Camel owns redelivery behavior.
6. JSLT creates success and failure results.
7. The Kafka output key is the ISIN.
8. The business `messageId` is propagated through structured logging.

Point out what the test does **not** require:

- No `AdviceWith`
- No mocked or skipped route endpoints
- No test-only branches in production code
- No Java processors or mapping beans

## 8–13 minutes — Introduce the Citrus mental model

Open:

```text
src/test/java/ch/example/poc/InstrumentEnrichmentHappyPathIT.java
```

Summarize the scenario:

```text
WHEN  Citrus publishes an input event to Kafka
AND   Citrus receives and validates the application's HTTP request
AND   Citrus sends an HTTP response
THEN  Citrus receives and validates the Kafka result
```

Explain the direction of each action:

```java
send().endpoint(instrumentsIn)
```

Citrus sends a message to the application.

```java
http().server(refDataService).receive()
```

Citrus receives and validates a request sent by the application.

```java
http().server(refDataService).send()
```

Citrus acts as the external service and returns a response.

```java
receive().endpoint(instrumentsOut)
```

Citrus receives and validates the application's final Kafka message.

Clarify the BDD terminology:

- `when` marks the trigger.
- `and` continues the interaction.
- `then` marks the observable result.
- These words improve readability; they do not introduce a separate runtime or specification
  language.

## 13–18 minutes — Run the happy path

Run only the happy-path test:

```shell
mvn -Ddocker.host=unix:///var/run/docker.sock \
  -Dit.test=InstrumentEnrichmentHappyPathIT verify
```

Point out that the test starts:

- The complete Spring Boot application
- The unmodified Camel routes
- A real Kafka broker in Testcontainers
- A Citrus HTTP server representing the external dependency

If `jq` is installed, show one correlated message lifecycle:

```shell
mvn -Ddocker.host=unix:///var/run/docker.sock \
  -Dit.test=InstrumentEnrichmentHappyPathIT verify 2>&1 \
  | jq -R 'fromjson? | select(.messageId == "milestone-3")'
```

Expected operations:

```text
Received instrument
Calling reference data service
Reference data lookup succeeded
Enrichment completed
Publishing result
Published result
```

Every message-related log entry contains the same business `messageId`, including asynchronous
Kafka publication.

## 18–22 minutes — Demonstrate retry behavior

Open:

```text
src/test/java/ch/example/poc/InstrumentEnrichmentRetryIT.java
```

Focus on the scenario sequence:

```text
request 1 -> 503
request 2 -> 503
request 3 -> 200
Kafka output -> ENRICHED
```

Explain what this proves:

- The external request is assembled correctly on every attempt.
- Camel sees genuine HTTP `503` responses.
- Camel performs exactly the configured number of calls.
- Apache HttpClient automatic retries are disabled, so it cannot inflate the call count.
- The final successful response remains correlated with the original Kafka message.

Optionally run:

```shell
mvn -Ddocker.host=unix:///var/run/docker.sock \
  -Dit.test=InstrumentEnrichmentRetryIT verify
```

Briefly mention the failure-path companion:

```text
src/test/java/ch/example/poc/InstrumentEnrichmentFailureIT.java
```

It returns `503` for every attempt and validates the final `FAILED` Kafka message.

## 22–26 minutes — Show business-readable scenarios

Open:

```text
src/test/java/ch/example/poc/InstrumentEnrichmentBusinessScenariosIT.java
```

Then open one scenario directory:

```text
src/test/resources/scenarios/allianz/
├── input.json
├── expected-request.json
├── enrichment.json
└── expected-output.json
```

Explain the design:

- JUnit runs one named invocation per business scenario.
- Citrus reads JSON documents directly from the classpath.
- Input and expectations are independent artifacts.
- Every field and the Kafka key are checked exactly.
- There are no `@ignore@` matchers in these business scenarios.
- A failure identifies the affected company instead of failing one large loop.

Relate this to the team's existing datasets:

> The resource directory plays a role similar to a dataset, but the scenario drives the complete
> application and verifies both external boundaries rather than one isolated route fragment.

Run the parameterized scenarios if time permits:

```shell
mvn -Ddocker.host=unix:///var/run/docker.sock \
  -Dit.test=InstrumentEnrichmentBusinessScenariosIT verify
```

Expected result: three successful invocations—Allianz, TotalEnergies, and Philips.

## 26–28 minutes — Explain concurrent submission

Open:

```text
src/test/java/ch/example/poc/InstrumentEnrichmentConcurrentMessagesIT.java
```

Be precise about what it demonstrates:

- Three Citrus producers submit Kafka messages concurrently.
- The application deliberately has one Kafka consumer.
- That consumer processes the queued messages sequentially.
- Submission order is unspecified.
- This is a queue-handling smoke test, not the strict field-correlation test.
- Exact business correlation is covered by the resource-backed parameterized scenarios.

Avoid describing this as concurrent route execution.

## 28–30 minutes — Recommend an adoption strategy

Show this model:

```text
Many focused route tests
AdviceWith + mock endpoints + datasets
                  |
                  v
A few application-boundary tests
Citrus + real Kafka + simulated HTTP peer
```

Recommend keeping existing route tests for:

- Complex routing branches
- Transformation edge cases
- Fast feedback during implementation
- Large datasets that do not need real transports

Recommend Citrus tests for risks at application boundaries:

- Authentication and HTTP protocol details
- Retry and terminal failure behavior
- Kafka keys and headers
- Serialization and deserialization
- Spring property and endpoint wiring
- Correlation across multiple external interactions

Close with:

> We are not replacing focused route tests. We are adding confidence that the route Spring starts
> speaks the protocols we expect and still behaves correctly after the mocks are removed.

## Questions to invite

- Which existing incidents would have required a real boundary-level test to catch?
- Which integrations have retry behavior that is currently mocked away?
- Which two or three critical flows would benefit most from one Citrus happy-path and one failure
  test?
- Should container-backed tests run on every commit or in a dedicated verification stage?

## If the live demo fails

Do not debug Docker during the presentation.

1. Show the already-green test source and explain the expected interactions.
2. Open `target/failsafe-reports` if reports from the preparation run are still available.
3. Show the structured-log example in `README.md` and continue with the retry test walkthrough.
4. State that the failure is test infrastructure startup, not an application assertion failure.

The discussion should remain focused on the testing model rather than Docker Desktop.
