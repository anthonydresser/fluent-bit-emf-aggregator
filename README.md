# Fluent Bit EMF Aggregator

This provides a PoC For a fluentbit plugin that takes an emf stream, aggreates the data, then emits a combined stream, where possible, on a aggreagation period.

This can drastically reduce the size of logs pushed into CloudWatch while allowing your service to push request level emf logs into the fluentbit use case.

## Usecase

Cloudwatch ingestion is expensive. Emitting emf logs per request into Cloudwatch can sky rocket those ingestion costs. However, if you still want to emit request level emf logs into another environment (not Cloudwatch), but still want emf capatible metrics in cloudwatch to be used with Custom Metrics, there is not really good way to do this.

This is a PoC to unlock that functionality. The expectation is your application will emit request level emf logs into your fluentbit environment. You can then direct that input to 2 different outputs: 1 being this plugin which will aggregate it then push those into cloudwatch to be converted to custom metrics; the other being an output stream that takes those request level logs and pushes them into some other environment that you can then query at the request level.


## Caveats

1. The goal of this usecase is to reduce the size of the logs being ingested into CloudWatch, and so, to push for that goal, this code stripes out any non-relevant keys in the emf; only attributes necessary for the emf format and references by the dimensions on the metrics is maintained.

2. This is not meant to be a solution just for aggregating emf. If your usecase does not need request level metrics, you are better off just using a in application aggregator and using the existing cloudwatch fluentbit plugin to push those into cloudwatch. Going from `internal metric (your application) -> emf -> internal metric (this plugin) -> emf -> cloudwatch` is not an efficient use of resources.

3. This will end up outputting multiple emf stream per aggregation period due to the format constraints of emf. If your metrics depend on a dimension called `api` and your application is emitting emf records with different values for the `api` dimension, there is no way to merge these together, therefore you will get a separate output stream for each value of `api` emitted.

4. Due to a weird bug between golang 1.21 and fluent-bit, I am forced to use golang 1.20 instead. Think always means we are forced to use an older version of the cloudwatch logs client, 1.36.0.

## Project structure

This project contains the PoC for the fluentbit plugin written in `golang` under the `fluent-bit-emf` folder.

This fluent-bit-test folder and the toplevel Dockerfile.fluent-bit setups a sample usage that launches fluent-bit 1.19 (the target for this PoC) with a basic configuration and this plugin running in.

In order to test this and get confirmation of behavior as well as "compression" stats, a test-bed docker image is available under `test-bed` which provides a basic go program that just emits random emf formatted logs.

At the top level is a `compose.yml` file, which will launch the test-bed and the example fluentbit configuration, with the aggregator plugin. It emits logs detailing how much of the input is being compressed by the plugin, ex.

``` bash
fluent-bit-1     | [info] [emf-aggregator] Compressed 1041505 bytes into 64610 bytes or 93%; and 1754 Records into 36 or 97%
```

The testbed PoC can be ran via `docker compose up --build`;
