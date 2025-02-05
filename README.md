# Fluent Bit EMF Aggregator

This provides a PoC For a fluentbit plugin that takes an emf stream, aggreates the data, then emits a combined stream, where possible, on a aggreagation period.

This can drastically reduce the size of logs pushed into CloudWatch while allowing your service to push request level emf logs into the fluentbit use case.

## Usecase

Cloudwatch logs ingestion can be expensive if you emit a lot of extrenuous data to it (https://www.reddit.com/r/aws/comments/1g4vkt5/cloudwatch_logs_cost/, https://last9.io/blog/how-to-cut-down-aws-cloudwatch-costs/, https://bit.kevinslin.com/p/youre-paying-too-much-for-cloudwatch, https://cast.ai/blog/the-truth-about-cloudwatch-pricing/). Emitting emf logs per request can emit a lot of data into Cloudwatch. However, if you still want to have access to request level emf logs, but don't want to send every emf emitted to cloudwatch for custom metrics there is no good existing solution.

This is a PoC to unlock that functionality. The expectation is your application will emit request level emf logs into your fluentbit environment. You can then direct that input to 2 different outputs: 1 being this plugin which will aggregate it then push those into cloudwatch to be converted to custom metrics; the other being an output stream that takes those request level logs and pushes them into some other environment for when you need access to the request level data (disk, s3, etc.).

## Caveats

1. The goal of this usecase is to reduce the size of the logs being ingested into CloudWatch, and so, to push for that goal, this code stripes out any non-relevant keys in the emf; only attributes necessary for the emf format and references by the dimensions on the metrics is maintained.

2. This is not meant to be a solution just for aggregating emf. If your usecase does not need request level metrics, you are better off just using a in application aggregator and using the existing cloudwatch fluentbit plugin to push those into cloudwatch. Going from `internal metric (your application) -> emf -> internal metric (this plugin) -> emf -> cloudwatch` is not an efficient use of resources.

3. This will end up outputting multiple emf stream per aggregation period due to the format constraints of emf. If your metrics depend on a dimension called `api` and your application is emitting emf records with different values for the `api` dimension, there is no way to merge these together, therefore you will get a separate output stream for each value of `api` emitted.

4. Due to a weird bug between golang 1.21 and fluent-bit (see https://github.com/golang/go/issues/62440), I am forced to use golang 1.20 instead. This always means I am forced to use an older version of the cloudwatch logs client, 1.36.0.

5. This code makes no optimizations about the metric values that are emited, it uses them as is. This means limiting the percision of the values emited will help compress the outputs further. E.X if the application emits a latency value with nanosecond percision, the latency metric will be emitted with a nanosecond percision. However, the EMF format is optimized for being able to compress mutli data points of the same value into a smaller form factor. So emitting 1200 ns and emitting 1201 ns are both emitted with a count of 1 `values: [1200, 1201], counts: [1, 1]`. Instead if milisecond percision is used, these will be emit as the same value, resulting in a more compressed output `values: [1200], count: [2]`. With this in mind, think though how much percision is really necessary for your metrics and emit the lowest percision that still meets your requirements.

## Project structure

This project contains the PoC for the fluentbit plugin written in `golang` under the `fluent-bit-emf` folder.

This fluent-bit-test folder and the toplevel Dockerfile.fluent-bit setups a sample usage that launches fluent-bit 1.19 (the target for this PoC) with a basic configuration and this plugin running in.

In order to test this and get confirmation of behavior as well as "compression" stats, a test-bed docker image is available under `test-bed` which provides a basic go program that just emits random emf formatted logs.

At the top level is a `compose.yml` file, which will launch the test-bed and the example fluentbit configuration, with the aggregator plugin. It emits logs detailing how much of the input is being compressed by the plugin, ex.

`mock-cloudwatch-server` contains a mock cloudwatch logs server implementation in order to test the integration with cloudwatch without actually having to hit cloudwatch apis. This is launched by default in the docker compose file. In order to switch between it and logging to disk, update the [test fluent-bit-config](https://github.com/anthonydresser/fluent-bit-emf-aggregator/blob/main/fluent-bit-test/fluent-bit.conf#L13).

``` bash
fluent-bit-1     | [info] [emf-aggregator] Compressed 1041505 bytes into 64610 bytes or 93%; and 1754 Records into 36 or 97%
```

The testbed PoC can be ran via `docker compose up --build`;
