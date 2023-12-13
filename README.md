# plumber kafka
## easy to use Kafka CLI tool that plays nicely with Plumber

This is a simple CLI tool for kafka that is similar to [kcat](https://github.com/edenhill/kcat). It is pure rust
and is compatible with [Plumber](https://github.com/MostlyMax/plumber) (this just means that it handles signals in a way that Plumber expects).

## features
- producing to a topic
- balanced consuming from list of topics

## example
This example assumes that you already have a Kafka cluster running with an existing topic. The easiest way to do this is by
following the guide on the [Apache Kafka Quickstart](https://kafka.apache.org/quickstart).
