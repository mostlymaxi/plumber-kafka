# plumber kafka
## easy to use Kafka CLI tool that plays nicely with Plumber

This is a utility meant to replace [kcat](https://github.com/edenhill/kcat).

It can either produce messages by reading stdin and pushing to a topic

or

Consume messages by polling a topic and pushing to stdout.

## why make a kcat replacement?
kcat doesn't handle signals well. That's it.

plumber kafka will make sure to flush messages on exit which makes it much more preferable when using with pipelines that are expected to run smoothly.
