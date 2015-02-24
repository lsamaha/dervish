# dervish

Demonstrates use of AWS kinesis consumers to write event data to s3 and index metadata in dynamodb.

## About

A dervish is a collection of consumers that consume application events produced by dervishers.

## Getting Started

A dervish is created by installing the dervish python package and calling the entry point whirl. All necessary
AWS resources are generated if they do not already exist.

### Warning: not all of these AWS resources are free and cost estimates vary. If the dervishers are stopped,
the cost should be low or none. If they are run for a long time, or higher throughput is tested, the costs accumulate.

## Example

```bash
pip install dervish
dervish
usage: dervish
```

## Result

The result of invoking the dervish is:
- validation of the associated AWS account for this environment (see AWS' own environment documentation)
- creation of a dynamodb table named 'event' if it does not already exist
- creation of a range index on the event "at" numeric epoch property
- a single shard kinesis consumer on the dervisher stream
- an entry in the event table for each event message consumed
- an entry in s3 at <bucket>/event for each event message consumed

## Do It Yourself

By reviewing the docs and the whirling class, it should be easy to see how to generate a graph of custom consumers.
