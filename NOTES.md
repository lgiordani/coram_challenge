# Notes

While I understand this is a simple technical challenge, it's nevertheless interesting to mention some possible choices for a real implementation

## Bulk insertion

Is ingestion our bottleneck?

If the rate is very high we might want to discuss if it's mandatory to have data ingested and processed immediately or if we can have a buffer and ingest bulk data. For example, we might collect data for 5 minutes and then ingest it in one go.

In this case, I'd prefer to route data to a stream processing system like Kafka to have it handy for some initial preprocessing or early alarms, and then to extract it periodically and load it into Postgres.

There are many options for optimised bulk ingestion, like `COPY` or multi-values inserts. This is a good solution if the storage on Postgres is done to keep historical data and not for immediate consumption.

## Sharding

There are other ways to improve performances but they strongly depend on the wider use case, so I do not have enough data at the moment to suggest one solution or the other. However, for the sake of completeness, I think sharding might be interesting.

It allows us to implement parallel ingestion, assuming different event types come from different sources (e.g. two different detectors). We can also shard by timestamp, even though it's probably not what we want in this case, as that is typical in a long-term storage system. If the system has both the need to store events for a long time and to process them in real time it is again useful to consider sending the same event to multiple destinations, for example two databases sharded with different strategies.

## Database maintenance

If we ingest a lot of data it might be useful to think about the rate indexes are recreated, and the rate ANALYSE is run. Again, if we go down the bulk ingestion it's simple to drop and recreate them around the ingestion event, otherwise we need to schedule some periodic update.

## SQL optimizations

The CTEs used in the aggregation might be reimplemented as functions or views, which would boost performances.

## Python group_by

I'm not totally sure about the ordering after a `group_by` in Python. I'd like to investigate the docs and the code to make sure the output is coherent with the `ORDER BY` implemented in SQL. Otherwise, we might need to order results in Python, which would affect performances.

## Python decorator

The Python decorator is a proper alternative to using global variables, which in general are a bad practice. I decided to create a parametrised decorator as it seems to fit the use case better, but a simpler hard coded class- or function-based decorator would have solved the problem anyway.
