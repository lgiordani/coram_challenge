import time
import itertools
import operator
from collections import defaultdict
from pprint import pprint

import sqlalchemy as sa

#############################################
#############################################
# Please have a look at the file NOTES.md
# for some comments about the implementation
#############################################
#############################################

ALARM_CONSECUTIVE_EVENTS = 5

INSERT_EVENT_STATEMENT = sa.text(
    """
    INSERT INTO events (time, type)
    VALUES (:time, :type)
    """
)


def database_connection() -> sa.Connection:
    engine = sa.create_engine("postgresql://postgres:postgres@postgres:5432/postgres")

    for attempt in range(5):
        try:
            conn = engine.connect()
        except sa.exc.OperationalError as e:
            if attempt == 4:
                raise e
            time.sleep(1)

    conn.execute(
        sa.text(
            "CREATE TABLE IF NOT EXISTS events "
            "(id SERIAL PRIMARY KEY, time TIMESTAMP WITH TIME ZONE, type VARCHAR)"
        )
    )

    return conn


class PreprocessIngest:
    """
    This decorator will preprocess the ingest parameters
    and keep track of the number of consecutive events
    of the same type and raise an alarm in case that
    is over a threshold.
    """

    def __init__(self, threshold):
        # This is a tuple (event_type, number_of_occurrences)
        self.latest_event = (None, 0)
        self.threshold = threshold

    def __call__(self, wrapped_func):
        def _wrap(conn: sa.Connection, timestamp: str, event_type: str):
            event, occurrences = self.latest_event

            if event_type != event:
                self.latest_event = (event_type, 1)
                return wrapped_func(conn, timestamp, event_type)

            if (occurrences := occurrences + 1) > self.threshold:
                print(
                    (
                        f"WARNING!! Event of type {event_type} "
                        f"occurred {self.threshold} times"
                    )
                )
                self.latest_event = (None, 0)
                return wrapped_func(conn, timestamp, event_type)

            self.latest_event = (event_type, occurrences)
            return wrapped_func(conn, timestamp, event_type)

        return _wrap


@PreprocessIngest(ALARM_CONSECUTIVE_EVENTS)
def ingest_data(conn: sa.Connection, timestamp: str, event_type: str):
    conn.execute(
        INSERT_EVENT_STATEMENT, parameters={"time": timestamp, "type": event_type}
    )


def aggregate_events(conn: sa.Connection) -> dict[str, list[tuple[str, str]]]:
    resultproxy = conn.execute(
        sa.text(
            """
            WITH categories AS (
              SELECT
                CASE
                  WHEN type in ('pedestrian', 'bicycle')
                  THEN 'people'
                  ELSE 'vehicles'
                END AS category,
                time
              FROM events
            ), reset AS (
              SELECT
                category,
                time,
                CASE WHEN time - LAG(time, 1)
                  OVER (
                    PARTITION BY category
                    ORDER BY time
                  ) > interval '1 minute'
                  THEN 1 END as reset_marker
              FROM categories
              ORDER BY time, category
            ), groups AS (
              SELECT
                category,
                time,
                COUNT(reset_marker) OVER (ORDER BY time) AS group
              FROM reset
            )
            SELECT
              category,
              MIN(time),
              MAX(time)
            FROM groups
            GROUP BY category, groups.group
            ORDER BY MIN(time)
            """
        )
    )

    # Group by element 0, which is the category
    # From the pure elegance point of view it would be better to
    # use `_mapping` of each rowproxy in resultproxy,
    # but this will affect performances a bit
    grouped_events = itertools.groupby(resultproxy, operator.itemgetter(0))

    # The format of each grouped event, according to the query above is
    # (category, interval_start, interval_end)
    # Thus, when we unpack it we can capture interval_start and interval_end
    # (elements 1 and 2) and convert them into strings

    grouped_events_dict = {"people": [], "vehicles": []}

    for group in grouped_events:
        category, events = group

        # This captures interval_start and interval_end
        timestamps = [(event[1].isoformat(), event[2].isoformat()) for event in events]
        grouped_events_dict[category] = timestamps

    return grouped_events_dict


def main():
    conn = database_connection()

    # Simulate real-time events every 30 seconds
    events = [
        ("2023-08-10T18:30:30", "pedestrian"),
        ("2023-08-10T18:31:00", "pedestrian"),
        ("2023-08-10T18:31:00", "car"),
        ("2023-08-10T18:31:30", "pedestrian"),
        ("2023-08-10T18:35:00", "pedestrian"),
        ("2023-08-10T18:35:30", "pedestrian"),
        ("2023-08-10T18:36:00", "pedestrian"),
        ("2023-08-10T18:37:00", "pedestrian"),
        ("2023-08-10T18:37:30", "pedestrian"),
    ]

    # This test implements the example in README.md
    # events = [
    #     ("2023-08-10T18:30:30", "car"),
    #     ("2023-08-10T18:31:00", "car"),
    #     ("2023-08-10T18:31:30", "car"),
    #     ("2023-08-10T18:35:00", "car"),
    #     ("2023-08-10T18:35:30", "car"),
    #     ("2023-08-10T18:36:00", "car"),
    #     ("2023-08-10T18:37:30", "car"),
    #     ("2023-08-10T18:38:00", "car"),
    # ]

    for timestamp, event_type in events:
        ingest_data(conn, timestamp, event_type)

    aggregate_results = aggregate_events(conn)

    print(f"Aggregate results")
    pprint(aggregate_results)


if __name__ == "__main__":
    main()
