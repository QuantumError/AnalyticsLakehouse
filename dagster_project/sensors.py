import json
from dagster import sensor, RunRequest, SensorEvaluationContext, SkipReason
from jobs import on_new_snapshot_job
from datetime import datetime, timezone

WATCHED_TABLES = [
    ("raw", "raw_data"),
]

@sensor(job=on_new_snapshot_job, minimum_interval_seconds=30, required_resource_keys={"iceberg"})
def iceberg_snapshot_sensor(context: SensorEvaluationContext):
    iceberg = context.resources.iceberg
    cursor_state: dict = json.loads(context.cursor) if context.cursor else {}
    run_requests = []

    for namespace, table in WATCHED_TABLES:
        cursor_key = f"{namespace}__{table}__last_snapshot"
        try:
            snapshots = iceberg.get_snapshots(namespace, table)
        except Exception as e:
            context.log.warning(f"Could not reach {namespace}.{table}: {e}")
            continue

        if not snapshots:
            continue

        latest_snapshot = max(snapshots, key=lambda s: s["timestamp-ms"])
        latest_id = latest_snapshot["snapshot-id"]

        snapshot_ts = datetime.fromtimestamp(
            latest_snapshot["timestamp-ms"] / 1000, tz=timezone.utc
        ).strftime("%Y_%m_%d__%H_%M_%S")

        last_seen = cursor_state.get(cursor_key)
        last_seen = int(last_seen) if last_seen is not None else None

        if last_seen is None or latest_id != last_seen:
            context.log.info(
                f"New snapshot on {namespace}.{table}: {last_seen} → {latest_id}"
            )
            op_config = {"snapshot_id": latest_id, "snapshot_ts": snapshot_ts}
            run_requests.append(
                RunRequest(
                    run_key=f"{namespace}__{table}__{latest_id}",
                    run_config={
                        "ops": {
                            "run_fact_flights": {"config": op_config},
                            "run_dim_airplane": {"config": op_config},
                            "run_dim_date":     {"config": op_config},
                            "run_dim_carrier":  {"config": op_config},
                            "run_dim_time":     {"config": op_config},
                            "run_dim_delays":   {"config": op_config},
                            "run_dbt_tests":    {"config": op_config},
                        }
                    },
                    tags={"namespace": namespace, "table": table},
                )
            )
            cursor_state[cursor_key] = str(latest_id)

    context.update_cursor(json.dumps(cursor_state))

    if run_requests:
        yield from run_requests
    else:
        yield SkipReason("No new snapshots detected.")