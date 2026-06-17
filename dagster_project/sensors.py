import json
from dagster import sensor, RunRequest, SensorEvaluationContext, SkipReason
from jobs import on_new_snapshot_job

WATCHED_TABLES = [
    ("raw", "raw_data"),
]

#----------------------------------------------------------------------------
# Sensor — watches for new Iceberg snapshots, triggers a run with the snapshot_id in config
#----------------------------------------------------------------------------

@sensor(job=on_new_snapshot_job, minimum_interval_seconds=30, required_resource_keys={"iceberg"})
def iceberg_snapshot_sensor(context: SensorEvaluationContext):
    iceberg = context.resources.iceberg

    cursor_state: dict = json.loads(context.cursor) if context.cursor else {}

    run_requests = []

    for namespace, table in WATCHED_TABLES:
        cursor_key = f"{namespace}__{table}__last_snapshot"

        try:
            latest_id = iceberg.get_latest_snapshot_id(namespace, table)
        except Exception as e:
            context.log.warning(f"Could not reach {namespace}.{table}: {e}")
            continue

        if latest_id is None:
            continue

        last_seen = cursor_state.get(cursor_key)
        last_seen = int(last_seen) if last_seen is not None else None

        if last_seen is None or latest_id != last_seen:
            context.log.info(
                f"New snapshot on {namespace}.{table}: "
                f"{last_seen} → {latest_id}"
            )
            run_requests.append(
                RunRequest(
                    run_key=f"{namespace}__{table}__{latest_id}",
                    run_config={
                        "ops": {
                            "run_fact_flights": {
                                "config": {
                                    "snapshot_id": latest_id,
                                }
                            }
                        }
                    },
                    tags={"namespace": namespace, "table": table},
                )
            )
            cursor_state[cursor_key] = str(latest_id)

    context.update_cursor(json.dumps(cursor_state))
    return run_requests if run_requests else SkipReason("No new snapshots detected.")