from dagster import job, op, OpExecutionContext, In, Nothing

# ---------------------------------------------------------------------------
# Dim tables
# ---------------------------------------------------------------------------

@op(
    required_resource_keys={"dbt"},
    config_schema={"snapshot_id": int, "snapshot_ts": str},
)
def run_dim_airplane(context: OpExecutionContext) -> None:
    snapshot_ts = context.op_config["snapshot_ts"]
    context.log.info("Running dbt model: dim_airplane")
    context.resources.dbt.run_model("dim_airplane", vars={"snapshot_ts": snapshot_ts})
    context.log.info("dim_airplane complete")

@op(
    required_resource_keys={"dbt"},
    config_schema={"snapshot_id": int, "snapshot_ts": str},
)
def run_dim_date(context: OpExecutionContext) -> None:
    snapshot_ts = context.op_config["snapshot_ts"]
    context.log.info("Running dbt model: dim_date")
    context.resources.dbt.run_model("dim_date", vars={"snapshot_ts": snapshot_ts})
    context.log.info("dim_date complete")

@op(
    required_resource_keys={"dbt"},
    config_schema={"snapshot_id": int, "snapshot_ts": str},
)
def run_dim_carrier(context: OpExecutionContext) -> None:
    snapshot_ts = context.op_config["snapshot_ts"]
    context.log.info("Running dbt model: dim_carrier")
    context.resources.dbt.run_model("dim_carrier", vars={"snapshot_ts": snapshot_ts})
    context.log.info("dim_carrier complete")

@op(
    required_resource_keys={"dbt"},
    config_schema={"snapshot_id": int, "snapshot_ts": str},
)
def run_dim_time(context: OpExecutionContext) -> None:
    snapshot_ts = context.op_config["snapshot_ts"]
    context.log.info("Running dbt model: dim_time")
    context.resources.dbt.run_model("dim_time", vars={"snapshot_ts": snapshot_ts})
    context.log.info("dim_time complete")

@op(
    required_resource_keys={"dbt"},
    config_schema={"snapshot_id": int, "snapshot_ts": str},
)
def run_dim_delays(context: OpExecutionContext) -> None:
    snapshot_ts = context.op_config["snapshot_ts"]
    context.log.info("Running dbt model: dim_delays")
    context.resources.dbt.run_model("dim_delays", vars={"snapshot_ts": snapshot_ts})
    context.log.info("dim_delays complete")

# ---------------------------------------------------------------------------
# Facts table - run dim tables before it
# ---------------------------------------------------------------------------

@op(
    required_resource_keys={"dbt"},
    config_schema={"snapshot_id": int, "snapshot_ts": str},
    ins={
        "after_airplane": In(Nothing),
        "after_date": In(Nothing),
        "after_carrier": In(Nothing),
        "after_time": In(Nothing),
        "after_delays": In(Nothing),
    },
)
def run_fact_flights(context: OpExecutionContext) -> None:
    snapshot_id = context.op_config["snapshot_id"]
    snapshot_ts = context.op_config["snapshot_ts"]
    context.log.info(
        f"Running dbt model: fact_flights (incremental) "
        f"for snapshot_id={snapshot_id}, snapshot_ts={snapshot_ts}"
    )
    context.resources.dbt.run_model("fact_flights", vars={"snapshot_ts": snapshot_ts})
    context.log.info("fact_flights complete")

# ---------------------------------------------------------------------------
# Tests — run after fact_flights completes
# ---------------------------------------------------------------------------

ALL_MODELS = [
    "dim_airplane",
    "dim_date",
    "dim_carrier",
    "dim_time",
    "dim_delays",
    "fact_flights",
]

@op(
    required_resource_keys={"dbt"},
    config_schema={"snapshot_id": int, "snapshot_ts": str},
    ins={"after_fact_flights": In(Nothing)},
)
def run_dbt_tests(context: OpExecutionContext) -> None:
    snapshot_ts = context.op_config["snapshot_ts"]
    context.log.info("Running dbt tests for all models")
    context.resources.dbt.test_models(ALL_MODELS, vars={"snapshot_ts": snapshot_ts})
    context.log.info("dbt tests complete")

# ---------------------------------------------------------------------------
# Job — run dim jobs in parallel
# ---------------------------------------------------------------------------

@job(
    tags={"dagster/concurrency_key": "on_new_snapshot_job"},
    config={
        "ops": {
            "run_fact_flights": {
                "config": {"snapshot_id": 0, "snapshot_ts": "1970_01_01"}
            },
            "run_dim_airplane": {
                "config": {"snapshot_id": 0, "snapshot_ts": "1970_01_01"}
            },
            "run_dim_date": {
                "config": {"snapshot_id": 0, "snapshot_ts": "1970_01_01"}
            },
            "run_dim_carrier": {
                "config": {"snapshot_id": 0, "snapshot_ts": "1970_01_01"}
            },
            "run_dim_time": {
                "config": {"snapshot_id": 0, "snapshot_ts": "1970_01_01"}
            },
            "run_dim_delays": {
                "config": {"snapshot_id": 0, "snapshot_ts": "1970_01_01"}
            },
            "run_dbt_tests": {
                "config": {"snapshot_id": 0, "snapshot_ts": "1970_01_01"}
            },
        }
    }
)
def on_new_snapshot_job():
    
    airplane = run_dim_airplane()
    date = run_dim_date()
    carrier = run_dim_carrier()
    time = run_dim_time()
    delays = run_dim_delays()
    
    fact = run_fact_flights(
        after_airplane=airplane,
        after_date=date,
        after_carrier=carrier,
        after_time=time,
        after_delays=delays,
    )

    run_dbt_tests(after_fact_flights=fact)