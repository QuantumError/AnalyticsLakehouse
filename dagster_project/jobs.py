import os
import shutil
import stat

from dagster import Out, job, op, OpExecutionContext, In, Nothing

#----------------------------------------------------------------------------
# Dependency management op — runs dbt deps, ensures clean state before installing packages
#----------------------------------------------------------------------------

@op(required_resource_keys={"dbt"}, out=Out(Nothing))
def dbt_deps(context: OpExecutionContext) -> None:
    packages_dir = "/opt/dbt/flights/dbt_packages"
    
    if os.path.exists(packages_dir):
        context.log.info(f"Removing existing packages at {packages_dir}")
        try:
            shutil.rmtree(packages_dir)
            context.log.info("Successfully removed existing packages directory")
        except PermissionError:
            context.log.warning("Standard removal failed due to permissions. Attempting force-delete via chmod...")
            try:
                for root, dirs, files in os.walk(packages_dir):
                    for d in dirs:
                        os.chmod(os.path.join(root, d), stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)
                    for f in files:
                        os.chmod(os.path.join(root, f), stat.S_IRWXU | stat.S_IRWXG | stat.S_IRWXO)
                shutil.rmtree(packages_dir)
                context.log.info("Successfully forced removal of the packages directory")
            except Exception as e:
                context.log.error(f"Failed to remove packages directory: {e}")
                raise

    context.log.info("Installing dbt packages...")
    result = context.resources.dbt.deps()
    
    context.log.info(f"dbt deps completed with result: {result}")
    
    if os.path.exists(packages_dir):
        installed_packages = os.listdir(packages_dir)
        context.log.info(f"Installed packages: {installed_packages}")
    else:
        context.log.error(f"Packages directory not found after dbt deps: {packages_dir}")
        raise RuntimeError("dbt deps failed to install packages")

# ---------------------------------------------------------------------------
# Dimension ops — run in parallel, always full table refreshes
# ---------------------------------------------------------------------------

@op(required_resource_keys={"dbt"}, ins={"deps": In(Nothing)})
def run_dim_airports(context: OpExecutionContext) -> None:
    context.log.info("Running dbt model: dim_airports")
    context.resources.dbt.run_model("dim_airports")
    context.log.info("dim_airports complete")

@op(required_resource_keys={"dbt"}, ins={"deps": In(Nothing)})
def run_dim_calendar(context: OpExecutionContext) -> None:
    context.log.info("Running dbt model: dim_calendar")
    context.resources.dbt.run_model("dim_calendar")
    context.log.info("dim_calendar complete")

@op(required_resource_keys={"dbt"}, ins={"deps": In(Nothing)})
def run_dim_carriers(context: OpExecutionContext) -> None:
    context.log.info("Running dbt model: dim_carriers")
    context.resources.dbt.run_model("dim_carriers")
    context.log.info("dim_carriers complete")

# ---------------------------------------------------------------------------
# Fact op — incremental append, waits for all dims to finish
# ---------------------------------------------------------------------------

@op(
    required_resource_keys={"dbt"},
    config_schema={"snapshot_id": int},
    ins={
        "after_airports": In(Nothing),
        "after_calendar": In(Nothing),
        "after_carriers": In(Nothing),
    },
)
def run_fact_flights(context: OpExecutionContext) -> None:
    snapshot_id = context.op_config["snapshot_id"]
    context.log.info(
        f"Running dbt model: fact_flights (incremental) "
        f"for snapshot_id={snapshot_id}"
    )
    context.resources.dbt.run_model("fact_flights")
    context.log.info("fact_flights complete")

# ---------------------------------------------------------------------------
# Job — dims fan-out in parallel, fact fans-in after all three finish
# ---------------------------------------------------------------------------

@job(
    config={
        "ops": {
            "run_fact_flights": {
                "config": {"snapshot_id": 0}
            }
        }
    }
)
def on_new_snapshot_job():
    deps_complete = dbt_deps()
    
    airports = run_dim_airports(deps=deps_complete)
    calendar = run_dim_calendar(deps=deps_complete)
    carriers = run_dim_carriers(deps=deps_complete)

    run_fact_flights(
        after_airports=airports,
        after_calendar=calendar,
        after_carriers=carriers,
    )