from dagster import Definitions
from resources import IcebergRestResource, DbtCustomResource
from jobs import on_new_snapshot_job
from sensors import iceberg_snapshot_sensor

defs = Definitions(
    jobs=[on_new_snapshot_job],
    sensors=[iceberg_snapshot_sensor],
    resources={
        "iceberg": IcebergRestResource(catalog_url="http://iceberg-rest:8181"),
        "dbt": DbtCustomResource(
            project_dir="/opt/dbt/flights",
            profiles_dir="/opt/dbt/flights",
            target="dev",
        ),
    },
)