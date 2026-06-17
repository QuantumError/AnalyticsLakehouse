# Information

This project features a local data lakehouse pipeline.
The project is fully contained within a Docker environment to ensure safe practices and local functionality.
The goal of the project is to create a scalable, open-source lakehouse pipeline that can function as an alternative to more expensive vendor solutions.

# Requirements and installations

This project requires the use of Docker. You can install Docker at https://docs.docker.com/engine/install/.

The dataset used in this project is found at https://www.kaggle.com/datasets/ahmedelsayedrashad/airline-on-time-performance-data.
You should install the dataset locally and extract it into the /data subfolder in the project. 

Other installations are done via the docker-compose file. 

To start up the project, you need to open a CLI at the project folder and type 'docker-compose up'. That will start all the necessary services. 

After that, you can run the script that gets data from the raw .parquet dataset into the Iceberg REST catalog database by running command 'docker exec -it spark-iceberg spark-submit /home/iceberg/notebooks/ingest.py'. Note that this command can be quite CPU heavy, so check that you have high enough CPU and memory limits in Docker.

Now that the data is in Iceberg, the next step is to enable workflow orchestration for the lakehouse. This is done via Dagster. Your local Dagster is located at http://localhost:3000 (open that in your browser). Inside the Dagster visual environment, go to the 'Automation' page, then set 'Running' slider on. This will set up the sensors that check for new Iceberg snapshots every 30 seconds and send dbt models downstream. This process is even more CPU heavy, since it sends a dbt star schema downstream, and requires several minutes to run. 

After the first Dagster jobs have finished running, you can go to Superset for data analytics. Your local Superset is at http://localhost:8088. Login with username admin and password admin. Inside Superset, go to 'Settings' -> 'Database Connections'. Click '+Database', select 'Trino' from the list. Paste 'trino://admin@trino:8080/iceberg' into the SQLAlchemy URI text field, test connection and after that you can connect to the database.

As you can notice, Trino, and thus Superset users, can only access the 'default' namespace which has the dbt models. This is to ensure that the analysts only get the quality data from the downstream.

# About the choice of tools

While a lot of modern data engineering is very cloud heavy, utilizing tools like Databricks, AWS, Azure etc. has it's downsides. One of them is customizability. Many vendors offer 'one size fits all' solutions that make it hard to work with edge cases and smaller, local, ecosystems. However, it's entirely possible to replicate many of the functionalities that cloud environments can offer. The main drawback is often cost of computation, especially when working with large - big data. But that problem can also be solved using smart distributed and parallel computing. 

Iceberg has become a big name in open table format analytics. One of the main reasons Iceberg has become so popular, and why it is used here, is the time travel and snapshot functionality. Every time changes at the REST catalog level are made (such as deleting a table), a new snapshot is created that includes the timestamp, id, etc.. It's possible to query from past snapshots, which makes certain operations much more accessible.

This project uses Trino because it's a great open-source query engine that has a solid Iceberg connector and Superset supports it. 