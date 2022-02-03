# customer-churn-example

Welcome to Continual's customer churn example. For a full walk through, please visit the [documentation](https://docs.continual.ai/customer-churn). You can also find some great context at this [blog post](https://continual.ai/post/use-case-deep-dive-customer-churn) as well.

Note: This project is designed to run with Snowflake. You should be able to adapt it to other warehouse vendors pretty easily, but [let us know](https://docs.continual.ai/help-support) if you have any issues.

## Running the example

Download the source data at Kaggle:

```sh
kaggle competitions download -c kkbox-churn-prediction-challenge
```

You can then upload it to your cloud data warehouse of choice in the manner of choice. For convenience, we've included a few short scripts to upload this manually in snowflake.

1. First run the [ddl.sql](https://github.com/continual-ai/customer-churn-example/blob/main/sql/ddl.sql) file

2. Then you can upload the CSVs downloaded from kaggle via `snowsql`. Copy the commands in [snowsql_staging.sql](https://github.com/continual-ai/customer-churn-example/blob/main/sql/snowsql_staging.sql) into `snowsql` and execute them.


## For dbt users

 If you're using dbt, you'll now just be able to use the [dbt project](https://github.com/continual-ai/customer-churn-example/blob/main/dbt) provided. `dbt_project.yml` is configured to use the `continual` profile. You'll either need to change it or create your own in your `profiles.yml` file. Then you can execute:

 ```sh
 dbt run
 ```

To build all the required tables/views.

When dbt is finished, you can then run

```sh
continual run ./continual/featuresets ./continual/models
```

You'll of course need to ensure that you have an account in continual and have logged in with the CLI and have set up a default project.

You're now done! You can check out the Continual Web UI to monitor the model and see results as it finishes. (Note: It should take about 2 hrs to finish.)

## For non-dbt users.

We highly recommend using dbt for your transformations. If this is not feasible, we've provided to sql scripts [feature_engineering.sql](https://github.com/continual-ai/customer-churn-example/blob/main/sql/feature_engineering.sql) and [prediction_engineering.sql](https://github.com/continual-ai/customer-churn-example/blob/main/sql/prediction_engineering.sql) that you can run in Snowflake to build out all required tables/views.

After that, you can simply execute the following:

```sh
continual push ./continual/featuresets ./continual/models
```

You'll of course need to ensure that you have an account in continual and have logged in with the CLI and have set up a default project. Note: If you modify any of the table names in the .sql scripts, you'll want to update the queries in the `.yaml` in `continual/featuresets` and `continual/models` accordingly.

You're now done! You can check out the Continual Web UI to monitor the model and see results as it finishes. (Note: It should take about 2 hrs to finish.)

## Having issues?
Feel free to [let us know](https://docs.continual.ai/help-support) if you have any issues, or you can open a PR with any suggested modifications.

Thanks!
