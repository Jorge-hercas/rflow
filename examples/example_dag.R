# examples/example_dag.R
#
#
#   Rscript examples/example_dag.R
#
library(rflow)

# 1. Define a DAG, just like `with DAG(...) as dag:` in Airflow's Python API.
dag <- with_dag(
  DAG$new(
    dag_id = "sales_report",
    description = "Extract sales data, transform in parallel, and load a report",
    schedule_interval = "@daily",
    start_date = "2026-07-01",
    default_args = list(retries = 2, retry_delay = 3),
    tags = c("example", "sales")
  ),
  {
    extract <- r_task("extract_sales", function(context) {
      cat("Extracting sales data for", context$ds, "\n")
      data.frame(region = c("EMEA", "APAC", "AMER"), revenue = c(120, 90, 150))
    })

    transform_totals <- r_task("compute_totals", function(context) {
      df <- context$ti$extract_sales
      sum(df$revenue)
    })

    transform_top_region <- r_task("compute_top_region", function(context) {
      df <- context$ti$extract_sales
      df$region[which.max(df$revenue)]
    })

    flaky_enrichment <- r_task("enrich_with_forecast", function() {
      # Simulate a task that occasionally fails transiently -- rflow will
      # retry it automatically per default_args (retries = 2).
      if (runif(1) < 0.3) stop("forecast service momentarily unavailable")
      "forecast attached"
    }, retries = 4, retry_delay = 1)

    load_report <- r_task("load_report", function(context) {
      sprintf("Report for %s: total=%d, top_region=%s, note=%s",
              context$ds, context$ti$compute_totals, context$ti$compute_top_region,
              context$ti$enrich_with_forecast)
    })

    notify <- r_task("notify_slack", function(context) {
      cat("[slack] ", context$ti$load_report, "\n")
    }, trigger_rule = "all_done")  # notify whether upstream succeeded or not

    extract %>>% list(transform_totals, transform_top_region)
    list(transform_totals, transform_top_region, flaky_enrichment) %>>% load_report %>>% notify
  }
)

print(dag)

# 2. Run it once (sequential executor), persisting to a local SQLite metadata DB.
con <- rflow_db_connect("rflow.db")
result <- run_dag(dag, con = con)

cat("\nFinal report:\n  ", result$results$load_report, "\n")

# 3. Explore run history from the CLI-style helpers.
list_runs(con, "sales_report")
show_run(con, result$run_id)

# 4. Launch the interactive dashboard (uncomment to try it):
# rflow_ui(dag, con = con)
