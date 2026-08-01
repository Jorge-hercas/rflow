# inst/shiny/app.R
#
# Standalone launcher for the rflow dashboard. Deploy this the same way you'd
# deploy any Shiny app (shiny::runApp(), shinyapps.io, Shiny Server, Posit
# Connect, etc). It looks for two options, settable via `options()` or
# environment variables before sourcing this file:
#
#   options(rflow.dags_file = "path/to/your_dags.R")  # sourced to register DAGs
#   options(rflow.db_path   = "path/to/rflow.db")      # metadata DB (default "rflow.db")
#
library(rflow)

dags_file <- getOption("rflow.dags_file", Sys.getenv("RFLOW_DAGS_FILE", unset = NA))
db_path <- getOption("rflow.db_path", Sys.getenv("RFLOW_DB_PATH", unset = "rflow.db"))

if (!is.na(dags_file) && nzchar(dags_file) && file.exists(dags_file)) {
  source(dags_file)
} else {
  # Fall back to the bundled example so `runApp()` works out of the box.
  source(system.file("..", "examples", "example_dag.R", package = "rflow"))
}

con <- rflow_db_connect(db_path)
rflow_ui(dags = lapply(list_dags(), get_dag), con = con)
