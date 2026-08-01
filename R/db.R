#' Connect to (and if needed, initialize) the rflow metadata database
#'
#' Analogous to Airflow's metadata database. Stores dag_run and task_instance
#' rows so that run history, states, retries, durations, and XCom-like return
#' values survive across R sessions.
#'
#' @param path File path for the SQLite database. Defaults to `rflow.db` in
#'   the current working directory. Use `":memory:"` for an ephemeral DB.
#' @return A `DBIConnection`.
#' @export
rflow_db_connect <- function(path = "rflow.db") {
  con <- DBI::dbConnect(RSQLite::SQLite(), path)
  DBI::dbExecute(con, "PRAGMA foreign_keys = ON;")
  rflow_db_init(con)
  con
}

#' @keywords internal
rflow_db_init <- function(con) {
  DBI::dbExecute(con, "
    CREATE TABLE IF NOT EXISTS dag_run (
      run_id TEXT PRIMARY KEY,
      dag_id TEXT NOT NULL,
      execution_date TEXT NOT NULL,
      state TEXT NOT NULL,
      start_date TEXT,
      end_date TEXT,
      run_type TEXT DEFAULT 'manual',
      conf TEXT
    );
  ")
  DBI::dbExecute(con, "
    CREATE TABLE IF NOT EXISTS task_instance (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      run_id TEXT NOT NULL,
      dag_id TEXT NOT NULL,
      task_id TEXT NOT NULL,
      execution_date TEXT NOT NULL,
      state TEXT NOT NULL,
      try_number INTEGER DEFAULT 0,
      max_tries INTEGER DEFAULT 0,
      start_date TEXT,
      end_date TEXT,
      duration REAL,
      xcom TEXT,
      error TEXT,
      log TEXT,
      UNIQUE(run_id, task_id)
    );
  ")
  DBI::dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_ti_dag ON task_instance(dag_id, task_id);")
  DBI::dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_run_dag ON dag_run(dag_id);")
  invisible(con)
}

#' @keywords internal
.db_escape_conf <- function(conf) {
  tryCatch(jsonlite::toJSON(conf, auto_unbox = TRUE, null = "null"), error = function(e) "{}")
}

#' @keywords internal
db_insert_dag_run <- function(con, run_id, dag_id, execution_date, state = "running",
                               run_type = "manual", conf = list()) {
  DBI::dbExecute(con, "INSERT OR REPLACE INTO dag_run
      (run_id, dag_id, execution_date, state, start_date, end_date, run_type, conf)
      VALUES (?, ?, ?, ?, ?, NULL, ?, ?)",
    params = list(run_id, dag_id, as.character(execution_date), state,
                  as.character(Sys.time()), run_type, .db_escape_conf(conf)))
  invisible(run_id)
}

#' @keywords internal
db_update_dag_run_state <- function(con, run_id, state) {
  end_date <- if (state %in% c("success", "failed")) as.character(Sys.time()) else NA
  DBI::dbExecute(con, "UPDATE dag_run SET state = ?, end_date = COALESCE(?, end_date) WHERE run_id = ?",
                 params = list(state, end_date, run_id))
  invisible(NULL)
}

#' @keywords internal
db_upsert_task_instance <- function(con, run_id, dag_id, task_id, execution_date, state,
                                     try_number = 0, max_tries = 0, start_date = NA, end_date = NA,
                                     duration = NA_real_, xcom = NULL, error = NA_character_, log = NA_character_) {
  xcom_json <- if (is.null(xcom)) NA_character_ else tryCatch(
    jsonlite::toJSON(xcom, auto_unbox = TRUE, null = "null", force = TRUE),
    error = function(e) NA_character_)
  existing <- DBI::dbGetQuery(con, "SELECT id FROM task_instance WHERE run_id = ? AND task_id = ?",
                               params = list(run_id, task_id))
  if (nrow(existing) == 0) {
    DBI::dbExecute(con, "INSERT INTO task_instance
        (run_id, dag_id, task_id, execution_date, state, try_number, max_tries,
         start_date, end_date, duration, xcom, error, log)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
      params = list(run_id, dag_id, task_id, as.character(execution_date), state,
                    try_number, max_tries, as.character(start_date), as.character(end_date),
                    duration, as.character(xcom_json), error, log))
  } else {
    DBI::dbExecute(con, "UPDATE task_instance SET state=?, try_number=?, max_tries=?, start_date=?,
                   end_date=?, duration=?, xcom=?, error=?, log=? WHERE run_id=? AND task_id=?",
      params = list(state, try_number, max_tries, as.character(start_date), as.character(end_date),
                    duration, as.character(xcom_json), error, log, run_id, task_id))
  }
  invisible(NULL)
}

#' Fetch dag_run history from the metadata database
#' @param con connection from [rflow_db_connect()]
#' @param dag_id optional filter
#' @export
rflow_db_runs <- function(con, dag_id = NULL) {
  if (is.null(dag_id)) {
    DBI::dbGetQuery(con, "SELECT * FROM dag_run ORDER BY execution_date DESC")
  } else {
    DBI::dbGetQuery(con, "SELECT * FROM dag_run WHERE dag_id = ? ORDER BY execution_date DESC",
                     params = list(dag_id))
  }
}

#' Fetch task_instance rows for a given run
#' @param con connection from [rflow_db_connect()]
#' @param run_id the dag run id
#' @export
rflow_db_task_instances <- function(con, run_id) {
  DBI::dbGetQuery(con, "SELECT * FROM task_instance WHERE run_id = ? ORDER BY start_date",
                   params = list(run_id))
}
