#' Launch the rflow interactive dashboard (the "webserver")
#'
#' A Shiny control-room dashboard for browsing registered DAGs, inspecting
#' their dependency graph, triggering runs, and reviewing run history and
#' per-task logs -- analogous to the Airflow webserver, entirely in R.
#'
#' @param dags A list of [DAG] objects to expose (defaults to every DAG
#'   registered in the current session via [list_dags()]).
#' @param con A DBI connection from [rflow_db_connect()]. Defaults to a fresh
#'   on-disk `rflow.db` in the working directory.
#' @param ... Passed through to `shiny::runApp()` (e.g. `port =`, `launch.browser =`).
#' @return Does not return; runs the Shiny app (blocking) or, in non-interactive
#'   test contexts, returns the `shiny.appobj` invisibly without launching it.
#' @export
rflow_ui <- function(dags = NULL, con = rflow_db_connect("rflow.db"), ...) {
  if (!requireNamespace("shiny", quietly = TRUE)) {
    stop("rflow: the 'shiny' package is required for rflow_ui(). Install it with install.packages('shiny').")
  }
  if (is.null(dags)) {
    dags <- lapply(list_dags(), get_dag)
  } else if (inherits(dags, "DAG")) {
    dags <- list(dags)
  }
  names(dags) <- vapply(dags, function(d) d$dag_id, character(1))
  if (length(dags) == 0) stop("rflow: no DAGs to display. Define one with with_dag()/DAG$new() first.")

  css <- "
    :root{
      --bg:#0D1117; --panel:#151B23; --panel2:#10151C; --line:#232B36;
      --text:#E6EDF3; --muted:#8B949E; --accent:#5FA8D3;
      --ok:#3FB950; --fail:#F85149; --run:#D29922; --skip:#6E7681;
      --rflow-edge:#3D4552;
    }
    body{background:var(--bg); color:var(--text);
         font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Helvetica,Arial,sans-serif;}
    .rflow-shell{display:grid; grid-template-columns:250px 1fr; min-height:100vh;}
    .rflow-side{background:var(--panel2); border-right:1px solid var(--line); padding:20px 14px;}
    .rflow-brand{font-family:ui-monospace,Menlo,Consolas,monospace; font-size:15px; letter-spacing:.5px;
                 color:var(--text); margin-bottom:2px;}
    .rflow-brand .dot{color:var(--ok);}
    .rflow-tag{font-size:11px; color:var(--muted); margin-bottom:22px; text-transform:uppercase; letter-spacing:1px;}
    .rflow-dag-item{display:block; width:100%; text-align:left; background:transparent; border:1px solid transparent;
                    color:var(--muted); padding:9px 10px; border-radius:6px; margin-bottom:4px; cursor:pointer;
                    font-family:ui-monospace,Menlo,Consolas,monospace; font-size:13px;}
    .rflow-dag-item:hover{background:var(--panel); color:var(--text);}
    .rflow-dag-item.active{background:var(--panel); color:var(--text); border-color:var(--accent);}
    .rflow-main{padding:26px 32px;}
    .rflow-header{display:flex; justify-content:space-between; align-items:baseline; margin-bottom:4px;}
    .rflow-h1{font-size:22px; font-weight:600; margin:0;}
    .rflow-sub{color:var(--muted); font-size:13px; margin-bottom:18px;}
    .rflow-panel{background:var(--panel); border:1px solid var(--line); border-radius:10px; padding:18px; margin-bottom:20px;}
    .rflow-panel h3{margin-top:0; font-size:13px; text-transform:uppercase; letter-spacing:1px; color:var(--muted);}
    .rflow-btn{background:var(--accent); color:#0D1117; border:none; padding:9px 16px; border-radius:6px;
               font-weight:600; cursor:pointer; font-size:13px;}
    .rflow-btn:hover{filter:brightness(1.1);}
    table.rflow-table{width:100%; border-collapse:collapse; font-family:ui-monospace,Menlo,Consolas,monospace; font-size:12.5px;}
    table.rflow-table th{text-align:left; color:var(--muted); font-weight:500; padding:6px 8px; border-bottom:1px solid var(--line);}
    table.rflow-table td{padding:6px 8px; border-bottom:1px solid var(--line);}
    .rflow-pill{display:inline-block; padding:2px 8px; border-radius:20px; font-size:11px; font-weight:600;}
    .rflow-pill.success{background:rgba(63,185,80,.15); color:var(--ok);}
    .rflow-pill.failed{background:rgba(248,81,73,.15); color:var(--fail);}
    .rflow-pill.running{background:rgba(210,153,34,.15); color:var(--run);}
    .rflow-pill.skipped{background:rgba(110,118,129,.15); color:var(--skip);}
    .rflow-pill.pending{background:rgba(110,118,129,.15); color:var(--muted);}
    .rflow-log{background:var(--panel2); border:1px solid var(--line); border-radius:8px; padding:14px;
               font-family:ui-monospace,Menlo,Consolas,monospace; font-size:12px; color:var(--muted);
               white-space:pre-wrap; max-height:320px; overflow-y:auto;}
    .rflow-legend{display:flex; gap:16px; font-size:12px; color:var(--muted); margin-top:8px;}
    .rflow-legend span{display:inline-flex; align-items:center; gap:6px;}
    .rflow-dot{width:9px; height:9px; border-radius:50%;}
  "

  ui <- shiny::fluidPage(
    title = "rflow",
    shiny::tags$head(shiny::tags$style(shiny::HTML(css))),
    shiny::div(class = "rflow-shell",
      shiny::div(class = "rflow-side",
        shiny::div(class = "rflow-brand", shiny::HTML("rflow&nbsp;<span class='dot'>&#9679;</span>")),
        shiny::div(class = "rflow-tag", "R-native orchestrator"),
        shiny::uiOutput("dag_list")
      ),
      shiny::div(class = "rflow-main",
        shiny::div(class = "rflow-header",
          shiny::h1(class = "rflow-h1", shiny::textOutput("dag_title", inline = TRUE)),
          shiny::actionButton("trigger_btn", "Trigger run", class = "rflow-btn")
        ),
        shiny::div(class = "rflow-sub", shiny::textOutput("dag_sub", inline = TRUE)),
        shiny::div(class = "rflow-panel",
          shiny::h3("Dependency graph"),
          shiny::uiOutput("graph_svg"),
          shiny::div(class = "rflow-legend",
            shiny::tags$span(shiny::div(class = "rflow-dot", style = "background:#3FB950"), "success"),
            shiny::tags$span(shiny::div(class = "rflow-dot", style = "background:#F85149"), "failed"),
            shiny::tags$span(shiny::div(class = "rflow-dot", style = "background:#D29922"), "running"),
            shiny::tags$span(shiny::div(class = "rflow-dot", style = "background:#6E7681"), "skipped"),
            shiny::tags$span(shiny::div(class = "rflow-dot", style = "background:#30363D"), "pending")
          )
        ),
        shiny::div(class = "rflow-panel",
          shiny::h3("Run history"),
          shiny::uiOutput("runs_table")
        ),
        shiny::div(class = "rflow-panel",
          shiny::h3("Task instances & logs (most recent run)"),
          shiny::uiOutput("ti_table"),
          shiny::uiOutput("log_view")
        )
      )
    )
  )

  server <- function(input, output, session) {
    selected_dag_id <- shiny::reactiveVal(names(dags)[1])
    refresh_tick <- shiny::reactiveVal(0)

    lapply(names(dags), function(id) {
      shiny::observeEvent(input[[paste0("select_", id)]], selected_dag_id(id))
    })

    output$dag_list <- shiny::renderUI({
      cur <- selected_dag_id()
      shiny::tagList(lapply(names(dags), function(id) {
        cls <- if (identical(id, cur)) "rflow-dag-item active" else "rflow-dag-item"
        shiny::actionButton(paste0("select_", id), id, class = cls)
      }))
    })

    current_dag <- shiny::reactive(dags[[selected_dag_id()]])

    output$dag_title <- shiny::renderText(current_dag()$dag_id)
    output$dag_sub <- shiny::renderText({
      d <- current_dag()
      sprintf("%s  |  schedule: %s  |  %d tasks",
              d$description %||% "(no description)", d$schedule_interval %||% "manual", length(d$task_ids()))
    })

    latest_states <- shiny::reactive({
      refresh_tick()
      runs <- rflow_db_runs(con, selected_dag_id())
      if (nrow(runs) == 0) return(NULL)
      latest_run <- runs$run_id[1]
      ti <- rflow_db_task_instances(con, latest_run)
      list(run_id = latest_run, states = setNames(ti$state, ti$task_id), ti = ti)
    })

    output$graph_svg <- shiny::renderUI({
      st <- latest_states()
      states_vec <- if (is.null(st)) NULL else st$states
      shiny::HTML(render_dag_svg(current_dag(), states = states_vec))
    })

    output$runs_table <- shiny::renderUI({
      refresh_tick()
      runs <- rflow_db_runs(con, selected_dag_id())
      if (nrow(runs) == 0) return(shiny::tags$div(class = "rflow-sub", "No runs yet -- click 'Trigger run' to start one."))
      rows <- apply(runs, 1, function(r) {
        shiny::tags$tr(
          shiny::tags$td(r[["run_id"]]),
          shiny::tags$td(r[["execution_date"]]),
          shiny::tags$td(shiny::tags$span(class = paste("rflow-pill", r[["state"]]), r[["state"]])),
          shiny::tags$td(r[["run_type"]])
        )
      })
      shiny::tags$table(class = "rflow-table",
        shiny::tags$thead(shiny::tags$tr(shiny::tags$th("run_id"), shiny::tags$th("execution_date"),
                                          shiny::tags$th("state"), shiny::tags$th("type"))),
        shiny::tags$tbody(rows)
      )
    })

    output$ti_table <- shiny::renderUI({
      st <- latest_states()
      if (is.null(st)) return(shiny::tags$div(class = "rflow-sub", "(no task instances yet)"))
      rows <- apply(st$ti, 1, function(r) {
        shiny::tags$tr(
          shiny::tags$td(r[["task_id"]]),
          shiny::tags$td(shiny::tags$span(class = paste("rflow-pill", r[["state"]]), r[["state"]])),
          shiny::tags$td(sprintf("%s/%s", r[["try_number"]], r[["max_tries"]])),
          shiny::tags$td(sprintf("%.3fs", as.numeric(r[["duration"]]))),
          shiny::tags$td(if (!is.na(r[["error"]])) r[["error"]] else "-")
        )
      })
      shiny::tags$table(class = "rflow-table",
        shiny::tags$thead(shiny::tags$tr(shiny::tags$th("task_id"), shiny::tags$th("state"),
                                          shiny::tags$th("try"), shiny::tags$th("duration"), shiny::tags$th("error"))),
        shiny::tags$tbody(rows)
      )
    })

    output$log_view <- shiny::renderUI({
      st <- latest_states()
      if (is.null(st)) return(NULL)
      logs <- DBI::dbGetQuery(con, "SELECT task_id, log FROM task_instance WHERE run_id = ?", params = list(st$run_id))
      shiny::tagList(lapply(seq_len(nrow(logs)), function(i) {
        shiny::tags$details(
          shiny::tags$summary(logs$task_id[i]),
          shiny::tags$div(class = "rflow-log", logs$log[i] %||% "(no output)")
        )
      }))
    })

    shiny::observeEvent(input$trigger_btn, {
      d <- current_dag()
      shiny::showNotification(sprintf("Triggering '%s'...", d$dag_id), type = "message", duration = 2)
      tryCatch({
        run_dag(d, con = con, verbose = FALSE)
      }, error = function(e) {
        shiny::showNotification(sprintf("Run failed to launch: %s", conditionMessage(e)), type = "error")
      })
      refresh_tick(refresh_tick() + 1)
    })
  }

  shiny::shinyApp(ui, server, options = list(...))
}
