#' @keywords internal
.state_color <- function(state) {
  switch(state %||% "pending",
    success = "#3FB950",
    failed  = "#F85149",
    running = "#D29922",
    skipped = "#6E7681",
    pending = "#30363D",
    "#30363D"
  )
}

#' Render a DAG's dependency graph as a self-contained SVG string
#'
#' Lays tasks out left-to-right in topological "layers" (stages), connecting
#' each task to its downstream tasks with elbow connectors — a transit-map
#' style rendering. If `states` is supplied (a named character vector,
#' task_id -> state), nodes are colored by run state; otherwise all nodes are
#' drawn in a neutral "defined" color.
#'
#' @param dag A [DAG] object.
#' @param states Optional named character vector: task_id -> one of
#'   "success"/"failed"/"running"/"skipped"/"pending".
#' @param node_w,node_h Node box dimensions in px.
#' @param col_gap,row_gap Spacing between layers/rows in px.
#' @return A character string of raw SVG markup.
#' @export
render_dag_svg <- function(dag, states = NULL, node_w = 148, node_h = 40, col_gap = 220, row_gap = 64) {
  topo <- dag$topo()
  layers <- topo$layers
  n_layers <- length(layers)
  max_rows <- max(vapply(layers, length, integer(1)))

  width <- n_layers * col_gap + node_w + 40
  height <- max_rows * row_gap + 60

  pos <- list()
  for (li in seq_along(layers)) {
    ids <- layers[[li]]
    n <- length(ids)
    y0 <- (height - (n * row_gap)) / 2
    for (ri in seq_along(ids)) {
      pos[[ids[ri]]] <- list(x = 20 + (li - 1) * col_gap, y = y0 + (ri - 1) * row_gap + row_gap / 2)
    }
  }

  edges <- character(0)
  for (id in dag$task_ids()) {
    t <- dag$get_task(id)
    p_from <- pos[[id]]
    for (d in t$downstream_task_ids) {
      p_to <- pos[[d]]
      x1 <- p_from$x + node_w; y1 <- p_from$y
      x2 <- p_to$x; y2 <- p_to$y
      mx <- (x1 + x2) / 2
      path <- sprintf("M %.1f %.1f C %.1f %.1f, %.1f %.1f, %.1f %.1f",
                       x1, y1, mx, y1, mx, y2, x2, y2)
      edges <- c(edges, sprintf(
        '<path d="%s" stroke="var(--rflow-edge, #3D4552)" stroke-width="2" fill="none" marker-end="url(#arrow)"/>',
        path))
    }
  }

  nodes <- character(0)
  for (id in names(pos)) {
    p <- pos[[id]]
    state <- if (is.null(states)) "pending" else (states[[id]] %||% "pending")
    color <- .state_color(state)
    label <- if (nchar(id) > 18) paste0(substr(id, 1, 16), "...") else id
    nodes <- c(nodes, sprintf(paste0(
      '<g class="rflow-node">',
      '<rect x="%.1f" y="%.1f" width="%d" height="%d" rx="8" ',
      'fill="#151B23" stroke="%s" stroke-width="2.5"/>',
      '<circle cx="%.1f" cy="%.1f" r="5" fill="%s"/>',
      '<text x="%.1f" y="%.1f" fill="#E6EDF3" font-family="ui-monospace,Menlo,Consolas,monospace" ',
      'font-size="13">%s</text>',
      '</g>'),
      p$x, p$y - node_h / 2, node_w, node_h, color,
      p$x + 18, p$y, color,
      p$x + 34, p$y + 4.5, label))
  }

  sprintf(paste0(
    '<svg viewBox="0 0 %d %d" xmlns="http://www.w3.org/2000/svg" width="100%%" height="%d">',
    '<defs><marker id="arrow" markerWidth="8" markerHeight="8" refX="7" refY="3" orient="auto">',
    '<path d="M0,0 L0,6 L7,3 z" fill="#3D4552"/></marker></defs>',
    '%s%s</svg>'),
    width, height, height,
    paste(edges, collapse = ""), paste(nodes, collapse = ""))
}
