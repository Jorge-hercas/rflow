.onAttach <- function(libname, pkgname) {
  packageStartupMessage("rflow ", utils::packageVersion("rflow"),
                         " -- a native R workflow orchestrator inspired by Apache Airflow.")
}
