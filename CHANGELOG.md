# Changelog

## Lista de cambios

### Bugs corregidos

| # | Archivo(s) | Problema | Corrección |
| --- | --- | --- | --- |
| 1 | `R/utils.R`, `R/executor.R` | El templating `{{ ds }}` no se aplicaba correctamente a `op_args`. | Nueva función `rflow_render_args()` que renderiza recursivamente, considerando también listas anidadas, antes de ejecutar la tarea. |
| 2 | `R/executor.R`, `R/task.R` | `timeout` se guardaba pero **nunca se hacía cumplir**. | Implementado con `setTimeLimit()`. Se documentó que no interrumpe `Sys.sleep()` ni llamadas bloqueantes de sistema o red, una limitación de R base. |
| 3 | `R/scheduler.R` | Un error inesperado (por ejemplo, una base de datos caída) **mataba todo el loop** del scheduler. | `run_dag()` se envolvió en `tryCatch` por DAG y por tick; el loop sigue vivo y registra el error. |
| 4 | `R/scheduler.R` | `catchup` Se referenciaba incorrectamente. | Nueva `.rflow_due_execution_dates()`: con `catchup = TRUE` dispara todos los intervalos perdidos; con `FALSE`, solo el más reciente. |
| 5 | `R/dag.R` | `default_args` de la DAG **sobreescribía siempre** los valores explícitos de una tarea, incluso al pasar `retries = 7`. | Sistema de flags `.explicit_args`: el valor explícito en `r_task()` ahora siempre gana, con semántica similar a Airflow. |
| 6 | `R/ui.R` | Un log `NA` se mostraba como el texto literal `"NA"` en vez de `"(no output)"`. | Los logs ausentes ahora se muestran como `"(no output)"`. |
| 7 | `R/ui.R` | El botón **Trigger run** del dashboard bloqueaba toda la UI mientras corría el DAG. | Ejecución en segundo plano mediante `future`, con fallback síncrono cuando no hay un plan configurado o la base es `:memory:`; un único poller evita filtrar *observers*. |

### Funcionalidad nueva

| # | Archivo(s) | Qué agrega |
| --- | --- | --- |
| 8 | `R/task.R` | `on_success_callback` y `on_failure_callback` en `r_task()`: hooks para notificaciones o alertas, con manejo propio de errores. |
| 9 | `R/db.R` | `rflow_db_connect()` ahora acepta una `DBIConnection` ya abierta (Postgres, MySQL, etc.), no solo una ruta de SQLite. |
| 10 | `R/db.R` | `INSERT OR REPLACE` (específico de SQLite) se reemplazó por `INSERT ... ON CONFLICT ... DO UPDATE`, una sintaxis SQL estándar. |

### Cambios generales en la librería

Cambios detectados al ejecutar `R CMD check --as-cran`.

| # | Archivo(s) | Corrección |
| --- | --- | --- |
| 11 | `DESCRIPTION` | Se eliminó la dependencia `digest`, declarada pero nunca usada. |
| 12 | `R/dag.R` | Se añadieron los `importFrom` faltantes de `setNames` (`stats`) y `modifyList` (`utils`). |
| 13 | `R/executor.R` | Se corrigió el falso positivo de variable global causado por `textConnection("log_capture", ...)`. |
| 14 | `DESCRIPTION` | `License: MIT` ahora incluye `+ file LICENSE`; además, la descripción ya no comienza con el nombre del paquete. |
| 15 | `R/executor.R` | Se documentó el parámetro `run_type` de `run_dag()`. |
| 16 | `.Rbuildignore` | Se añadieron `.github/` y `examples/`, que generaban NOTEs al construir el paquete. |
| 17 | `NEWS.md` | Se creó el archivo y se dejó en un formato parseable. |

### Nuevas pruebas unitarias

- **24 pruebas nuevas**: de 20 a 44.
  - `tests/testthat/test-executor-fixes.R`: templating, timeout, `default_args` y callbacks.
  - `tests/testthat/test-scheduler-fixes.R`: catchup y resiliencia ante errores.
  - `tests/testthat/test-db-fixes.R`: conexión externa y *upsert* portable.
- `.github/workflows/R-CMD-check.yaml`: ejecuta `R CMD check` en Linux, macOS y Windows, con dos versiones de R.
- `.github/workflows/test-coverage.yaml`: mide cobertura con `covr` y la publica en Codecov.
