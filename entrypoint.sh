#!/bin/bash
set -e

target=${TARGET_ENV:-dev}

case "$1" in
  "seed")
    dbt seed --profile pharma_project --profiles-dir /home/dbtuser/.dbt --target $target
    ;;
  "snapshot")
    dbt snapshot --profile pharma_project --profiles-dir /home/dbtuser/.dbt --target $target
    ;;
  "run")
    dbt run --profile pharma_project --profiles-dir /home/dbtuser/.dbt --target $target ${@:2}
    ;;
  "test")
    dbt test --profile pharma_project --profiles-dir /home/dbtuser/.dbt --target $target
    ;;
  *)
    exec "$@"
    ;;
esac