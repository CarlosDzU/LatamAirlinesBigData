#!/usr/bin/env bash
# Exporta las tablas de consumo de gold a ./powerbi_data/ (Parquet para Power BI y CSV para Looker Studio).
# Uso: scripts/exportar_tablero.sh            (desde S3, requiere LAKE_BUCKET)
#      LATAM_MODO=local scripts/exportar_tablero.sh   (desde data/lake)
set -euo pipefail
RAIZ="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DESTINO="$RAIZ/powerbi_data"
TABLAS="fact_vuelo dim_fecha dim_aeropuerto dim_aerolinea dim_condicion_clima dim_ruta kpi_diario kpi_alerta_mensual"
mkdir -p "$DESTINO"
if [[ "${LATAM_MODO:-aws}" == "aws" ]]; then
  for t in $TABLAS; do
    aws s3 sync "s3://${LAKE_BUCKET:?defina LAKE_BUCKET}/gold/$t/" "$DESTINO/$t/" --only-show-errors --exclude '_*'
  done
  ORIGEN="$DESTINO"
else
  ORIGEN="${LATAM_LAKE_LOCAL:-$RAIZ/data/lake}/gold"
fi
PYTHON="${PYTHON:-python3}"
# Si el Python local no tiene pandas/pyarrow, la conversión a CSV se hace en la imagen Docker del proyecto.
if ! "$PYTHON" -c "import pandas, pyarrow" 2>/dev/null && command -v docker >/dev/null 2>&1; then
  exec docker run --rm -u "$(id -u):$(id -g)" -v "$RAIZ":/app -v "$ORIGEN":/tmp/lk/gold -w /app \
    -e HOME=/tmp -e LATAM_MODO=local -e LATAM_LAKE_LOCAL=/tmp/lk latam-ops-tests:1.0 bash scripts/exportar_tablero.sh
fi
"$PYTHON" - "$ORIGEN" "$DESTINO" $TABLAS <<'PY'
import sys
from pathlib import Path

import pandas as pd

origen, destino, tablas = Path(sys.argv[1]), Path(sys.argv[2]), sys.argv[3:]
for t in tablas:
    ruta = origen / t
    if not ruta.exists():
        print(f"[exportar] sin datos: {t}")
        continue
    df = pd.read_parquet(ruta)
    (destino / "csv").mkdir(parents=True, exist_ok=True)
    df.to_csv(destino / "csv" / f"{t}.csv", index=False)
    print(f"[exportar] {t}: {len(df)} filas -> csv/{t}.csv")
PY
