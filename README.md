# MYMail_review_load

Repositorio con scripts en Python para analizar/exportar ficheros de validacion y trazas (IA/RPA/orquestador) y generar Excels de apoyo.

## Estructura del repo

- `data/`: ficheros de entrada y salidas por fecha (`data/<fecha>/...`).
- `scriptsOld/`: scripts Python.
- `requirements.txt`: dependencias (incluye `openpyxl` para Excel).

## Ficheros de entrada

Los scripts trabajan con una carpeta `data/<fecha>/` que debe contener (segun el caso):

- `data/<fecha>/validaciones.csv`: validaciones (usa `IdCorreo`, `Validado`, `Automatismo`, `@timestamp`, etc.).
- `data/<fecha>/fichas_levantadas.csv`: fichas levantadas (usa `IdCorreo`, `Automatismo`).
- `data/<fecha>/orquestador_contexto.csv`: ejecuciones del orquestador en modo contexto (usa `IDU`, `@timestamp`, `MatriculaAsesor`).
- `data/<fecha>/ia-transacciones.csv`: detalle de IA/transacciones (separador `;`, usa `idLotus`, `Location`, `Sublocation`, `Subject`, `Question`, `MailToAgent`).
- `data/<fecha>/ia.csv`: IA "simple" (separador `;`, usa `idLotus`, `Location`, `Sublocation`) - necesario para el analisis `laura`.
- `data/<fecha>/rpa.csv`: trazas RPA (usa `IDU`, `@timestamp`, etc.) - necesario para el analisis `laura`.

Nota: `ia-transacciones.csv` es grande; puede tardar y consumir memoria al cargarlo con `pandas`.

## Preparacion (importante)

Los scripts leen rutas relativas tipo `data/<fecha>/...`, asi que hay que poner los CSV en esa estructura.

1) Crear entorno e instalar dependencias:

```powershell
cd <ruta-del-repo>
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

2) Preparar carpeta de trabajo `data/<fecha>/`.

El valor `fecha` es literalmente una subruta dentro de `data/` (puede ser `4enero`, `2026-01-04`, `diciembre/8_diciembre`, etc.).

Ejemplo con `fecha = 2026-01-04`:

```powershell
mkdir data\2026-01-04
Copy-Item <origen>\validaciones.csv data\2026-01-04\
Copy-Item <origen>\fichas_levantadas.csv data\2026-01-04\
Copy-Item <origen>\orquestador_contexto.csv data\2026-01-04\
Copy-Item <origen>\ia-transacciones.csv data\2026-01-04\
```

Si ademas vas a usar el analisis `laura`, copia tambien:

```powershell
Copy-Item <origen>\ia.csv data\2026-01-04\
Copy-Item <origen>\rpa.csv data\2026-01-04\
```

Nota: ejecuta siempre desde la raiz del repo para que `data/...` resuelva bien:
`cd <ruta-del-repo>; python script_semanal.py`

## Scripts

### `script_semanal.py`

Script principal (todo en un solo fichero): genera Excels semanales y, opcionalmente, el analisis de Laura y el muestreo de revision de NO VALIDADOS.

Ejecuta (por defecto hace el modo `semanal`):

```powershell
python script_semanal.py
```

Parametros utiles:

- `--fecha 4enero`: subcarpeta dentro de `data/`.
- `--no-laura`: no ejecuta el analisis de Laura.
- `--no-revision`: no genera el muestreo `validaciones_revision.xlsx`.
- `--revision-size 150`: tamano del muestreo (por defecto 150).
- `--revision-min-full 5`: si una combinacion (`Automatismo`, `Segmento`) tiene <= 5 casos NO VALIDADOS, se incluyen todos para evitar sesgo.
- `--no-stats`: no genera el fichero `Stats_<fecha>.xlsx`.
- `--stats-template <ruta>`: plantilla base (por defecto `Plantilla_Stats.xlsx`).
- `--stats-output <ruta|nombre>`: nombre/ruta del stats (por defecto `data/<fecha>/output/Stats_<fecha>.xlsx`).

Outputs en `data/<fecha>/output/`:

- `validaciones.xlsx`
- `fichas_levantadas.xlsx`
- `ejecuciones.xlsx`
- `validaciones_revision.xlsx`
- `mails_por_subtematica.xlsx` (si estan `ia.csv` y `rpa.csv`)
- `Stats_<fecha>.xlsx` (basado en `Plantilla_Stats.xlsx`)

#### Stats `Stats_<fecha>.xlsx`

Se construye copiando `Plantilla_Stats.xlsx` y anadiendo hojas con la informacion de los Excels generados:

- `Resumen_General` (conteos principales)
- `Validaciones_Pivot`, `Fichas_Pivot`, `Ejecuciones_Pivot`
- `Laura_*` (si existe `mails_por_subtematica.xlsx`)
- `Revision_*` y `Revision_Pesos`

Nota: si `Plantilla_Stats.xlsx` esta abierto en Excel, el script no podra copiarlo y mostrara un `[WARN]`.

Subcomandos:

- `python script_semanal.py semanal --fecha 4enero`
- `python script_semanal.py stats --fecha 4enero`
- `python script_semanal.py laura --folder data/4enero --segmento-csv data/CIF-Segmento_3.csv --output mails.xlsx`

#### Excels generados

- `validaciones.xlsx`:
  - `Data`: validaciones enriquecidas con `ia-transacciones.csv`.
  - `Pivot`: filas `Automatismo`, columnas `Validado | Segmento`, valores `count(IdCorreo)`.
  - La cabecera de `Data` se colorea: azul para `idLotus/Location/Sublocation/Subject/Question/MailToAgent/Faltan datos?` y verde para el resto.
- `fichas_levantadas.xlsx`:
  - `Data`: datos deduplicados.
  - `Pivot`: filas `Automatismo`, columnas `Segmento`, valores `count(IdCorreo)`, con `(blank)` y `Total`.
- `ejecuciones.xlsx`:
  - `Data`: cruce de VALIDADO vs orquestador_contexto.
  - `Pivot`: filas `Automatismo`, columnas `Encontrados | Segmento`, valores `count(IdCorreo Validacion)`, con `(blank)` y `Total`.

#### Muestreo `validaciones_revision.xlsx` (NO VALIDADOS)

Se genera a partir de `validaciones.xlsx` (hoja `Data`) filtrando `Validado != "VALIDADO"` y estratificando por (`Automatismo`, `Segmento`).

1) Pesos (ponderaciones)

- Se leen de `validaciones_revision_pesos.json` (misma carpeta que `script_semanal.py`; si no existe, se crea con 1.0).
- Peso final por estrato = `default * automatismo[Automatismo] * segmento[Segmento] * pair["Automatismo||Segmento"]`.

2) Regla anti-sesgo para pocos casos

- Si un estrato tiene pocos casos (<= `--revision-min-full`) y el muestreo lo permite, se incluyen todos esos correos.

3) Asignacion de cupos

- Con el resto de estratos, se asignan cupos proporcionales a `N * peso` (redondeo y ajuste para que la suma sea el tamano objetivo).
- El muestreo es determinista por estrato (semilla estable por `fecha/Automatismo/Segmento`) para que sea reproducible.

4) Salida

- `Data`: filas seleccionadas para revision.
- `Resumen`: poblacion NO VALIDADO, peso y muestra por (`Automatismo`, `Segmento`) + total.

### `scriptsOld/tmos.py` y `scriptsOld/funciones_tmos.py`

Calculo de TMO (tiempo) a partir de trazas RPA.

- `scriptsOld/tmos.py` define un `mes` y busca ficheros en `data/TMOs/<mes>/...` (no estan en este repo).
- `scriptsOld/funciones_tmos.py` implementa el calculo y exporta Excels por tematica.
