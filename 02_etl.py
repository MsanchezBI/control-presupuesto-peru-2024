import polars as pl
import os
from datetime import datetime

# ══════════════════════════════════════════════════════════════
# CONFIG
# ══════════════════════════════════════════════════════════════
RAIZ        = r"C:\Users\Manuel\OneDrive\Desktop\Proyecto BI\Control y seguimiento de presupuestos"
RUTA_CSV    = os.path.join(RAIZ, "data", "raw", "2024-Gasto-Devengado-Mensual.csv")
RUTA_OUTPUT = os.path.join(RAIZ, "output", "parquet_final")

os.makedirs(RUTA_OUTPUT, exist_ok=True)

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

MESES = ["ENERO","FEBRERO","MARZO","ABRIL","MAYO","JUNIO",
         "JULIO","AGOSTO","SEPTIEMBRE","OCTUBRE","NOVIEMBRE","DICIEMBRE"]

COLS_INT = [
    "ANO_EJE","SEC_FUNC","PROGRAMA_PPTO","PRODUCTO_PROYECTO",
    "ACTIVIDAD_ACCION_OBRA","CATEGORIA_GASTO","TIPO_TRANSACCION",
    "GENERICA","SUBGENERICA","SUBGENERICA_DET","ESPECIFICA","ESPECIFICA_DET"
]

COLS_FLOAT = [
    "MONTO_PIA","MONTO_PIM","MONTO_CERTIFICADO_ANUAL",
    "MONTO_COMPROMETIDO_ANUAL","MONTO_DEVENGADO_ANUAL","MONTO_GIRADO_ANUAL",
] + [f"MONTO_DEVENGADO_{m}" for m in MESES]

MAPA_MES = {m: i for i, m in enumerate(MESES, 1)}
COLS_DEVENGADO = [f"MONTO_DEVENGADO_{m}" for m in MESES]

COLS_FK = [
    "ANO_EJE","DEPARTAMENTO_META",
    "FUNCION","DIVISION_FUNCIONAL","GRUPO_FUNCIONAL",
    "FUENTE_FINANCIAMIENTO","RUBRO","TIPO_ACT_PROY",
    "MONTO_PIA","MONTO_PIM",
    "MONTO_COMPROMETIDO_ANUAL","MONTO_GIRADO_ANUAL",
]

# ══════════════════════════════════════════════════════════════
# EXTRACT — leer CSV completo para dimensiones
# ══════════════════════════════════════════════════════════════
log("EXTRACT — leyendo CSV...")

df = pl.read_csv(
    RUTA_CSV,
    encoding     = "latin1",
    infer_schema = False,
    null_values  = ["", " ", "NULL"],
)

df = df.with_columns(
    [pl.col(c).cast(pl.Int64,   strict=False) for c in COLS_INT  ] +
    [pl.col(c).cast(pl.Float64, strict=False) for c in COLS_FLOAT]
)

log(f"CSV cargado: {df.shape[0]:,} filas x {df.shape[1]} columnas")

# ══════════════════════════════════════════════════════════════
# TRANSFORM — dimensiones (rápidas, no consumen memoria)
# ══════════════════════════════════════════════════════════════
log("TRANSFORM — construyendo dimensiones...")

anio = int(df["ANO_EJE"][0])

dim_tiempo = pl.DataFrame({
    "IdTiempo"       : [anio * 100 + i for i in range(1, 13)],
    "Anio"           : [anio] * 12,
    "NumMes"         : list(range(1, 13)),
    "NombreMes"      : [m.capitalize() for m in MESES],
    "NombreMesCorto" : ["Ene","Feb","Mar","Abr","May","Jun",
                        "Jul","Ago","Sep","Oct","Nov","Dic"],
    "NombreMesTablón": MESES,
    "Trimestre"      : [(i - 1) // 3 + 1 for i in range(1, 13)],
    "NombreTrimestre": [f"Q{(i-1)//3+1}" for i in range(1, 13)],
})
log(f"  Dim_Tiempo:       {dim_tiempo.shape[0]:>6} filas")

dim_departamento = (
    df.select(["DEPARTAMENTO_META","DEPARTAMENTO_META_NOMBRE"])
    .unique()
    .rename({
        "DEPARTAMENTO_META"       : "CodigoDepartamento",
        "DEPARTAMENTO_META_NOMBRE": "Departamento",
    })
    .with_columns(
        pl.when(pl.col("Departamento") == "EXTERIOR")
          .then(pl.lit("98 - Exterior (Fuera del país)"))
          .otherwise(pl.col("Departamento").str.to_titlecase())
          .alias("Departamento")
    )
    .sort("CodigoDepartamento")
)
log(f"  Dim_Departamento: {dim_departamento.shape[0]:>6} filas")

dim_funcion = (
    df.select([
        "FUNCION","FUNCION_NOMBRE",
        "DIVISION_FUNCIONAL","DIVISION_FUNCIONAL_NOMBRE",
        "GRUPO_FUNCIONAL","GRUPO_FUNCIONAL_NOMBRE"
    ])
    .unique()
    .rename({
        "FUNCION"                  : "CodigoFuncion",
        "FUNCION_NOMBRE"           : "Funcion",
        "DIVISION_FUNCIONAL"       : "CodigoDivision",
        "DIVISION_FUNCIONAL_NOMBRE": "Division",
        "GRUPO_FUNCIONAL"          : "CodigoGrupo",
        "GRUPO_FUNCIONAL_NOMBRE"   : "Grupo",
    })
    .with_columns([
        (pl.col("CodigoFuncion") + "-" +
         pl.col("CodigoDivision") + "-" +
         pl.col("CodigoGrupo")).alias("IdFuncion"),
        pl.col("Funcion").str.to_titlecase(),
        pl.col("Division").str.to_titlecase(),
        pl.col("Grupo").str.to_titlecase(),
    ])
    .select([
        "IdFuncion","CodigoFuncion","Funcion",
        "CodigoDivision","Division","CodigoGrupo","Grupo"
    ])
    .sort(["CodigoFuncion","CodigoDivision","CodigoGrupo"])
)
log(f"  Dim_Funcion:      {dim_funcion.shape[0]:>6} filas")

dim_fuente = (
    df.select([
        "FUENTE_FINANCIAMIENTO","FUENTE_FINANCIAMIENTO_NOMBRE",
        "RUBRO","RUBRO_NOMBRE"
    ])
    .unique()
    .rename({
        "FUENTE_FINANCIAMIENTO"       : "CodigoFuente",
        "FUENTE_FINANCIAMIENTO_NOMBRE": "Fuente",
        "RUBRO"                       : "CodigoRubro",
        "RUBRO_NOMBRE"                : "Rubro",
    })
    .with_columns([
        pl.col("Fuente").str.to_titlecase(),
        pl.col("Rubro").str.to_titlecase(),
    ])
    .sort(["CodigoFuente","CodigoRubro"])
)
log(f"  Dim_Fuente:       {dim_fuente.shape[0]:>6} filas")

dim_tipo = (
    df.select(["TIPO_ACT_PROY"])
    .unique()
    .rename({"TIPO_ACT_PROY": "CodigoTipo"})
    .with_columns(
        pl.when(pl.col("CodigoTipo") == "2")
          .then(pl.lit("Proyecto"))
          .otherwise(pl.lit("Actividad"))
          .alias("TipoActProy")
    )
    .sort("CodigoTipo")
)
log(f"  Dim_TipoActProy:  {dim_tipo.shape[0]:>6} filas")

# ══════════════════════════════════════════════════════════════
# TRANSFORM — Fact_Ejecucion en chunks para evitar OOM
# Procesa 250,000 filas a la vez, agrega y acumula
# ══════════════════════════════════════════════════════════════
log("TRANSFORM — construyendo Fact_Ejecucion en chunks...")

CHUNK_SIZE   = 250_000
total_filas  = df.shape[0]
n_chunks     = (total_filas // CHUNK_SIZE) + 1
acumulado    = []

for i in range(n_chunks):
    inicio = i * CHUNK_SIZE
    fin    = min(inicio + CHUNK_SIZE, total_filas)
    if inicio >= total_filas:
        break

    chunk = (
        df.slice(inicio, fin - inicio)
        .select(COLS_FK + COLS_DEVENGADO)
        .unpivot(
            on            = COLS_DEVENGADO,
            index         = COLS_FK,
            variable_name = "NombreMesTablón",
            value_name    = "MONTO_DEVENGADO",
        )
        .with_columns(
            pl.col("NombreMesTablón")
              .str.replace("MONTO_DEVENGADO_", "")
              .alias("NombreMesTablón")
        )
        .with_columns([
            (pl.col("ANO_EJE") * 100 +
             pl.col("NombreMesTablón")
               .replace(MAPA_MES)
               .cast(pl.Int64))
            .alias("IdTiempo"),
            (pl.col("FUNCION") + "-" +
             pl.col("DIVISION_FUNCIONAL") + "-" +
             pl.col("GRUPO_FUNCIONAL"))
            .alias("IdFuncion"),
        ])
        .filter(pl.col("MONTO_DEVENGADO").is_not_null())
        .rename({
            "ANO_EJE"              : "Anio",
            "DEPARTAMENTO_META"    : "CodigoDepartamento",
            "FUENTE_FINANCIAMIENTO": "CodigoFuente",
            "RUBRO"                : "CodigoRubro",
            "TIPO_ACT_PROY"        : "CodigoTipo",
        })
        .group_by([
            "IdTiempo","Anio","NombreMesTablón",
            "CodigoDepartamento","IdFuncion",
            "CodigoFuente","CodigoRubro","CodigoTipo",
        ])
        .agg([
            pl.sum("MONTO_PIA").alias("MONTO_PIA"),
            pl.sum("MONTO_PIM").alias("MONTO_PIM"),
            pl.sum("MONTO_COMPROMETIDO_ANUAL").alias("MONTO_COMPROMETIDO"),
            pl.sum("MONTO_GIRADO_ANUAL").alias("MONTO_GIRADO"),
            pl.sum("MONTO_DEVENGADO").alias("MONTO_DEVENGADO"),
        ])
    )

    acumulado.append(chunk)
    log(f"  Chunk {i+1}/{n_chunks} procesado ({inicio:,} → {fin:,})")

# Concatenar todos los chunks y agregar una vez más
# para consolidar filas que quedaron partidas entre chunks
log("  Consolidando chunks...")

fact_ejecucion = (
    pl.concat(acumulado)
    .group_by([
        "IdTiempo","Anio","NombreMesTablón",
        "CodigoDepartamento","IdFuncion",
        "CodigoFuente","CodigoRubro","CodigoTipo",
    ])
    .agg([
        pl.sum("MONTO_PIA").alias("MONTO_PIA"),
        pl.sum("MONTO_PIM").alias("MONTO_PIM"),
        pl.sum("MONTO_COMPROMETIDO").alias("MONTO_COMPROMETIDO"),
        pl.sum("MONTO_GIRADO").alias("MONTO_GIRADO"),
        pl.sum("MONTO_DEVENGADO").alias("MONTO_DEVENGADO"),
    ])
    .sort(["IdTiempo","CodigoDepartamento","IdFuncion"])
)

log(f"  Fact_Ejecucion:   {fact_ejecucion.shape[0]:>10,} filas")

# ══════════════════════════════════════════════════════════════
# LOAD — exportar Parquets finales
# ══════════════════════════════════════════════════════════════
log("LOAD — exportando Parquets finales...")

tablas = {
    "dim_tiempo"      : dim_tiempo,
    "dim_departamento": dim_departamento,
    "dim_funcion"     : dim_funcion,
    "dim_fuente"      : dim_fuente,
    "dim_tipoactproy" : dim_tipo,
    "fact_ejecucion"  : fact_ejecucion,
}

for nombre, tabla in tablas.items():
    ruta = os.path.join(RUTA_OUTPUT, f"{nombre}.parquet")
    tabla.write_parquet(ruta, compression="snappy")
    size_mb = os.path.getsize(ruta) / 1_000_000
    log(f"  ✓ {nombre}.parquet → {size_mb:.1f} MB  ({tabla.shape[0]:,} filas)")

log("ETL completado")
log(f"Scripts: 01_descarga.py → 02_etl.py")
log(f"Output:  {RUTA_OUTPUT}")