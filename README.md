# Control y Seguimiento de Presupuesto Público Perú 2024

Dashboard de análisis del presupuesto institucional vs ejecución de gasto del Estado peruano, construido sobre datos abiertos del Ministerio de Economía y Finanzas (MEF).

---

## Pregunta central

> **¿El Estado peruano está ejecutando eficientemente su presupuesto 2024?**

El dashboard responde esta pregunta desde tres ángulos: resumen ejecutivo nacional, análisis territorial por departamento y análisis sectorial por función de gasto.

---

## Demo

![Resumen Ejecutivo](docs/screenshots/resumen.png)
![Análisis Territorial](docs/screenshots/territorial.png)
![Análisis Sectorial](docs/screenshots/sectorial.png)

---

## Arquitectura del proyecto

```
Fuente de datos (MEF)
        │
        ▼
01_descarga.py          EXTRACT — descarga CSV desde datos abiertos del MEF
        │
        ▼
02_etl.py               TRANSFORM + LOAD — limpieza, modelo estrella, exporta Parquets
        │
        ▼
parquet_final/          6 archivos Parquet listos para Power BI
        │
        ▼
Control de presupuestos.pbix    Dashboard Power BI (3 páginas)
```

### Modelo estrella

```
                    dim_tiempo
                        │
dim_departamento ── fact_ejecucion ── dim_funcion
                        │
              dim_fuente ── dim_tipoactproy
```

| Tabla | Filas | Descripción |
|---|---|---|
| `fact_ejecucion` | ~244,000 | Devengado mensual agregado por claves del modelo |
| `dim_tiempo` | 12 | Año, mes, trimestre con clave surrogate IdTiempo |
| `dim_departamento` | 26 | 25 regiones + Exterior (código 98) |
| `dim_funcion` | 373 | Jerarquía Función → División → Grupo con IdFuncion |
| `dim_fuente` | 9 | Fuente de financiamiento y rubro |
| `dim_tipoactproy` | 2 | Actividad vs Proyecto |

---

## Decisiones de arquitectura

### ¿Por qué Polars en lugar de pandas?
El CSV fuente tiene 2.7 millones de filas y 73 columnas. Polars es entre 5 y 10 veces más rápido que pandas en este volumen y consume menos memoria, lo que permite correr el ETL completo en una máquina con 8 GB de RAM.

### ¿Por qué Parquet en lugar de CSV?
El CSV original pesa ~700 MB. Los Parquets finales pesan 5 MB en total — una compresión del 99%. Power BI carga el modelo en segundos en lugar de minutos.

### ¿Por qué procesar en chunks?
El unpivot de los 12 meses de devengado multiplica las filas por 12 (2.7M → 33M). Con 8 GB de RAM eso genera un segmentation fault si se procesa todo de golpe. El ETL divide el CSV en chunks de 250,000 filas, agrega cada chunk y consolida al final — la memoria máxima usada en cualquier momento es proporcional a 250K filas.

### ¿Por qué IdFuncion como clave surrogate?
El campo `FUNCION` solo identifica el primer nivel de la jerarquía funcional. Como cada función tiene múltiples divisiones y grupos, `FUNCION` no es único en `dim_funcion` — una relación directa generaría muchos-a-muchos en Power BI. `IdFuncion = CodigoFuncion + CodigoDivision + CodigoGrupo` garantiza unicidad desde el origen del ETL, sin reprocesos posteriores.

### ¿Por qué PIM con DISTINCTCOUNT en DAX?
El PIM es un valor anual único que aparece replicado en cada fila mensual del tablón original. Al hacer unpivot, el PIM queda repetido 12 veces por combinación. La medida DAX `DIVIDE(SUM(MONTO_PIM), DISTINCTCOUNT(IdTiempo))` divide por el número real de meses en el contexto de filtro, devolviendo el PIM correcto sin importar si el usuario filtra por mes, trimestre o año completo.

---

## Hallazgos principales

- **Ejecución nacional:** S/ 238,975 millones devengados sobre S/ 262,058 millones de PIM → **91.2%**
- **Patrón temporal:** La ejecución se concentra en el segundo semestre. Q4 acumula el 28.1% del devengado anual.
- **Homogeneidad territorial:** Las 25 regiones superan el 80% de ejecución. La brecha no es territorial sino sectorial.
- **Brecha por tipo:** Actividades ejecutan al 94.7% vs Proyectos al 81.6% — los proyectos de inversión concentran la brecha.
- **Fuente dominante:** Recursos Ordinarios representan S/ 160 mil millones del PIM (61% del total).

---

## Estructura del repositorio

```
📁 /
├── 📄 01_descarga.py           Script de descarga del CSV fuente
├── 📄 02_etl.py                Pipeline ETL completo (Extract → Transform → Load)
├── 📄 README.md                Este archivo
├── 📄 .gitignore               Excluye data cruda y Parquets generados
│
├── 📁 data/
│   └── 📁 raw/                 CSV fuente (no versionado — ver .gitignore)
│       └── .gitkeep
│
└── 📁 output/
    └── 📁 parquet_final/       Parquets generados (no versionados)
        └── .gitkeep
```

---

## Requisitos

```
Python 3.9+
polars
pyarrow
```

Instalar dependencias:

```bash
pip install polars pyarrow
```

---

## Cómo reproducir

**1. Clonar el repositorio**

```bash
git clone https://github.com/tu-usuario/control-presupuesto-peru-2024.git
cd control-presupuesto-peru-2024
```

**2. Editar la variable `RAIZ`**

En `01_descarga.py` y `02_etl.py`, ajusta la variable `RAIZ` a la ruta de tu máquina:

```python
RAIZ = r"C:\tu\ruta\al\proyecto"
```

**3. Descargar el CSV fuente**

```bash
python 01_descarga.py
```

Descarga el archivo `2024-Gasto-Devengado-Mensual.csv` desde el portal de datos abiertos del MEF (~700 MB). Solo se necesita correr una vez.

**4. Ejecutar el ETL**

```bash
python 02_etl.py
```

Genera los 6 archivos Parquet en `output/parquet_final/`. Tiempo estimado: 4-6 minutos dependiendo del hardware.

**5. Conectar Power BI**

Abre `Control de presupuestos.pbix`. Si los Parquets están en una ruta distinta, actualiza las fuentes de datos en Power BI Desktop:

```
Inicio → Transformar datos → Configuración de origen de datos
```

---

## Fuente de datos

| Campo | Detalle |
|---|---|
| Nombre | Presupuesto y Ejecución de Gasto – Devengado Mensual |
| Entidad | Ministerio de Economía y Finanzas del Perú (MEF) |
| Portal | [datosabiertos.mef.gob.pe](https://datosabiertos.mef.gob.pe) |
| Año | 2024 |
| Licencia | Datos abiertos del Estado peruano |
| Diccionario | [Ver diccionario oficial](https://datosabiertos.mef.gob.pe/dataset/presupuesto-y-ejecucion-de-gasto-devengado-mensual/resource/0d38c6bd-8f1b-474d-ade3-32c97260f78a) |

---

## Stack tecnológico

| Herramienta | Uso |
|---|---|
| Python 3.11 | ETL pipeline |
| Polars | Transformación de datos (alternativa a pandas, 5-10x más rápido) |
| PyArrow | Escritura de archivos Parquet |
| Power BI Desktop | Modelado y visualización |
| DAX | Medidas calculadas (PIM, % Ejecución, Brecha, KPIs HTML) |
| Parquet + Snappy | Formato de intercambio ETL → Power BI |

---

## Autor

Manuel Alejandro Sánchez Cárdenas  
[LinkedIn](https://linkedin.com/in/tu-perfil) · [GitHub](https://github.com/tu-usuario)
