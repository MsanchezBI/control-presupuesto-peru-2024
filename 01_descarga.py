import urllib.request
import os
from datetime import datetime

# ── CONFIG — ajusta solo esta línea si cambias de máquina ───
RAIZ     = r"C:\Users\Manuel\OneDrive\Desktop\Proyecto BI\Control y seguimiento de presupuestos"
URL      = "https://fs.datosabiertos.mef.gob.pe/datastorefiles/2024-Gasto-Devengado-Mensual.csv"
ARCHIVO  = "2024-Gasto-Devengado-Mensual.csv"

# ── Construir rutas desde la raíz ───────────────────────────
RUTA_RAW = os.path.join(RAIZ, "data", "raw")
DESTINO  = os.path.join(RUTA_RAW, ARCHIVO)

# ── Crear carpeta si no existe ───────────────────────────────
os.makedirs(RUTA_RAW, exist_ok=True)

# ── Descarga con barra de progreso ──────────────────────────
print(f"[{datetime.now().strftime('%H:%M:%S')}] Iniciando descarga...")
print(f"Destino: {DESTINO}\n")

def progreso(count, block_size, total_size):
    if total_size > 0:
        pct = count * block_size * 100 / total_size
        mb  = count * block_size / 1_000_000
        print(f"\r  Descargado: {mb:.1f} MB  ({min(pct, 100):.1f}%)", end="")

urllib.request.urlretrieve(URL, DESTINO, reporthook=progreso)

print(f"\n\n[{datetime.now().strftime('%H:%M:%S')}] ✓ Descarga completa")
print(f"  Guardado en : {DESTINO}")
print(f"  Tamaño      : {os.path.getsize(DESTINO) / 1_000_000:.1f} MB")