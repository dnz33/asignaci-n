# Versión corregida del script:
# - Se comenta time.sleep(1)
# - Se valida que solo se asignen retenes preventivos si el bus fue realmente asignado
# - Se genera hoja adicional con buses no asignados

import pandas as pd
import tkinter as tk
from tkinter import filedialog
import time

# === Selección de archivos ===
root = tk.Tk()
root.withdraw()
restricciones_path = filedialog.askopenfilename(title="Selecciona el archivo de RESTRICCIONES")
servicios_path = filedialog.askopenfilename(title="Selecciona el archivo de servicios")

# === Carga de archivos ===
restricciones = pd.read_excel(restricciones_path)
servicios = pd.read_excel(servicios_path)

# === Normalización de columnas ===
restricciones.columns = restricciones.columns.str.upper().str.strip()
servicios.columns = servicios.columns.str.upper().str.strip()

# === Filtro de buses válidos ===
restricciones = restricciones[
    (restricciones['ESTADO'].str.upper() == 'OPERATIVO') &
    (restricciones['TIPO COMBUSTIBLE'].str.upper() == 'GNV') &
    (restricciones['PATIO'] != 'PATIO EXTERNO') &
    (~restricciones['EMPRESA'].isna())
]

# === Expandir rutas ===
restricciones['RUTA_DISPONIBLE'] = restricciones['RUTA_DISPONIBLE'].fillna('')
restricciones = restricciones.assign(RUTA_EXPANDIDA=restricciones['RUTA_DISPONIBLE'].astype(str).str.split(';')).explode('RUTA_EXPANDIDA')
restricciones['RUTA_EXPANDIDA'] = restricciones['RUTA_EXPANDIDA'].str.strip()

# === Inicializar asignación ===
servicios['BUS_ASIGNADO'] = None
servicios['H ORDEN'] = pd.to_datetime(servicios['H INICIO'], errors='coerce')
servicios = servicios.sort_values(by='H ORDEN')


# === Incorporar lógica por FILA ===
restricciones['FILA'] = restricciones['FILA'].fillna(9999).astype(int)
filas_ordenadas = sorted(restricciones['FILA'].unique())
servicios['BUS_ASIGNADO'] = None  # Reiniciar asignaciones

for fila in filas_ordenadas:
    restricciones_fila = restricciones[restricciones['FILA'] == fila]
    preventivos = restricciones_fila[restricciones_fila['PREVENTIVO'].str.upper() == 'SI'].copy()
    no_preventivos = restricciones_fila[restricciones_fila['PREVENTIVO'].str.upper() != 'SI'].copy()

    # Asignar buses PREVENTIVO
    for idx_serv, servicio in servicios[servicios['BUS_ASIGNADO'].isna()].sort_values(by='H ORDEN').iterrows():
        ruta = str(servicio['RUTA'])
        empresa = servicio['EMPRESA']
        patio_serv = servicio['PATIO']
        candidatos = preventivos[
            (preventivos['EMPRESA'] == empresa) &
            (preventivos['PATIO'] == patio_serv)
        ]
        if patio_serv == "PATIO BERLIN":
            candidatos = candidatos[candidatos['RUTA_EXPANDIDA'] == '201']
        elif patio_serv == "PATIO MUSA":
            pass
        elif patio_serv == "PATIO COGORNO":
            disponibles = no_preventivos[
                (no_preventivos['EMPRESA'] == empresa) &
                (no_preventivos['PATIO'] == patio_serv) &
                (~no_preventivos['ID BUS'].isin(servicios['BUS_ASIGNADO']))
            ]
            if not disponibles.empty:
                continue
            else:
                candidatos = candidatos[candidatos['RUTA_EXPANDIDA'] == '201']

        for idx_bus, bus in candidatos.iterrows():
            if ruta == bus['RUTA_EXPANDIDA'] and pd.isna(servicios.at[idx_serv, 'BUS_ASIGNADO']):
                servicios.at[idx_serv, 'BUS_ASIGNADO'] = bus['ID BUS']
                preventivos = preventivos[preventivos['ID BUS'] != bus['ID BUS']]
                break

    # Asignar buses NO PREVENTIVO (deuda y sin deuda)
    for (empresa, patio), grupo_serv in servicios[servicios['BUS_ASIGNADO'].isna()].groupby(['EMPRESA', 'PATIO']):
        servicios_grupo = grupo_serv.copy()
        buses_disp = no_preventivos[
            (no_preventivos['EMPRESA'] == empresa) &
            (no_preventivos['PATIO'] == patio)
        ].copy()

        if buses_disp.empty:
            continue

        con_deuda = buses_disp[buses_disp['DEUDA'].str.upper() == 'SI'].sort_values(by='KM ACUM TOTAL', ascending=True)
        sin_deuda = buses_disp[buses_disp['DEUDA'].str.upper() != 'SI'].sort_values(by='KM ACUM TOTAL', ascending=True)

        for idx_serv, servicio in servicios_grupo.iterrows():
            if pd.notna(servicios.at[idx_serv, 'BUS_ASIGNADO']):
                continue
            ruta = str(servicio['RUTA'])
            for idx, bus in con_deuda.iterrows():
                if ruta == bus['RUTA_EXPANDIDA']:
                    servicios.at[idx_serv, 'BUS_ASIGNADO'] = bus['ID BUS']
                    con_deuda = con_deuda.drop(idx)
                    break

        for idx_serv, servicio in servicios_grupo.iterrows():
            if pd.notna(servicios.at[idx_serv, 'BUS_ASIGNADO']):
                continue
            ruta = str(servicio['RUTA'])
            for idx, bus in sin_deuda.iterrows():
                if ruta == bus['RUTA_EXPANDIDA']:
                    servicios.at[idx_serv, 'BUS_ASIGNADO'] = bus['ID BUS']
                    sin_deuda = sin_deuda.drop(idx)
                    break

retenes = []
retenes_usados = set()
articulados_excluidos = [567, 568, 569, 724, 725, 726]

buses_asignados = servicios['BUS_ASIGNADO'].dropna().unique()
buses_restantes = restricciones[
    (~restricciones['ID BUS'].isin(buses_asignados)) &
    (~restricciones['ID BUS'].isin(articulados_excluidos)) &
    (restricciones['DEUDA'].str.upper() != 'SI')
].sort_values(by='KM ACUM TOTAL')

for (empresa, patio), grupo in restricciones.groupby(['EMPRESA', 'PATIO']):
    candidatos = buses_restantes[
        (buses_restantes['EMPRESA'] == empresa) &
        (buses_restantes['PATIO'] == patio) &
        (~buses_restantes['ID BUS'].isin(retenes_usados))
    ]
    seleccionados = candidatos.drop_duplicates(subset='ID BUS').head(2)
    for _, bus in seleccionados.iterrows():
        retenes.append({
            'EMPRESA': empresa,
            'PATIO': patio,
            'BUS_RETEN': bus['ID BUS'],
            'KM_ACUMULADO': bus['KM ACUM TOTAL'],
            'TIPO_RETEN': 'GENERAL'
        })
        retenes_usados.add(bus['ID BUS'])

buses_restantes = buses_restantes[~buses_restantes['ID BUS'].isin(retenes_usados)]

preventivos_ext = restricciones[
    ~restricciones['ID BUS'].duplicated() &
    (restricciones['PREVENTIVO'].str.upper() == 'SI') &
    (restricciones['PATIO'] != 'PATIO COGORNO')
]

for _, preventivo in preventivos_ext.drop_duplicates(subset='ID BUS').iterrows():
    cogorno = buses_restantes[
        (buses_restantes['PATIO'] == 'PATIO COGORNO') &
        (~buses_restantes['ID BUS'].isin(retenes_usados))
    ]
    seleccionado = cogorno.drop_duplicates(subset='ID BUS').head(1)
    if not seleccionado.empty and int(preventivo['ID BUS']) in servicios['BUS_ASIGNADO'].values:
        bus = seleccionado.iloc[0]
        retenes.append({
            'EMPRESA': preventivo['EMPRESA'],
            'PATIO': preventivo['PATIO'],
            'BUS_RETEN': bus['ID BUS'],
            'KM_ACUMULADO': bus['KM ACUM TOTAL'],
            'TIPO_RETEN': 'PREVENTIVO'
        })
        retenes_usados.add(bus['ID BUS'])

# === Guardar salida con hoja adicional "NO ASIGNADOS" ===
df_retenes = pd.DataFrame(retenes)
output_path = filedialog.asksaveasfilename(title="Guardar archivo de salida", defaultextension=".xlsx")
with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
    servicios.to_excel(writer, index=False, sheet_name="ASIGNACION")
    df_retenes.to_excel(writer, index=False, sheet_name="RETENES")
    buses_asignados = servicios['BUS_ASIGNADO'].dropna().unique()
    no_asignados = restricciones[~restricciones['ID BUS'].isin(buses_asignados)].drop_duplicates(subset='ID BUS')
    no_asignados.to_excel(writer, index=False, sheet_name="NO ASIGNADOS")
print(f"Archivo generado: {output_path}")