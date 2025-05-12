import streamlit as st
import pandas as pd
import zipfile
import io
import os
import json
from datetime import datetime
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

st.set_page_config(layout="wide")
st.title("Procesador de archivos MIA")

# Autenticación con Google Drive usando secrets y carpeta específica
FOLDER_ID = "TU_FOLDER_ID_AQUI"  # Reemplaza esto con el ID real de la carpeta de Drive

def autenticar_drive():
    gauth = GoogleAuth()
    credentials_dict = {
        "client_config_backend": "settings",
        "client_config": {
            "client_id": st.secrets["google_drive"]["client_id"],
            "client_secret": st.secrets["google_drive"]["client_secret"],
            "auth_uri": st.secrets["google_drive"]["auth_uri"],
            "token_uri": st.secrets["google_drive"]["token_uri"],
            "auth_provider_x509_cert_url": st.secrets["google_drive"]["auth_provider_x509_cert_url"],
            "redirect_uris": st.secrets["google_drive"]["redirect_uris"]
        }
    }
    gauth.settings = credentials_dict
    gauth.LocalWebserverAuth()
    return GoogleDrive(gauth)

def descargar_ultimo_archivo(drive, nombre_archivo):
    query = f"title='{nombre_archivo}' and trashed=false and '{FOLDER_ID}' in parents"
    file_list = drive.ListFile({'q': query}).GetList()
    if file_list:
        archivo = file_list[0]
        archivo.GetContentFile(nombre_archivo)
        return pd.read_excel(nombre_archivo)
    return None

def subir_archivo(drive, nombre_local, nombre_remoto):
    query = f"title='{nombre_remoto}' and trashed=false and '{FOLDER_ID}' in parents"
    file_list = drive.ListFile({'q': query}).GetList()
    for file in file_list:
        file.Delete()
    file_drive = drive.CreateFile({
        'title': nombre_remoto,
        'parents': [{"id": FOLDER_ID}]
    })
    file_drive.SetContentFile(nombre_local)
    file_drive.Upload()

# Conexión con Google Drive
drive = autenticar_drive()
drive_filename = "DatosCombinados.xlsx"

# Mostrar último archivo si existe
st.write("📂 Cargando archivo desde Google Drive...")
df_combinado = None
try:
    df_combinado = descargar_ultimo_archivo(drive, drive_filename)
    if df_combinado is not None:
        st.success("Archivo DatosCombinados.xlsx cargado desde Google Drive")
        st.dataframe(df_combinado, use_container_width=True)
    else:
        st.info("No se encontró el archivo en Google Drive.")
except Exception as e:
    st.error(f"Error al leer desde Google Drive: {e}")

# Opción para subir nuevo ZIP o generar libros por responsable
tabs = st.tabs(["Actualizar Datos", "Generar Libros por Responsable"])

with tabs[0]:
    uploaded_file = st.file_uploader("Carga tu archivo ZIP con los libros de Excel", type="zip")
    if uploaded_file is not None:
        with zipfile.ZipFile(uploaded_file) as z:
            expected_files = ["ORDENES.xlsx", "INVENTARIO.xlsx", "ESTADO.xlsx", "PRECIOS.xlsx", "GESTION.xlsx"]
            file_dict = {name: z.open(name) for name in expected_files if name in z.namelist()}

            if "ORDENES.xlsx" in file_dict:
                df_ordenes = pd.read_excel(file_dict["ORDENES.xlsx"])
                df_ordenes.columns = [f"{col}_ORDENES" for col in df_ordenes.columns]

                if "LRDTE_ORDENES" in df_ordenes.columns:
                    today = datetime.today()
                    df_ordenes.insert(0, "CONTROL_DIAS", df_ordenes["LRDTE_ORDENES"].apply(lambda x: (datetime.strptime(str(int(x)), "%Y%m%d") - today).days))

                df_combinado = df_ordenes.copy()

                if "INVENTARIO.xlsx" in file_dict:
                    df_inventario = pd.read_excel(file_dict["INVENTARIO.xlsx"])
                    df_inventario.columns = [f"{col}_INVENTARIO" for col in df_inventario.columns]
                    df_inventario_unique = df_inventario.drop_duplicates(subset=["Cod. Producto_INVENTARIO"])
                    df_combinado = pd.merge(df_combinado, df_inventario_unique, left_on="LPROD_ORDENES", right_on="Cod. Producto_INVENTARIO", how="left")

                if "ESTADO.xlsx" in file_dict:
                    df_estado = pd.read_excel(file_dict["ESTADO.xlsx"])
                    df_estado.columns = [f"{col}_ESTADO" for col in df_estado.columns]
                    df_combinado["KEY_ORDENES"] = df_combinado["LORD_ORDENES"].astype(str) + df_combinado["LLINE_ORDENES"].astype(str)
                    df_estado["KEY_ESTADO"] = df_estado["LORD_ESTADO"].astype(str) + df_estado["LLINE_ESTADO"].astype(str)
                    df_estado_unique = df_estado.drop_duplicates(subset=["KEY_ESTADO"])
                    df_combinado = pd.merge(df_combinado, df_estado_unique, left_on="KEY_ORDENES", right_on="KEY_ESTADO", how="left")

                if "PRECIOS.xlsx" in file_dict:
                    df_precios = pd.read_excel(file_dict["PRECIOS.xlsx"])
                    df_precios.columns = [f"{col}_PRECIOS" for col in df_precios.columns]
                    df_precios_unique = df_precios.drop_duplicates(subset=["LPROD_PRECIOS"])
                    for col in ["VALOR_PRECIOS", "On Hand_PRECIOS"]:
                        if col in df_precios_unique.columns:
                            df_precios_unique[col] = pd.to_numeric(df_precios_unique[col], errors='coerce').fillna(0).astype(int)
                    df_combinado = pd.merge(df_combinado, df_precios_unique, left_on="LPROD_ORDENES", right_on="LPROD_PRECIOS", how="left")

                if "GESTION.xlsx" in file_dict:
                    df_gestion = pd.read_excel(file_dict["GESTION.xlsx"])
                    df_gestion.columns = [f"{col}_GESTION" for col in df_gestion.columns]
                    df_gestion_unique = df_gestion.drop_duplicates(subset=["HNAME_GESTION"])
                    df_combinado = pd.merge(df_combinado, df_gestion_unique, left_on="HNAME_ORDENES", right_on="HNAME_GESTION", how="left")

                st.success("Datos combinados generados")
                st.dataframe(df_combinado, use_container_width=True)

                df_combinado.to_excel(drive_filename, index=False)
                subir_archivo(drive, drive_filename, drive_filename)
                st.success("Archivo actualizado en Google Drive")

with tabs[1]:
    if df_combinado is not None and "RESPONSABLE_GESTION" in df_combinado.columns:
        columnas_exportar = [
            "CONTROL_DIAS", "CNME_ORDENES", "HROUT_ORDENES", "HSTAT_ORDENES", "LODTE_ORDENES", "LRDTE_ORDENES",
            "LORD_ORDENES", "HCPO_ORDENES", "LLINE_ORDENES", "LSTAT_ORDENES", "LPROD_ORDENES", "LDESC_ORDENES",
            "LQORD_ORDENES", "LQALL_ORDENES", "LQSHP_ORDENES", "HNAME_ORDENES", "Faltan_ORDENES", "Stock 10_ORDENES",
            "Ubicación_INVENTARIO", "Contenedor_INVENTARIO", "Cantidad_INVENTARIO", "pedido_INVENTARIO",
            "ESTADO_ESTADO", "OBSERVACION_ESTADO", "VALOR_PRECIOS", "On Hand_PRECIOS"
        ]

        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, "w") as zip_file:
            for responsable in df_combinado["RESPONSABLE_GESTION"].dropna().unique():
                df_responsable = df_combinado[df_combinado["RESPONSABLE_GESTION"] == responsable][columnas_exportar].copy()
                df_responsable["VALOR_TOTAL"] = df_responsable.apply(
                    lambda row: row["LQORD_ORDENES"] * row["VALOR_PRECIOS"] if pd.notna(row["LQORD_ORDENES"]) and pd.notna(row["VALOR_PRECIOS"]) else "",
                    axis=1
                )
                temp_buffer = io.BytesIO()
                with pd.ExcelWriter(temp_buffer, engine="xlsxwriter") as writer:
                    df_responsable.to_excel(writer, index=False, sheet_name="Datos")
                temp_buffer.seek(0)
                zip_file.writestr(f"{responsable}.xlsx", temp_buffer.read())
        zip_buffer.seek(0)

        st.download_button(
            label="Descargar todos los libros por Responsable (ZIP)",
            data=zip_buffer,
            file_name="Exportacion_Responsables.zip",
            mime="application/zip"
        )
