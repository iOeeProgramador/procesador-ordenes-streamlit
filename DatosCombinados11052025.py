import os
import sqlite3

def guardar_en_sqlite(df):
    conn = sqlite3.connect(DB_PATH)
    df.to_sql("datos_combinados", conn, if_exists="replace", index=False)
    conn.close()

def leer_desde_sqlite():
    if os.path.exists(DB_PATH):
        conn = sqlite3.connect(DB_PATH)
        df = pd.read_sql("SELECT * FROM datos_combinados", conn)
        conn.close()
        return df
    return None
DB_PATH = "datos_combinados.db"

if os.path.exists(DB_PATH):
    st.caption("Base de datos SQLite detectada: se omitirá el cargador ZIP hasta usar 'Actualizar Datos'.")

df_combinado = leer_desde_sqlite()
if df_combinado is not None:
    st.success("Datos cargados desde base local SQLite")
    st.dataframe(df_combinado, use_container_width=True)
else:
    st.info("No se encontraron datos previos guardados.")

# Tabs para cargar ZIP o exportar por responsable
tabs = st.tabs(["Actualizar Datos", "Generar Libros por Responsable"])

with tabs[0]:
    mostrar_uploader = st.button("Actualizar Datos desde ZIP")
    uploaded_file = st.file_uploader("Carga tu archivo ZIP con los libros de Excel", type="zip") if mostrar_uploader else None
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

                guardar_en_sqlite(df_combinado)
                st.success("Datos guardados localmente en base SQLite")

with tabs[1]:
    if os.path.exists(DB_PATH):
        with open(DB_PATH, "rb") as f:
            st.download_button("📥 Descargar base SQLite actual (.db)", f, file_name="datos_combinados.db")
    if df_combinado is not None and "RESPONSABLE_GESTION" in df_combinado.columns:
        columnas_exportar = [
            "CONTROL_DIAS", "CNME_ORDENES", "HROUT_ORDENES", "HSTAT_ORDENES", "LODTE_ORDENES", "LRDTE_ORDENES",
            "LORD_ORDENES", "HCPO_ORDENES", "LLINE_ORDENES", "LSTAT_ORDENES", "LPROD_ORDENES", "LDESC_ORDENES",
            "LQORD_ORDENES", "LQALL_ORDENES", "LQSHP_ORDENES", "HNAME_ORDENES", "Faltan_ORDENES", "Stock 10_ORDENES",
            "Ubicación_INVENTARIO", "Contenedor_INVENTARIO", "Cantidad_INVENTARIO", "pedido_INVENTARIO",
            "ESTADO_ESTADO", "OBSERVACION_ESTADO", "VALOR_PRECIOS", "On Hand_PRECIOS"
        ]

        zip_buffer = io.BytesIO()
        fecha_actual = datetime.today().strftime("%Y%m%d")
        zip_filename = f"Exportacion_Responsables_{fecha_actual}.zip"

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
            file_name=zip_filename,
            mime="application/zip"
        )
