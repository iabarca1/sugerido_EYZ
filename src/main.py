import streamlit as st
import pyodbc
import time
from procesamiento import extraer_datos
from formato_datos import transformacion_datos
from alertas import alerta_estado
from alertas import alerta_compra
from descarga import archivo_excel

def set_layout():
    st.set_page_config(page_title="Stock Assistant", layout="wide")

def apply_custom_styles():
    st.markdown("""
        <style>
        .stApp {
            background-color: white;
        }
        .stAlert {
            background-color: black;
            color: white;
        }
        /* Estilo para el botón de ancho completo */
        div.stButton > button:first-child {
            width: 100%;
        }
        /* Centrar el botón de descarga específicamente */
        .centered-download-btn {
            width: 100%;
        }
        </style>
    """, unsafe_allow_html=True)

def display_header():
    st.markdown("""
        <h1 style='text-align: center; color: black; background-color: white;'>
            Hi! I am Piero
        </h1>
        <h2 style='text-align: center; color: gray; background-color: white;'>
            Your personal stock assistant
        </h2>
    """, unsafe_allow_html=True)

@st.cache_resource
def init_connection():
    return pyodbc.connect(
        "DRIVER={ODBC Driver 17 for SQL Server};SERVER="
        + st.secrets["server"]
        + ";DATABASE="
        + st.secrets["database"]
        + ";UID="
        + st.secrets["username"]
        + ";PWD="
        + st.secrets["password"]
    )

def main():
    set_layout()
    apply_custom_styles()
    display_header()

    if st.button("💡 Obtén tu Sugerido de Compra"):
        success_message = st.empty()
        info_message = st.empty()
        try:
            conn = init_connection()
            success_message.success("Conexión exitosa")
            time.sleep(1)
            success_message.empty()
            info_message.info('Cargando datos...')
            with st.spinner('Por favor espera...'):
                base = extraer_datos(conn)
                df = transformacion_datos(base)
            info_message.empty()
            st.dataframe(df)
            texto_alerta_estado = alerta_estado(base)
            st.warning(texto_alerta_estado)
            texto_alerta_compra = alerta_compra(base)
            st.warning(texto_alerta_compra)

            # Mostrar el mensaje de carga antes del botón de descarga
            carga_info = st.empty()
            time.sleep(1)
            carga_info.info('Preparando datos para descarga...')
            with st.spinner('Por favor espera...'):
                base = extraer_datos(conn)
            # Descargar botón
            st.download_button(
                label="📥 Descargar Sugerido de Compra en Excel",
                data=archivo_excel(base),
                file_name='sugerido_compra.xlsx',
                mime='application/vnd.ms-excel'
            )
            # Limpiar el mensaje de carga después de que el botón de descarga esté listo
            carga_info.empty()
        except Exception as e:
            success_message.empty()
            info_message.empty()
            st.error(f"Error: {e}")

if __name__ == "__main__":
    main()