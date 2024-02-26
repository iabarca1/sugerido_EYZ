import streamlit as st
import pyodbc
import time
from procesamiento import extraer_datos  # Importa la función desde procesamiento.py

def set_layout():
    st.set_page_config(page_title="Stock Assistant", layout="wide")

def apply_custom_styles():
    st.markdown("""
        <style>
        .stApp {
            background-color: white;
        }
        /* Cambia el color de fondo de los mensajes st.info */
        .stAlert {
            background-color: black;
            color: white;
        }
        /* Estilo para el botón de ancho completo */
        div.stButton > button:first-child {
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
        try:
            conn = init_connection()
            time.sleep(1)  # Espera un segundo antes de mostrar el siguiente mensaje
            st.success("Conexión exitosa")
            time.sleep(1)  # Espera un segundo antes de mostrar el siguiente mensaje
            # Muestra un mensaje de carga de datos
            info_message = st.empty()
            info_message.info('Cargando datos...')
            # Usar st.spinner para mostrar el spinner durante la carga de datos
            with st.spinner('Por favor espera...'):
                df = extraer_datos(conn)
            info_message.empty()  # Limpia el mensaje de info
            # Mostrar el DataFrame resultante en la aplicación
            st.dataframe(df)
        except Exception as e:
            st.error(f"Error al conectar a la base de datos: {e}")

if __name__ == "__main__":
    main()


