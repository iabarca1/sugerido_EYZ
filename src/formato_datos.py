
def highlight_state_cell(s):
    if s['Estado'] == 'Crítico':
        return ['background-color: red' if col == 'Estado' else '' for col in s.index]
    elif s['Estado'] == 'Urgente':
        return ['background-color: yellow' if col == 'Estado' else '' for col in s.index]
    elif s['Estado'] == 'Seguro':
        return ['background-color: green' if col == 'Estado' else '' for col in s.index]
    else:
        return ['' for _ in s]

def transformacion_datos(data):

# Formato Output
    sugerido_frontend=data
    columnas_porcentaje2 = ["%Venta_Acumulada", "%Dias_Venta","Nivel_Seguridad","Nivel_Servicio_Proveedor"]
    for columna in columnas_porcentaje2:
        sugerido_frontend[columna] = sugerido_frontend[columna].apply(lambda x: f"{x:.2%}" if x != "Sin data disponible" else x)

    columnas_decimal2 = ["Tasa_Conversion", "Venta_Diaria_Promedio","Venta_Diaria_D.Estandar","Tiempo_Entrega_Promedio_SKU","Tiempo_Entrega_D.Estandar_SKU"]
    for columna in columnas_decimal2:
        sugerido_frontend[columna] = sugerido_frontend[columna].apply(lambda x: f"{x:.2f}" if x != "Sin data disponible" else x)

    columnas_decimal0 = ["Stock_Seguridad","Tiempo_Entrega_Proveedor","Stock_Disponible","Unidades_Pendientes","Unidades_Compra1","Unidades_Compra2"]
    for columna in columnas_decimal0:
        sugerido_frontend[columna] = sugerido_frontend[columna].apply(lambda x: f"{x:.0f}" if x != "Sin data disponible" else x)

    sugerido_frontend["Venta_Acumulada"] = sugerido_frontend["Venta_Acumulada"].apply(lambda x: f"${x:,.0f}")
    
    sugerido_frontend = sugerido_frontend.style.apply(highlight_state_cell, axis=1)

    return sugerido_frontend
