import pandas as pd

def alerta_estado(base):
    # First, ensure that the %Venta_Acumulada column is in numeric form to calculate the sum correctly
    base['%Venta_Acumulada'] = pd.to_numeric(base['%Venta_Acumulada'].str.rstrip('%'), errors='coerce') / 100

    # Group by 'Estado' and calculate the count of SKUs and the sum of %Venta_Acumulada
    resumen_estado = base.groupby('Estado').agg(Cantidad_SKU=("Id_SKU", 'count'), Suma_Venta=('%Venta_Acumulada', 'sum')).reset_index()

    # Formatear el resultado para la salida deseada
    alertas = []
    for _, fila in resumen_estado.iterrows():
        # Ensure that the Suma_Venta is formatted as a percentage
        suma_venta_formatted = f"{fila['Suma_Venta']:.2%}" if pd.notnull(fila['Suma_Venta']) else "Sin data disponible"
        alerta = f"{fila['Cantidad_SKU']} SKUs se encuentran en estado {fila['Estado']} representando el {suma_venta_formatted} de la venta total"
        alertas.append(alerta)

    # Join all alerts into a single string
    texto_alertas = ' ; '.join(alertas)

    return texto_alertas

def alerta_compra(base):
    # No necesitas convertir a numérico aquí a menos que %Venta_Acumulada necesite ser numérico para otro cálculo

    # Group by 'Punto_Re-orden' and calculate the count of SKUs and the sum of %Venta_Acumulada
    resumen_punto_reorden = base.groupby('Punto_Re-orden').agg(
        Cantidad_SKU=("Id_SKU", 'count'), 
        Suma_Venta=('%Venta_Acumulada', 'sum')
    ).reset_index()

    # Formatear el resultado para la salida deseada
    alertas = []
    for _, fila in resumen_punto_reorden.iterrows():
        suma_venta_formatted = f"{fila['Suma_Venta']:.2%}" if pd.notnull(fila['Suma_Venta']) else "Sin data disponible"
        alerta = f"{fila['Cantidad_SKU']} SKUs se encuentran propensos a '{fila['Punto_Re-orden']}' representando el {suma_venta_formatted} de la venta total"
        alertas.append(alerta)

    # Join all alerts into a single string with line breaks
    texto_alertas = ' ; '.join(alertas)

    return texto_alertas

