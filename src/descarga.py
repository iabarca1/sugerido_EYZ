from io import BytesIO
import pandas as pd

def archivo_excel(base):
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        base.to_excel(writer, index=False, sheet_name='Sheet1')
        workbook = writer.book
        worksheet = writer.sheets['Sheet1']

        # Formato para los números en porcentaje
        int_format = workbook.add_format({'num_format': '0'})
        decimal_format = workbook.add_format({'num_format': '0.00'})
        percent_format = workbook.add_format({'num_format': '0.00%'})
        pesos_format = workbook.add_format({'num_format': '$#,##0'})

        # Aplicar formato de porcentaje a las columnas específicas por nombre
        for idx, column in enumerate(base.columns):
            column_width = max(base[column].astype(str).map(len).max(), len(column))
            worksheet.set_column(idx, idx, column_width)  # Autoajustar el ancho de columna basado en el contenido

            if column in ['Stock_Seguridad', 'Tiempo_Entrega_Proveedor', 'Stock_Disponible', 'Unidades_Pendientes', 'Unidades_Compra1','Unidades_Compra2','Dias_Venta']:  # Los nombres de tus columnas
                worksheet.set_column(idx, idx, column_width, int_format)  # Aplicar el formato entero

            if column in ['Tasa_Conversion', 'Venta_Diaria_Promedio', 'Venta_Diaria_D.Estandar', 'Tiempo_Entrega_Promedio_SKU', 'Tiempo_Entrega_D.Estandar_SKU']:  # Los nombres de tus columnas
                worksheet.set_column(idx, idx, column_width, decimal_format)  # Aplicar el formato decimal
                
            if column in ['Nivel_Seguridad', 'Nivel_Servicio_Proveedor', '%Venta_Acumulada', '%Dias_Venta']:  # Los nombres de tus columnas
                worksheet.set_column(idx, idx, column_width, percent_format)  # Aplicar el formato de porcentaje

            if column in ['Venta_Acumulada']:  # Los nombres de tus columnas
                worksheet.set_column(idx, idx, column_width, pesos_format)  # Aplicar el formato de pesos

        # Formatos condicionales para la columna "Estado"
        format_red = workbook.add_format({'bg_color': '#FFC7CE', 'font_color': '#9C0006'})
        format_yellow = workbook.add_format({'bg_color': '#FFEB9C', 'font_color': '#9C6500'})
        format_green = workbook.add_format({'bg_color': '#C6EFCE', 'font_color': '#006100'})

        # Aplicar formato condicional a la columna "Estado"
        formato_critico = workbook.add_format({'bg_color': '#C00000', 'font_color': '#FFFFFF'})  # Rojo
        formato_urgente = workbook.add_format({'bg_color': '#FFFF00', 'font_color': '#000000'})  # Amarillo
        formato_seguro = workbook.add_format({'bg_color': '#00B050', 'font_color': '#FFFFFF'})   # Verde

        estado_col_idx = base.columns.get_loc('Estado') + 1  # +1 porque las columnas en Excel empiezan en 1
        worksheet.conditional_format(f'E2:E{len(base) + 1}', {'type': 'cell', 'criteria': 'equal to', 'value': '"Crítico"', 'format': formato_critico})
        worksheet.conditional_format(f'E2:E{len(base) + 1}', {'type': 'cell', 'criteria': 'equal to', 'value': '"Urgente"', 'format': formato_urgente})
        worksheet.conditional_format(f'E2:E{len(base) + 1}', {'type': 'cell', 'criteria': 'equal to', 'value': '"Seguro"', 'format': formato_seguro})

        

    data_procesada = output.getvalue()
    return data_procesada