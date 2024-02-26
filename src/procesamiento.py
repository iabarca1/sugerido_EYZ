import pandas as pd
import numpy as np
from scipy.stats import norm
from sklearn.experimental import enable_iterative_imputer
from sklearn.impute import IterativeImputer

def extraer_datos(cnxn):

# Extracción de datos

    consulta_maestro= '''
    SELECT KOPR,
       NOKOPR,
       UD01PR,
       UD02PR,
       RLUD,
       FMPR,
       PFPR,
       HFPR
    FROM MAEPR WITH (NOLOCK)
    WHERE TIPR='FPN' AND ATPR<>'OCU' AND ATPR<>'OCO'
    '''
    consulta_maestro= pd.read_sql_query(consulta_maestro, cnxn)
    
    consulta_venta='''
    Select
    	DDO.KOPRCT, ---Sku
    	EDO.FEEMDO,-- 'Fecha'
    	sum(CASE WHEN DDO.PRCT = 1 THEN 0
         WHEN EDO.TIDO IN ('GDV', 'GDP', 'GDD', 'GRC', 'GRP', 'GRI') AND DDO.PRCT = 0 THEN DDO.CAPRCO2 - DDO.CAPREX2
         WHEN EDO.TIDO IN ('NCV', 'NCC', 'NCX', 'NEV') AND DDO.PRCT = 0 THEN DDO.CAPRCO2 * (-1)
         ELSE DDO.CAPRCO2
    	 END) as 'Q_Vta_2da_Unidad',
    	 sum(CASE WHEN EDO.TIMODO = 'E' THEN DDO.VANELI * EDO.TAMODO * (CASE WHEN EDO.TIDO IN ('NCV', 'NCX', 'NEV', 'NCC') THEN -1.0 ELSE 1.0 END)
         ELSE DDO.VANELI * (CASE WHEN EDO.TIDO IN ('NCV', 'NCX', 'NEV', 'NCC') THEN -1.0 ELSE 1.0 END)
    	END) as 'Venta_Neta'
    
    FROM MAEEDO EDO WITH (NOLOCK)
    LEFT JOIN MAEDDO AS DDO WITH (NOLOCK) ON EDO.IDMAEEDO = DDO.IDMAEEDO
    AND DDO.LILG NOT IN ('GR', 'IM')
    LEFT JOIN MAEEDOOB AS OBDO WITH (NOLOCK) ON EDO.IDMAEEDO = OBDO.IDMAEEDO
    LEFT JOIN MAEPR WITH (NOLOCK) ON MAEPR.KOPR = DDO.KOPRCT
    LEFT JOIN MAEEN WITH (NOLOCK) ON MAEEN.KOEN = EDO.ENDO AND MAEEN.SUEN = EDO.SUENDO
    LEFT JOIN MEVENTO EDOIND WITH (NOLOCK) ON EDO.IDMAEEDO = EDOIND.IDRVE
    AND EDOIND.ARCHIRVE = 'MAEEDO' AND EDO.TIDO = 'GDV' AND EDOIND.KOTABLA = 'ESPECIAL'
    AND EDOIND.KOCARAC = 'INDTRASLAD'
    LEFT JOIN TABCT WITH (NOLOCK) ON DDO.KOPRCT = TABCT.KOCT AND DDO.PRCT = 1
    WHERE
    (EDO.EMPRESA = '01'
    AND EDO.TIDO IN ('BLV', 'BLX', 'BSV', 'ESC', 'FCV', 'FDB', 'FDE', 'FDV', 'FDX', 'FDZ', 'FEE', 'FEV', 'FVL', 'FVT', 'FVX', 'FVZ', 'FXV',
                     'FYV', 'NCE', 'NCV', 'NCX', 'NCZ', 'NEV', 'RIN')
    AND EDO.FEEMDO BETWEEN (GETDATE()-366) AND GETDATE()-1 --año movil
    AND EDO.NUDONODEFI = 0
    AND EDO.ESDO <> 'N'
    AND MAEPR.TIPR='FPN' AND MAEPR.ATPR<>'OCU' AND MAEPR.ATPR<>'OCO' -- solo se traen los skus que generan venta
    AND SUDO='CM' -- solo se trabajará con el punto de venta CM
    
    )
    group by KOPRCT, FEEMDO
    '''
    consulta_venta= pd.read_sql_query(consulta_venta, cnxn)

    consulta_inventario='''
    SELECT TABBOPR.KOPR,
    	'STFI'=ISNULL(MAEST.STFI2,0.0),
    	'VALSTOCK'=MAEST.STFI2*MAEPREM.PM*MAEPR.RLUD 
    FROM TABBOPR WITH ( NOLOCK )  
    INNER  JOIN MAEPREM ON MAEPREM.EMPRESA='01' AND MAEPREM.KOPR=TABBOPR.KOPR
    INNER JOIN MAEPR ON MAEPR.KOPR=MAEPREM.KOPR AND TABBOPR.KOPR=MAEPR.KOPR  AND
    MAEPR.TIPR <> 'SSN'  AND MAEPR.TIPR <> 'FLN'  AND NOT COALESCE(MAEPR.BLOQUEAPR,'')
    IN ( 'C','V','T','X' )  
    LEFT  JOIN MAEST ON TABBOPR.EMPRESA='01' AND TABBOPR.KOSU=MAEST.KOSU AND TABBOPR.KOBO=MAEST.KOBO AND TABBOPR.KOPR=MAEST.KOPR  
    WHERE  TABBOPR.KOBO IN ('BCM')
    AND TABBOPR.EMPRESA='01'
    AND MAEPR.TIPR='FPN' AND MAEPR.ATPR<>'OCU' AND MAEPR.ATPR<>'OCO'
        '''
    consulta_inventario= pd.read_sql_query(consulta_inventario, cnxn)

    consulta_niveldeservicio='''
    SELECT dbo.MAEDDO.NUDO AS NUMERO,
	dbo.MAEDDO.FEEMLI AS FECHA,
	dbo.MAEDDO.FEERLI AS FECH_ENTREGA, -- esta es la fecha de entrega comprometida
	ISNULL((SELECT TOP 1 CONVERT(varchar,MAEDDO_1.FEEMLI, 103) FROM MAEDDO AS MAEDDO_1 WHERE MAEDDO_1.IDRST = MAEDDO.IDMAEDDO),'') AS FECHDCTOREL,
	case when ISNULL((SELECT TOP 1 CONVERT(varchar,MAEDDO_1.FEEMLI, 103) FROM MAEDDO AS MAEDDO_1 WHERE MAEDDO_1.IDRST = MAEDDO.IDMAEDDO),'')=''
	then 0 --ver q valor queremos entregar en estos casos, o si lo queremos sacar de la vista
	else datediff(day,dbo.MAEDDO.FEERLI,ISNULL((SELECT TOP 1 CONVERT(varchar,MAEDDO_1.FEEMLI, 103) FROM MAEDDO AS MAEDDO_1 WHERE MAEDDO_1.IDRST = MAEDDO.IDMAEDDO),'')) 
	end as 'Dias atraso',
	dbo.MAEDDO.ENDO AS COD_PROV, 
	dbo.MAEEN.NOKOEN AS NOMBRE_PROVEEDOR,
	dbo.MAEDDO.KOPRCT AS CODIGO, --sku 
	dbo.MAEPR.PESOUBIC AS PESO_UD1,
	dbo.MAEDDO.CAPRCO2 AS PEDIDO_UD2,
	dbo.MAEDDO.CAPREX2 AS RECIBIDO_UD2, 
    dbo.MAEDDO.CAPRCO2 - dbo.MAEDDO.CAPREX2 - dbo.MAEDDO.CAPRAD2 AS PEND_UD2, 
	dbo.MAEDDO.CAPREX2/dbo.MAEDDO.CAPRCO2 as CUMPLIMIENTO,
	dbo.MAEDDO.UD02PR AS UM_UD2
    FROM dbo.MAEDDO 
    LEFT OUTER JOIN dbo.TABFU ON dbo.MAEDDO.KOFULIDO = dbo.TABFU.KOFU 
    LEFT OUTER JOIN dbo.MAEPR ON dbo.MAEDDO.KOPRCT = dbo.MAEPR.KOPR
    LEFT OUTER JOIN dbo.MAEEDOOB ON dbo.MAEDDO.IDMAEEDO = dbo.MAEEDOOB.IDMAEEDO 
    LEFT OUTER JOIN dbo.MAEEN ON dbo.MAEDDO.ENDO = dbo.MAEEN.KOEN AND dbo.MAEDDO.SUENDO = dbo.MAEEN.SUEN
    WHERE dbo.MAEDDO.TIDO = 'OCC' 
    AND dbo.MAEDDO.FEEMLI BETWEEN {d '2023-01-01'} AND getdate() --traemos info del 2023 a la fecha
    and dbo.MAEDDO.ESLIDO='C'
    AND dbo.MAEDDO.BOSULIDO='BCM'
        '''
    consulta_niveldeservicio= pd.read_sql_query(consulta_niveldeservicio, cnxn)
    
    consulta_OCpendientes='''
    SELECT dbo.MAEDDO.NUDO AS NUMERO,
	dbo.MAEDDO.FEEMLI AS FECHA,
	dbo.MAEDDO.FEERLI AS FECH_ENTREGA, -- esta es la fecha de entrega comprometida
	ISNULL((SELECT TOP 1 CONVERT(varchar,MAEDDO_1.FEEMLI, 103) FROM MAEDDO AS MAEDDO_1 WHERE MAEDDO_1.IDRST = MAEDDO.IDMAEDDO),'') AS FECHDCTOREL,
	datediff(day,getdate(),dbo.MAEDDO.FEERLI) as 'Plazo',
	dbo.MAEDDO.ENDO AS COD_PROV, 
	dbo.MAEEN.NOKOEN AS NOMBRE_PROVEEDOR,
	dbo.MAEDDO.KOPRCT AS CODIGO, --sku 
	dbo.MAEPR.PESOUBIC AS PESO_UD1,
	dbo.MAEDDO.CAPRCO2 AS PEDIDO_UD2,
	dbo.MAEDDO.CAPREX2 AS RECIBIDO_UD2, 
	dbo.MAEDDO.CAPREX2/dbo.MAEDDO.CAPRCO2 as CUMPLIMIENTO,
	dbo.MAEDDO.CAPRCO2 - dbo.MAEDDO.CAPREX2 - dbo.MAEDDO.CAPRAD2 AS PEND_UD2, 
	dbo.MAEDDO.UD02PR AS UM_UD2
    FROM dbo.MAEDDO 
    LEFT OUTER JOIN dbo.TABFU ON dbo.MAEDDO.KOFULIDO = dbo.TABFU.KOFU 
    LEFT OUTER JOIN dbo.MAEPR ON dbo.MAEDDO.KOPRCT = dbo.MAEPR.KOPR
    LEFT OUTER JOIN dbo.MAEEDOOB ON dbo.MAEDDO.IDMAEEDO = dbo.MAEEDOOB.IDMAEEDO 
    LEFT OUTER JOIN dbo.MAEEN ON dbo.MAEDDO.ENDO = dbo.MAEEN.KOEN AND dbo.MAEDDO.SUENDO = dbo.MAEEN.SUEN
    WHERE dbo.MAEDDO.TIDO = 'OCC' 
    AND dbo.MAEDDO.FEEMLI BETWEEN {d '2023-01-01'} AND getdate() --traemos info del 2023 a la fecha
    and dbo.MAEDDO.ESLIDO<>'C'
    AND dbo.MAEDDO.BOSULIDO='BCM'
    order by dbo.MAEDDO.CAPREX2/dbo.MAEDDO.CAPRCO2
     '''
    consulta_OCpendientes= pd.read_sql_query(consulta_OCpendientes, cnxn)
    
    
# Procesamiento de datos
    
    maestro=consulta_maestro
    maestro.rename(columns={
        'NOKOPR': 'Nombre_Producto',
        'UD01PR': 'Unidad_Medida1',
        'UD02PR': 'Unidad_Medida2',
        'RLUD': 'Tasa_Conversion',
        'FMPR': 'Familia_Principal',
    }, inplace=True)
    maestro = maestro.drop(columns=['PFPR', 'HFPR'])
    
    
    tipologia = consulta_venta.groupby("KOPRCT")["Venta_Neta"].sum().reset_index()
    tipologia.columns = ['KOPRCT', 'Venta_Acumulada']
    tipologia = tipologia[tipologia['Venta_Acumulada'] > 0]
    tipologia=tipologia[tipologia["KOPRCT"].str.strip().astype(str)!="2010314"]
    # Procesamiento tipologia: Venta total relativa por SKU
    tipologia["Venta_%"]=tipologia["Venta_Acumulada"]/tipologia["Venta_Acumulada"].sum()
    # Procesamiento tipologia: Porcentaje acumulado 
    tipologia=tipologia.sort_values(by="Venta_%",ascending=False)
    tipologia["Venta_%Acumulado"]=tipologia["Venta_%"].cumsum()
    # Procesamiento tipologia: Clasificación por tipologia.
    condiciones = [
        tipologia['Venta_%Acumulado'] <= 0.5,
       (tipologia['Venta_%Acumulado'] > 0.5) & (tipologia['Venta_%Acumulado'] <= 0.8),
        (tipologia['Venta_%Acumulado'] > 0.8) & (tipologia['Venta_%Acumulado'] <= 0.95),
        tipologia['Venta_%Acumulado'] > 0.95
    ]
    
    elecciones = ['Tipología 1', 'Tipología 2', 'Tipología 3', 'Tipología 4']
    
    tipologia['Categoría_SKU'] = np.select(condiciones, elecciones, default='Otro')
    # Procesamiento tipolgia: Eliminar columnas sobrantes
    tipologia.drop(columns='Venta_%Acumulado', inplace=True)
    
    #Variables estadisticas venta
    st_venta=consulta_venta.groupby("KOPRCT")["Q_Vta_2da_Unidad"].agg(["mean","std","count"])
    st_venta['std'] = np.where(st_venta['std'].isna(),0, st_venta['std'])
    st_venta.rename(columns={'mean': 'Venta_Promedio', 'std': 'Venta_D.Est',"count":"Dias_Venta"}, inplace=True)
    st_venta.reset_index(inplace=True)
    #variables venta. 
    dias_habiles=consulta_venta["FEEMDO"].nunique()
    st_venta["%Dias_Venta"]=st_venta["Dias_Venta"]/dias_habiles
    st_venta.drop(columns=['Venta_Promedio',"Venta_D.Est"],inplace=True)

    #Construcción dataframe.
    fechas_habiles = consulta_venta['FEEMDO'].unique()
    skus = consulta_venta['KOPRCT'].unique()
    ventafull= pd.MultiIndex.from_product([skus, fechas_habiles], names=['KOPRCT', 'FEEMDO']).to_frame(index=False)
    #Llamada datos. 
    ventafull =ventafull.merge(consulta_venta, on=['KOPRCT', 'FEEMDO'], how='left').fillna(0)
    #Calculo Estadistica. 
    st_ventafull = ventafull.groupby('KOPRCT')['Q_Vta_2da_Unidad'].agg(['mean', 'std', 'count'])
    st_ventafull.rename(columns={'mean': 'Venta_Promedio', 'std': 'Venta_D.Est', 'count': 'Venta_Conteo'}, inplace=True)
    st_ventafull.reset_index(inplace=True)
    st_ventafull.drop(columns=["Venta_Conteo"],inplace=True)
    
    # Procesamiento columnas dataframe.
    consulta_niveldeservicio.drop(columns=['Dias atraso',"PESO_UD1","PEND_UD2","CUMPLIMIENTO","UM_UD2"],inplace=True)
    consulta_niveldeservicio.rename(columns={'CODIGO': 'KOPRCT'}, inplace=True)
    # Formato fecha dataframe.
    consulta_niveldeservicio['FECHA'] = pd.to_datetime(consulta_niveldeservicio["FECHA"],format='%Y/%m/%d')
    consulta_niveldeservicio['FECH_ENTREGA'] = pd.to_datetime(consulta_niveldeservicio["FECH_ENTREGA"],format='%Y/%m/%d')
    consulta_niveldeservicio['FECHDCTOREL'] = pd.to_datetime(consulta_niveldeservicio["FECHDCTOREL"],format='%d/%m/%Y')
    # Nan FECH_ENTREGA.
    consulta_niveldeservicio.loc[(consulta_niveldeservicio['FECH_ENTREGA'] < consulta_niveldeservicio['FECHA']), 'FECH_ENTREGA'] = np.nan
    consulta_niveldeservicio.loc[(consulta_niveldeservicio['FECH_ENTREGA'] - consulta_niveldeservicio['FECHA']).dt.days>60, 'FECH_ENTREGA'] = np.nan
    #Calculo Tiempo Pactado
    consulta_niveldeservicio["Tiempo_Pactado"]=(consulta_niveldeservicio['FECH_ENTREGA'] - consulta_niveldeservicio['FECHA']).dt.days
    #Calculo Lead Time
    consulta_niveldeservicio["Lead_Time"]=(consulta_niveldeservicio['FECHDCTOREL'] - consulta_niveldeservicio['FECHA']).dt.days
    # Nan Lead Time
    consulta_niveldeservicio.loc[consulta_niveldeservicio['Lead_Time'] <= 0, 'Lead_Time'] = np.nan
    consulta_niveldeservicio.loc[consulta_niveldeservicio['Lead_Time'] > 60, 'Lead_Time'] = np.nan
    #Calculo Fill rate.
    consulta_niveldeservicio["Cumplimiento_Tiempo"] = np.where(consulta_niveldeservicio['FECH_ENTREGA'].isna() | consulta_niveldeservicio['FECHDCTOREL'].isna(), np.nan, (consulta_niveldeservicio['FECH_ENTREGA'] >= consulta_niveldeservicio['FECHDCTOREL']).astype(int))
    consulta_niveldeservicio["Cumplimiento_Cantidad"]=(consulta_niveldeservicio['RECIBIDO_UD2'] == consulta_niveldeservicio['PEDIDO_UD2']).astype(int)
    consulta_niveldeservicio["Fill_Rate"] = np.where(consulta_niveldeservicio['Cumplimiento_Tiempo'].isna() | consulta_niveldeservicio['Cumplimiento_Cantidad'].isna(), np.nan, (consulta_niveldeservicio['Cumplimiento_Tiempo']*consulta_niveldeservicio['Cumplimiento_Cantidad']))
    
    st_compra=consulta_niveldeservicio.groupby("KOPRCT")["Lead_Time"].agg(["mean","std"])
    st_compra.rename(columns={
        'mean': 'Lead_Time_Promedio',
        'std': 'Lead_Time_D.Est',
    }, inplace=True)
    st_compra.reset_index(inplace=True)
    st_compra.rename(columns={'CODIGO': 'KOPRCT'}, inplace=True)

    # Maestro proveedores. 
    maestro_proveedor = consulta_niveldeservicio[["COD_PROV", "NOMBRE_PROVEEDOR"]].drop_duplicates()
    # Calculo variables. 
    st_proveedor = consulta_niveldeservicio.groupby(["KOPRCT", "COD_PROV"])[["Fill_Rate", "Lead_Time"]].agg({
        "Fill_Rate": ["mean"],
        "Lead_Time": ["mean"]
    })
    st_proveedor.columns = ['_'.join(col).strip() for col in st_proveedor.columns.values]
    st_proveedor.reset_index(inplace=True)
    # Eliminar filas con NaN
    st_proveedor=st_proveedor.dropna()
    # Agregar nombre del proveedor
    st_proveedor=st_proveedor.merge(maestro_proveedor, left_on='COD_PROV', right_on='COD_PROV', how='left')
    nuevo_orden=['KOPRCT', 'COD_PROV', 'NOMBRE_PROVEEDOR','Fill_Rate_mean',
           'Lead_Time_mean']
    st_proveedor=st_proveedor[nuevo_orden]
    ranking_proveedores = st_proveedor.sort_values(by=['KOPRCT', 'Fill_Rate_mean'], ascending=[True, False])
    ranking_proveedores = ranking_proveedores.groupby('KOPRCT').head(1)

    # Procesamiento Inventario
    consulta_inventario.head()
    inventario=consulta_inventario
    inventario.rename(columns={'STFI': 'Stock_Disponible'}, inplace=True)
    
    compras = consulta_OCpendientes.groupby("CODIGO")["PEND_UD2"].sum().reset_index(name='Suma_PEND_UD2')
    compras.rename(columns={'CODIGO': 'KOPRCT',"Suma_PEND_UD2":"Unidades_Pendientes"}, inplace=True)
    
# Consolidación Parte I: Cálculo del stock de seguridad

    #Unión Maestro y tipología. 
    sugerido_base = maestro.merge(tipologia, left_on='KOPR', right_on='KOPRCT', how='left')
    sugerido_base.sort_values(by="Venta_%",ascending=False,inplace=True)
    sugerido_base.drop(columns=['KOPRCT'],inplace=True)
    Filtro_tipologia= ['Tipología 1', 'Tipología 2', 'Tipología 3','Tipología 4']
    sugerido_base = sugerido_base[sugerido_base['Categoría_SKU'].isin(Filtro_tipologia)]
    
    #Unión Estadistica venta full. 
    sugerido_base=sugerido_base.merge(st_ventafull, left_on='KOPR', right_on='KOPRCT', how='left')
    sugerido_base.drop(columns=['KOPRCT'],inplace=True)
    
    #Unión Estadistica venta. 
    sugerido_base=sugerido_base.merge(st_venta, left_on='KOPR', right_on='KOPRCT', how='left')
    #sugerido_base.fillna("Sin venta", inplace=True)
    sugerido_base.drop(columns=['KOPRCT'],inplace=True)
    
    #Unión Estadistica compra.
    sugerido_base=sugerido_base.merge(st_compra, left_on='KOPR', right_on='KOPRCT', how='left')
    sugerido_base.drop(columns=['KOPRCT'],inplace=True)
    
    #Imputación Nan
    columnas=sugerido_base.columns
    # Crear un subdataframe para la imputación. 
    sugerido_base_imputacion = sugerido_base[['KOPR', 'Familia_Principal','Venta_%',
           'Categoría_SKU', 'Venta_Promedio', 'Venta_D.Est', 'Dias_Venta',
           '%Dias_Venta', 'Lead_Time_Promedio', 'Lead_Time_D.Est']]
    sugerido_base_imputacion.set_index('KOPR', inplace=True)
    sugerido_base_imputacion=pd.get_dummies(sugerido_base_imputacion)
    # Inicializa el imputador iterativo
    imputer = IterativeImputer(max_iter=100,
        tol=1e-10)
    # Aplica la imputación múltiple
    sugerido_base_imputado= imputer.fit_transform(sugerido_base_imputacion)
    # El resultado es un array de numpy, así que lo convertimos de nuevo a un DataFrame
    sugerido_base_imputado= pd.DataFrame(sugerido_base_imputado, columns=sugerido_base_imputacion.columns, index=sugerido_base_imputacion.index)
    sugerido_base_imputado=sugerido_base_imputado[['Venta_%', 'Venta_Promedio', 'Venta_D.Est', 'Dias_Venta', '%Dias_Venta',
           'Lead_Time_Promedio', 'Lead_Time_D.Est']]
    # Combinar dataframe
    sugerido_base.set_index('KOPR', inplace=True)
    sugerido_base = sugerido_base.combine_first(sugerido_base_imputado)
    sugerido_base.reset_index(inplace=True)
    sugerido_base=sugerido_base[columnas]

    #Stock de seguridad.
    # Definir niveles
    Nivel_seguridad_t1 = 0.99
    Nivel_seguridad_t2 = 0.95
    Nivel_seguridad_t3 = 0.90
    Nivel_seguridad_t4 = 0.85
    
    # Definir condiciones
    condiciones = [
        sugerido_base['Categoría_SKU'] == "Tipología 1",
        sugerido_base['Categoría_SKU'] == "Tipología 2",
        sugerido_base['Categoría_SKU'] == "Tipología 3",
        sugerido_base['Categoría_SKU'] == "Tipología 4"
    ]
    
    # Definir elecciones
    elecciones = [
        Nivel_seguridad_t1,
        Nivel_seguridad_t2,
        Nivel_seguridad_t3,
        Nivel_seguridad_t4
    ]
    
    # Aplicar condiciones
    sugerido_base['Nivel_seguridad'] = np.select(condiciones, elecciones,default=np.nan)
    
    #Cálculo del stock de seguridad
    term_inside_sqrt = (sugerido_base["Lead_Time_Promedio"] * sugerido_base["Venta_D.Est"]**2) + (sugerido_base["Venta_Promedio"]**2 * sugerido_base["Lead_Time_D.Est"]**2)
    sugerido_base["Stock_seguridad"]=(norm.ppf(1 - (1 - (sugerido_base["Nivel_seguridad"])) / 2))*np.sqrt(term_inside_sqrt)
    sugerido_base['Stock_seguridad'] = sugerido_base['Stock_seguridad'].round()
    
# Consolidación Parte II: Cálculo del stock de seguridad
    
    #Unión Ranking Proveedor.
    sugerido_base=sugerido_base.merge(ranking_proveedores, left_on='KOPR', right_on='KOPRCT', how='left')
    sugerido_base.drop(columns=['KOPRCT'],inplace=True)
    sugerido_base['Lead_Time_mean'] = sugerido_base['Lead_Time_mean'].round()
    #sugerido_base.fillna("Sin data disponible", inplace=True)
    
    #Unión Inventario. 
    sugerido_base=sugerido_base.merge(inventario, left_on='KOPR', right_on='KOPR', how='left')
    sugerido_base.drop(columns=['VALSTOCK'],inplace=True)
    sugerido_base["Stock_Disponible"]=sugerido_base["Stock_Disponible"].fillna(0)
    
    #Unión Compra.
    sugerido_base=sugerido_base.merge(compras, left_on='KOPR', right_on='KOPRCT', how='left')
    sugerido_base.drop(columns=['KOPRCT'],inplace=True)
    sugerido_base['Unidades_Pendientes'].fillna(0, inplace=True)

    #Punto Re-orden.
    def punto_reorden(row):
    # Comprueba si COD_PROV no es NaN
        if not pd.isna(row["COD_PROV"]):
            # Realiza los cálculos y comprueba si es necesario realizar una compra
            if (row["Stock_Disponible"] + row["Unidades_Pendientes"] - 
                (row["Lead_Time_mean"] * row["Venta_Promedio"]) < row["Stock_seguridad"]):
                return "Compra"
            else:
                return "No_Compra"
        else:
            # Realiza los cálculos y comprueba si es necesario realizar una compra
            if (row["Stock_Disponible"] + row["Unidades_Pendientes"]< row["Stock_seguridad"]):
                return "Compra"
            else:
                return "No_Compra"          
    # Aplicar la función a cada fila del DataFrame
    sugerido_base['Punto_Re-orden'] = sugerido_base.apply(punto_reorden, axis=1)
    
    #Unidades a comprar.
    def unidades_compra(row):
    # Comprueba si COD_PROV no es NaN
        if not pd.isna(row["COD_PROV"]):
            # Realiza los cálculos y comprueba si es necesario realizar una compra
            if row["Punto_Re-orden"]=="Compra":
                return row["Stock_seguridad"]-(row["Stock_Disponible"] + row["Unidades_Pendientes"] - (row["Lead_Time_mean"] * row["Venta_Promedio"]))
            else:
                return 0
        else:
            # Realiza los cálculos y comprueba si es necesario realizar una compra
            if row["Punto_Re-orden"]=="Compra":
                return row["Stock_seguridad"]-(row["Stock_Disponible"] + row["Unidades_Pendientes"])
            else:
                return 0
    # Aplicar la función a cada fila del DataFrame
    sugerido_base['Unidades_Compra (UM2)'] = sugerido_base.apply(unidades_compra, axis=1)
    
    # Transformar a UM1
    sugerido_base["Unidades_Compra (UM1)"]=sugerido_base["Unidades_Compra (UM2)"]*sugerido_base["Tasa_Conversion"]
    
    # Ajustes: Redondear y reemplazar Nan
    sugerido_base["Unidades_Compra (UM2)"] = sugerido_base['Unidades_Compra (UM2)'].round()
    sugerido_base["Unidades_Compra (UM1)"] = sugerido_base['Unidades_Compra (UM1)'].round()
    sugerido_base.fillna("Sin data disponible", inplace=True)

# Formato Output
    sugerido_frontend=sugerido_base
    columnas_porcentaje2 = ["Venta_%", "%Dias_Venta","Nivel_seguridad","Fill_Rate_mean"]
    for columna in columnas_porcentaje2:
        sugerido_frontend[columna] = sugerido_frontend[columna].apply(lambda x: f"{x:.2%}" if x != "Sin data disponible" else x)

    columnas_decimal2 = ["Tasa_Conversion", "Venta_Promedio","Venta_D.Est","Lead_Time_Promedio","Lead_Time_D.Est"]
    for columna in columnas_decimal2:
        sugerido_frontend[columna] = sugerido_frontend[columna].apply(lambda x: f"{x:.2f}" if x != "Sin data disponible" else x)

    columnas_decimal0 = ["Stock_seguridad","Lead_Time_mean","Stock_Disponible","Unidades_Pendientes","Unidades_Compra (UM2)","Unidades_Compra (UM1)"]
    for columna in columnas_decimal0:
        sugerido_frontend[columna] = sugerido_frontend[columna].apply(lambda x: f"{x:.0f}" if x != "Sin data disponible" else x)

    sugerido_frontend["Venta_Acumulada"] = sugerido_frontend["Venta_Acumulada"].apply(lambda x: f"${x:,.0f}")
    
    return sugerido_frontend