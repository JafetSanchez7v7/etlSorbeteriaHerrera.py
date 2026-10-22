import hashlib
import pandas as pd 
from pymongo import MongoClient
from sqlalchemy import create_engine
import pyodbc

### DEFINICION DE CONEXIONES A MONGO DB ###
MONGO_URI ='mongodb://localhost:27017/'
LOGS_DB ='SystemLogsDb'
MONGO_COLLECTION ='AuditLogs'

SQL_SERVER ='localhost,1433'
SQL_DATABASE ='HerreraSystemLogs_DW'
SQL_USER = "sa"
SQL_PASSWORD = "Jafet0330R"

### ETL PROCCESS

def exec_etl():
    print("ejecutando proceso etl")

    mongo_client = MongoClient(MONGO_URI)
    mongo_db = mongo_client[LOGS_DB]
    collection = mongo_db[MONGO_COLLECTION]


    ##------------------------------------
    ## EXTRACION
    ##------------------------------------

    ## traemos todos los documentos sin los ids 
    documentos = list(collection.find({}, {'_id': 0}))

    if not documentos:
        print("no hay documentos en la collecion")
        return

    ## aplanamos los json de los detalles de request response y exception
    df_crudo = pd.json_normalize(documentos)

    ## reemplazamos los valor.valor a valor_valor para lecutra en sql server
    df_crudo.columns = df_crudo.columns.str.replace('.', '_')

    ##------------------------------------------------
    ## TRANSFORMACION
    ##------------------------------------------------

    print("proceso de transformacion para ajustar a la bd sql")


    
    ## dimension de fecha
    df_fecha = pd.DataFrame()

    ts ='Timestamp_original'
    
    df_fecha[ts] = pd.to_datetime(df_crudo['Timestamp'])
    df_fecha["Id_Fecha"] =df_fecha[ts].dt.strftime('%Y%m%d')
    df_fecha['Anio'] = df_fecha[ts].dt.year
    df_fecha['Mes'] = df_fecha[ts].dt.month
    df_fecha['Dia'] = df_fecha[ts].dt.day
    df_fecha['Hora'] = df_fecha[ts].dt.hour
    df_fecha['Minuto'] = df_fecha[ts].dt.minute

    df_fecha = df_fecha.drop_duplicates(subset=['Id_Fecha'])

    ## dimension request
    df_crudo = df_crudo.columns.str.replace('.', '_')

    df_request = pd.DataFrame()

    df_request['Method'] = df_crudo['Request_Method'].fillna('Desconocido')
    df_request['Path'] = df_crudo['Request_Path'].fillna('/')
    df_request['QueryString'] = df_crudo['Request_QueryString'].fillna('')
    df_request['Payload'] = df_crudo['Request_Payload'].fillna('')
    ## llave para metodo y la ruta
    df_request['Id_Request'] = df_request.apply(lambda row: hashlib.md5((row['Method'] + row['Path']).encode()).hexdigest()[:10], axis=1)

    df_request = df_request.drop_duplicates(subset=['Id_Request'])


    ##dimension response

    df_response = pd.DataFrame()

    df_response['StatusCode'] = df_crudo['Response_StatusCode'].fillna(0)
    df_response['StatusText'] = df_crudo['Response_Status'].fillna('')
    df_response['Id_Response'] = df_crudo['Response_StatusCode']

    df_response = df_response.drop_duplicates(subset=['Id_Response'])

    ## dimension excepcion
    df_excepcion = pd.DataFrame()

    df_excepcion['Message'] = df_crudo['Excepcion_Message'].fillna('No Hay Excepcion') if 'Exception_Message' in df_crudo.columns else 'No Hay Excepcion'
    df_excepcion['StackTrace'] = df_crudo['Excepcion_StackTrace'].fillna('N/A') if 'Exception_Message' in df_crudo.columns else 'N/A'
    df_excepcion['Source'] = df_crudo['Exception_Source'].fillna('N/A') if 'Exception_Source' in df_crudo.columns else 'N/A'

    df_excepcion['Id_Excepcion'] = df_excepcion.apply(lambda r: hashlib.md5(f"{r['Message']}".encode()).hexdigest()[:10], axis=1)
    df_excepcion = df_excepcion.drop_duplicates(subset=['Id_Excepcion'])
    

    ### hecho

    df_hechos = pd.DataFrame()
    df_hechos['TraceId'] = df_crudo['TraceId']
    df_hechos['Level'] = df_crudo['Level'].fillna('Information')
    df_hechos['Environment'] = df_crudo['Environment'].fillna('Development')
    df_hechos['ClientIp'] = df_crudo['ClientIp'].fillna(df_crudo.get('Ip', '127.0.0.1'))
    df_hechos['UserId'] = df_crudo['UserId'].fillna('Anonymous')
    df_hechos['UserName'] = df_crudo['UserName'].fillna('Anonymous')
    df_hechos['Rol'] = df_crudo['Rol'].fillna('None')
    df_hechos['Device'] = df_crudo['Device'].fillna('Unknown')

    # relaciones de las dimensiones que ya habiamos hecho
    df_hechos['Id_Fecha'] = pd.to_datetime(df_crudo['Timestamp']).dt.strftime('%Y%m%d')
    df_hechos['Id_Request'] = df_crudo.apply(lambda r: hashlib.md5(f"{r['Request_Method']}-{r['Request_Path']}".encode()).hexdigest()[:10], axis=1)
    df_hechos['Id_Response'] = df_crudo['Response_StatusCode'].fillna(200).astype(int)
    df_hechos['Id_Excepcion'] = df_crudo.apply(lambda r: hashlib.md5(f"{str(r.get('Exception_Message', 'No Exception'))}".encode()).hexdigest()[:10], axis=1)
    
    # Métrica de rendimiento numérico extraída del ResponseDetail de C#
    df_hechos['ElapsedMilliseconds'] = pd.to_numeric(df_crudo['Response_ElapsedMilliseconds']).fillna(0).astype(int)

    ## ------------------------
    ## CARGA
    ##-------------------------

    print("realizando carga de dimensiones y hechos")

    ## driver
    engine_str = f"mssql+pyodbc://{SQL_USER}:{SQL_PASSWORD}@{SQL_SERVER}/{SQL_DATABASE}?driver=ODBC+Driver+17+for+SQL+Server&TrustServerCertificate=yes"
    engine = create_engine(engine_str)

    conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SQL_SERVER};DATABASE={SQL_DATABASE};UID={SQL_USER};PWD={SQL_PASSWORD};TrustServerCertificate=yes;"
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    ## funcion interna para cargar las dimensiones

    def cargar_dimensiones(df, destino, pk):
        if df.empty:
            return
        
        # creamos una tabla en memoria para simular el staging
        df.to_sql(f"#{destino}", con=conn, if_exists='replace', index=False)

        #mapeamos
        columnas = ", ".join(df.columns)
        valores_source = ", ".join([f"source.{col}" for col in df.columns])
        actualizar_sets = ", ".join([f"target.{col}" for col in df.columns if col != pk])

        ## ejecutamos la query

        merge_query = f"""
        MERGE {destino} AS target
        USING #{destino} AS source
        ON target.{pk} = source.{pk}
        WHEN MATCHED THEN 
            UPDATE SET {actualizar_sets}
        WHEN NOT MATCHED THEN 
            INSERT ({columnas}) VALUES ({valores_source});
        """

        cursor.execute(merge_query)
        conn.commit()
        print(f" DIMENSION {destino} CARGADA")

    cargar_dimensiones(df_fecha, "Dim_Fecha", "Id_Fecha")
    cargar_dimensiones(df_request, "Dim_Request", "Id_Request")
    cargar_dimensiones(df_response, "Dim_Response", "Id_Response")
    cargar_dimensiones(df_excepcion, "Dim_Excepcion", "Id_Excepcion")

    # INSERTAMOS LA TABLA DE HECHOS VALIDANDO QUE NO SE DUPLIQUE

    #al igual que las dimensiones usamos una tabla temporal
    if not df_hechos.empty:
        df_hechos.to_sql("#Fact_ApiLogs", con=engine, if_exists='replace', index=False)
        
        insert_hechos_query = """
        INSERT INTO Fact_ApiLogs (TraceId, Level, Environment, ClientIp, UserId, UserName, Rol, Device, ID_Fecha, ID_Request, ID_Response, ID_Excepcion, ElapsedMilliseconds)
        SELECT source.TraceId, source.Level, source.Environment, source.ClientIp, source.UserId, source.UserName, source.Rol, source.Device, source.ID_Fecha, source.ID_Request, source.ID_Response, source.ID_Excepcion, source.ElapsedMilliseconds
        FROM #Fact_ApiLogs AS source
        WHERE NOT EXISTS (
            SELECT 1 FROM Fact_ApiLogs AS target WHERE target.TraceId = source.TraceId
        );
        """
        cursor.execute(insert_hechos_query)
        conn.commit()

    print("CARGA DEL HECHO FINALIZADO")

    cursor.close()
    conn.close()

    print("ETL finalizado")


    if __name__ is "__main__":
        exec_etl()
        


    


