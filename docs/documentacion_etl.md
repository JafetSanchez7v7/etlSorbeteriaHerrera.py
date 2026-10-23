# DOCUMENTACIÓN DEL PROCESO ETL TÉCNICO — SORBETERÍA HERRERA 2026

## Componente: Base de Datos Analítica

## 1. Introducción

El presente documento describe el proceso de extracción, transformación y carga (ETL) de los registros de auditoría generados por el sistema de Sorbetería Herrera. Este proceso permite trasladar los registros almacenados en MongoDB hacia un almacén de datos técnico implementado en SQL Server.

La información procesada se organiza mediante un modelo dimensional, con el propósito de facilitar su análisis posterior a través de consultas analíticas y cubos OLAP.

## 2. Objetivo

El proceso ETL de Sorbetería Herrera tiene como objetivo extraer los datos de auditoría almacenados en MongoDB, transformarlos mediante Python y bibliotecas como Pandas, y cargar los datos procesados en SQL Server, donde se encuentra el almacén de datos técnico destinado a su posterior análisis.

## 3. Alcance

El proceso ETL comprende las siguientes actividades:

- Extracción de los registros de auditoría desde MongoDB.
- Transformación y normalización de los documentos obtenidos mediante Python y Pandas.
- Preparación de los datos para su almacenamiento en las tablas dimensionales y la tabla de hechos.
- Carga de los datos en SQL Server mediante tablas de staging y operaciones de integración.
- Actualización de los registros dimensionales existentes y registro de nuevas entradas según sus claves.

El proceso no incluye la creación, configuración ni procesamiento de cubos OLAP, independientemente de su arquitectura de almacenamiento, ya que estas actividades corresponden a una fase posterior del proyecto.

Asimismo, el ETL no tiene como objetivo modificar los registros originales almacenados en MongoDB; las transformaciones se realizan sobre los datos extraídos para preparar su incorporación al almacén de datos.

## 4. Requerimientos del sistema

Para ejecutar el proceso ETL se requieren los siguientes componentes:

- **Docker:** para construir y ejecutar el contenedor del proceso ETL.
- **Docker Compose:** para configurar y ejecutar el servicio mediante el archivo de composición.
- **MongoDB:** instancia que contiene los registros de auditoría de la API.
- **SQL Server:** instancia que aloja el almacén de datos técnico.
- **Configuración de conexión:** variables de entorno con los parámetros necesarios para conectarse a las bases de datos.
- **Dependencias de Python:** Pandas, PyMongo, SQLAlchemy, PyODBC y las demás bibliotecas definidas en el proyecto.
- **Controlador ODBC:** controlador compatible con SQL Server instalado dentro del entorno de ejecución del ETL.

MongoDB y SQL Server pueden ejecutarse en servicios locales o contenedores, siempre que el contenedor ETL pueda acceder a ambos servicios mediante sus respectivas configuraciones de conexión.

## 5. Flujo del proceso ETL

### 5.1. Origen de los datos

La API de Sorbetería Herrera cuenta con un middleware encargado de interceptar las peticiones HTTP recibidas por la aplicación. A partir del contexto de cada petición, se recopila información relacionada con la solicitud, la respuesta y, cuando corresponde, las excepciones producidas durante su procesamiento.

Los registros generados se almacenan como documentos en MongoDB mediante el controlador correspondiente de MongoDB para .NET.

La estructura del registro de auditoría se representa mediante la clase `ApiUsageLog`, junto con las clases auxiliares `RequestDetail`, `ResponseDetail` y `ExceptionDetail`.

```csharp
public class ApiUsageLog
{
    public string? Id { get; set; }
    public string TraceId { get; set; } = string.Empty;
    public DateTime Timestamp { get; set; } = DateTime.UtcNow;
    public string Level { get; set; } = "Information";
    public string Environment { get; set; } = "Development";
    public string? ClientIp { get; set; }
    public string? UserId { get; set; }
    public RequestDetail Request { get; set; } = new();
    public ResponseDetail Response { get; set; } = new();
    public ExceptionDetail? Exception { get; set; }
    public string? UserName { get; set; }
    public string? Rol { get; set; }
    public string? Device { get; set; }
}

public class RequestDetail
{
    public string Method { get; set; } = string.Empty;
    public string Path { get; set; } = string.Empty;
    public string? QueryString { get; set; }
    public string? Payload { get; set; }
}

public class ResponseDetail
{
    public int StatusCode { get; set; }
    public long ElapsedMilliseconds { get; set; }
    public string? Status { get; set; }
}

public class ExceptionDetail
{
    public string Message { get; set; } = string.Empty;
    public string? StackTrace { get; set; }
    public string Source { get; set; } = string.Empty;
}
```

**Descripción de los campos principales:**

- `TraceId`: identificador asociado al seguimiento de una petición.
- `Timestamp`: fecha y hora en que se registra el evento.
- `Level` y `Environment`: nivel del registro y entorno de ejecución.
- `ClientIp`, `UserId`, `UserName` y `Rol`: información contextual del cliente o usuario, cuando está disponible.
- `Request`: información sobre el método HTTP, la ruta y otros datos de la solicitud.
- `Response`: información sobre el código de estado y el tiempo de procesamiento.
- `Exception`: información sobre una posible excepción, incluyendo su mensaje, origen y traza.

Los objetos `Request`, `Response` y `Exception` contienen estructuras anidadas que posteriormente deben normalizarse para facilitar su procesamiento en Python.

### 5.2. Extracción de los datos

Una vez almacenados los registros en MongoDB, el proceso ETL utiliza Python y la biblioteca PyMongo para conectarse a la colección de auditoría y recuperar los documentos.

El siguiente fragmento representa la extracción y normalización inicial de los datos:

```python
# Recuperar los documentos sin incluir el identificador interno de MongoDB.
documentos = list(collection.find({}, {'_id': 0}))

if not documentos:
    print("No hay documentos en la colección")
    return

# Aplanar los objetos anidados de los documentos.
df_crudo = pd.json_normalize(documentos)

# Normalizar los nombres de las columnas.
df_crudo.columns = df_crudo.columns.str.replace('.', '_')
```

La extracción se realiza mediante `collection.find()`, que recupera los documentos de la colección seleccionada. La proyección `{'_id': 0}` excluye el identificador interno de MongoDB de los resultados obtenidos.

Posteriormente, `pd.json_normalize()` convierte los documentos JSON en una estructura tabular representada mediante un DataFrame de Pandas. Los objetos anidados se convierten en columnas cuyos nombres reflejan su jerarquía.

Finalmente, se reemplazan los puntos de los nombres de las columnas por guiones bajos. Esta normalización facilita la referencia a los campos durante las transformaciones y la carga en SQL Server.

Si la colección no contiene documentos, el proceso muestra un mensaje y termina esta ejecución sin continuar con las etapas posteriores.

### 5.3. Transformación de los datos

Después de la extracción, los documentos se encuentran disponibles en memoria mediante el DataFrame `df_crudo`. A partir de este conjunto de datos se construyen DataFrames específicos para representar las dimensiones y los registros de hechos del almacén de datos técnico.

Durante esta etapa se realizan operaciones de normalización, tratamiento de valores nulos, conversión de tipos de datos y generación de identificadores que permiten relacionar los registros entre las diferentes tablas del modelo dimensional.

#### 5.3.1. Transformación de la dimensión de fecha

La dimensión de fecha permite organizar los registros de auditoría de acuerdo con el momento en que se produjo cada evento. Para construirla, se toma el campo `timestamp` de los documentos originales y se convierte a un tipo de dato de fecha y hora mediante Pandas.

```python
# Construcción de la dimensión de fecha.
df_fecha = pd.DataFrame()

ts = 'Timestamp_original'
df_fecha[ts] = pd.to_datetime(df_crudo['timestamp'])

# Generación de la clave de fecha en formato AAAAMMDD.
df_fecha['Id_Fecha'] = df_fecha[ts].dt.strftime('%Y%m%d')

# Descomposición de la fecha en atributos analíticos.
df_fecha['Anio'] = df_fecha[ts].dt.year
df_fecha['Mes'] = df_fecha[ts].dt.month
df_fecha['Dia'] = df_fecha[ts].dt.day
df_fecha['Hora'] = df_fecha[ts].dt.hour
df_fecha['Minuto'] = df_fecha[ts].dt.minute

# Evitar múltiples filas para la misma fecha.
df_fecha = df_fecha.drop_duplicates(subset=['Id_Fecha'])
```

La columna `Id_Fecha` se genera a partir de la fecha en formato `AAAAMMDD`, lo que permite utilizarla como clave de la dimensión. A su vez, los atributos `Anio`, `Mes`, `Dia`, `Hora` y `Minuto` permiten realizar agrupaciones y consultas analíticas por componentes temporales.

La operación `drop_duplicates()` elimina las filas que comparten el mismo identificador de fecha, de modo que se conserve una sola fila por día.

**Consideración técnica:** aunque se conservan los atributos de hora y minuto, la clave generada identifica únicamente el día. Por lo tanto, si se requiere una dimensión con una fila por instante o minuto, será necesario ajustar la clave y el criterio de eliminación de duplicados.

### 5.4. Carga de los datos

La etapa de carga incorpora los datos transformados al almacén de datos técnico de SQL Server. Para ello, el proceso utiliza SQLAlchemy para facilitar el acceso desde Pandas y PyODBC para ejecutar las operaciones SQL directamente sobre la conexión.

#### 5.4.1. Configuración de la conexión

Los parámetros de conexión se obtienen de variables de entorno, lo que permite mantener la configuración separada del código fuente.

```python
from sqlalchemy.engine import URL

host, port = SQL_SERVER.split(',', 1)

connection_url = URL.create(
    "mssql+pyodbc",
    username=SQL_USER,
    password=SQL_PASSWORD,
    host=host,
    port=int(port),
    database=SQL_DATABASE,
    query={
        "driver": "ODBC Driver 17 for SQL Server",
        "TrustServerCertificate": "yes"
    }
)

engine = create_engine(connection_url)

conn_str = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    f"SERVER={SQL_SERVER};"
    f"DATABASE={SQL_DATABASE};"
    f"UID={SQL_USER};"
    f"PWD={SQL_PASSWORD};"
    "TrustServerCertificate=yes;"
)

conn = pyodbc.connect(conn_str)
cursor = conn.cursor()
```

La función `URL.create()` construye la URL de conexión utilizada por SQLAlchemy. A partir de esta configuración, `create_engine()` crea el motor de conexión empleado por Pandas al escribir los DataFrames en SQL Server.

Por su parte, PyODBC establece una conexión directa mediante el controlador ODBC de SQL Server y proporciona un cursor que permite ejecutar sentencias SQL, incluyendo operaciones `MERGE`.

La opción `TrustServerCertificate=yes` permite confiar en el certificado del servidor sin realizar su validación completa. Su uso debe evaluarse según el entorno y los requisitos de seguridad de la conexión.

#### 5.4.2. Carga de las dimensiones mediante staging y MERGE

Para cargar las dimensiones se utiliza una función reutilizable denominada `cargar_dimensiones()`. Esta función recibe un DataFrame, el nombre de la tabla de destino y el nombre de su clave primaria.

El procedimiento utiliza una tabla de staging como área temporal de preparación antes de integrar los datos en la dimensión definitiva.

```python
def cargar_dimensiones(df, destino, pk):
    if df.empty:
        return

    staging_tabla = f"Staging_{destino}"

    df.to_sql(
        staging_tabla,
        con=engine,
        if_exists='replace',
        index=False
    )

    columnas = ", ".join(df.columns)
    valores_source = ", ".join(
        [f"source.{col}" for col in df.columns]
    )

    columnas_a_actualizar = [
        f"target.{col} = source.{col}"
        for col in df.columns
        if col != pk
    ]

    actualizar_sets = ", ".join(columnas_a_actualizar)

    if actualizar_sets:
        match_clause = (
            f"WHEN MATCHED THEN UPDATE SET {actualizar_sets}"
        )
    else:
        match_clause = ""

    merge_query = f"""
        MERGE {destino} AS TARGET
        USING {staging_tabla} AS SOURCE
        ON TARGET.{pk} = SOURCE.{pk}
        {match_clause}
        WHEN NOT MATCHED THEN
            INSERT ({columnas})
            VALUES ({valores_source});
    """

    cursor.execute(merge_query)
    conn.commit()

    print(f"DIMENSION {destino} CARGADA")
```

La función realiza las siguientes operaciones:

1. Comprueba si el DataFrame contiene registros. Si está vacío, termina la operación sin realizar la carga.
2. Escribe los datos en una tabla de staging utilizando `to_sql()`.
3. Construye una sentencia `MERGE` para comparar los registros de staging con los de la dimensión de destino.
4. Si existe una coincidencia en la clave primaria, actualiza los atributos del registro existente.
5. Si no existe una coincidencia, inserta un nuevo registro.
6. Confirma la transacción mediante `conn.commit()`.

La condición de coincidencia utiliza la clave primaria indicada mediante el parámetro `pk`. De esta forma, la operación integra los registros de staging en la dimensión de destino sin insertar nuevamente las claves que ya existen.

**Importante:** `if_exists='replace'` reemplaza la tabla de staging en cada ejecución; no reemplaza la dimensión definitiva. La tabla de destino conserva sus registros y es actualizada mediante `MERGE`.

Finalmente, la dimensión de fecha se carga mediante la siguiente llamada:

```python
cargar_dimensiones(df_fecha, "Dim_Fecha", "Id_Fecha")
```

Esta instrucción envía el DataFrame `df_fecha` a la función de carga, identifica `Dim_Fecha` como tabla de destino y establece `Id_Fecha` como clave utilizada para comparar los registros.


### 5.4.3. Carga de la tabla de hechos

Una vez preparadas las dimensiones, el proceso ETL realiza la carga de la tabla de hechos `Fact_ApiLogs`, que almacena los registros de auditoría y sus relaciones con las dimensiones del almacén de datos técnico.

Al igual que en la carga de las dimensiones, se utiliza una tabla de staging denominada `Staging_Fact_ApiLogs`. Esta tabla recibe temporalmente los registros transformados antes de incorporarlos a la tabla de hechos definitiva.

El siguiente fragmento representa el proceso de carga:

```python
# Utilizar una tabla de staging para preparar los registros de hechos.
if not df_hechos.empty:
    df_hechos.to_sql(
        "Staging_Fact_ApiLogs",
        con=engine,
        if_exists="replace",
        index=False
    )

    insert_hechos_query = """
        INSERT INTO Fact_ApiLogs (
            TraceId, Level, Environment, ClientIp,
            UserId, UserName, Rol, Device,
            Id_Fecha, Id_Request, Id_Response,
            Id_Excepcion, ElapsedMilliseconds
        )
        SELECT
            source.TraceId, source.Level, source.Environment,
            source.ClientIp, source.UserId, source.UserName,
            source.Rol, source.Device, source.Id_Fecha,
            source.Id_Request, source.Id_Response,
            source.Id_Excepcion, source.ElapsedMilliseconds
        FROM Staging_Fact_ApiLogs AS source
        WHERE NOT EXISTS (
            SELECT 1
            FROM Fact_ApiLogs AS target
            WHERE target.TraceId = source.TraceId
        );
    """

    cursor.execute(insert_hechos_query)
    conn.commit()

print("CARGA DEL HECHO FINALIZADO")

cursor.close()
conn.close()

print("ETL finalizado")
```

El procedimiento de carga comprende las siguientes operaciones:

1. **Preparación de staging:** si `df_hechos` contiene registros, Pandas los escribe en `Staging_Fact_ApiLogs`. La opción `if_exists="replace"` reemplaza la tabla de staging existente antes de escribir los datos de la ejecución actual.

2. **Inserción selectiva:** se ejecuta una sentencia `INSERT INTO ... SELECT` que transfiere los registros desde staging hacia `Fact_ApiLogs`.

3. **Prevención de duplicados:** la cláusula `WHERE NOT EXISTS` comprueba si ya existe un registro con el mismo `TraceId` en la tabla de hechos. Solo se insertan los registros cuyo identificador no se encuentra en el destino.

4. **Confirmación de la transacción:** mediante `conn.commit()` se confirman los cambios realizados en SQL Server.

5. **Liberación de recursos:** al terminar la carga, se cierran el cursor y la conexión de PyODBC.

A diferencia de la carga de las dimensiones, que utiliza `MERGE` para insertar y actualizar registros, la carga de hechos descrita utiliza una inserción selectiva. Por tanto, si un `TraceId` ya existe, el registro no se actualiza mediante este fragmento.

**Consideración técnica:** esta estrategia presupone que `TraceId` identifica de manera única cada registro de hechos. Si pueden existir varios registros legítimos con el mismo identificador, será necesario definir una clave de negocio o una clave única que represente correctamente cada evento. Para evitar duplicados también en ejecuciones concurrentes, conviene respaldar la lógica con una restricción única en SQL Server.

### 6. Configuración del entorno

El proceso utiliza variables de entorno para configurar las conexiones con MongoDB y SQL Server. Esta configuración permite separar las credenciales y los parámetros de infraestructura del código fuente.

El proyecto dispone de un archivo de ejemplo denominado `.env.example`, que documenta las variables requeridas sin incluir credenciales reales.

#### 6.1. Variables de entorno

Ejemplo de configuración:

```dotenv
MONGO_URI=mongodb://usuario:password@host:puerto/
LOGS_DB=nombre_bd_mongo
MONGO_COLLECTION=nombre_coleccion

SQL_SERVER=host_sql_server,puerto
SQL_DATABASE=nombre_bd_sql
SQL_USER=usuario_sql
SQL_PASSWORD=password_sql
```

Descripción de las variables:

| Variable | Descripción |
|---|---|
| `MONGO_URI` | Cadena de conexión utilizada para acceder a MongoDB. |
| `LOGS_DB` | Nombre de la base de datos de MongoDB que contiene los registros. |
| `MONGO_COLLECTION` | Colección de la que se extraen los documentos de auditoría. |
| `SQL_SERVER` | Host y puerto de la instancia de SQL Server. |
| `SQL_DATABASE` | Nombre del almacén de datos técnico de destino. |
| `SQL_USER` | Usuario empleado para conectarse a SQL Server. |
| `SQL_PASSWORD` | Contraseña asociada al usuario de SQL Server. |

Los valores del ejemplo deben sustituirse por los parámetros correspondientes al entorno donde se ejecutará el proceso. El archivo `.env` real debe mantenerse fuera del control de versiones cuando contiene credenciales.

### 7. Construcción del contenedor Docker

El proceso ETL se ejecuta dentro de un contenedor basado en Python. La imagen instala las dependencias del sistema necesarias para utilizar el controlador ODBC de Microsoft y las bibliotecas de Python declaradas en el archivo `requirement.txt`.

El `Dockerfile` define la imagen y las instrucciones de construcción del entorno de ejecución.

```dockerfile
FROM python:3.14-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV ACCEPT_EULA=Y

WORKDIR /app

# Instalar dependencias del sistema y configurar el repositorio de Microsoft.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        gnupg \
        g++ \
        unixodbc-dev \
    && curl -fsSL https://packages.microsoft.com/keys/microsoft.asc \
        | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg \
    && echo "deb [arch=amd64,arm64 signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" \
        > /etc/apt/sources.list.d/microsoft-prod.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql17 \
    && rm -rf /var/lib/apt/lists/*

# Instalar las dependencias de Python.
COPY requirement.txt .
RUN pip install --no-cache-dir -r requirement.txt

# Incorporar el script ETL.
COPY etl_script.py .

CMD ["python", "-u", "etl_script.py"]
```

La construcción de la imagen se realiza mediante las siguientes etapas:

1. Se utiliza una imagen base de Python sobre Debian Bookworm.
2. Se instalan las dependencias del sistema y el controlador ODBC de Microsoft.
3. Se copian e instalan las bibliotecas de Python.
4. Se incorpora el script `etl_script.py`.
5. Se establece el comando predeterminado para ejecutar el proceso ETL.

La variable `PYTHONUNBUFFERED=1` permite que los mensajes de salida se muestren sin el almacenamiento intermedio habitual de Python, facilitando el seguimiento de la ejecución mediante los registros del contenedor.

### 8. Orquestación mediante Docker Compose

Docker Compose permite definir la construcción y ejecución del servicio ETL, así como las variables de entorno y la conectividad necesaria para acceder a servicios que se ejecutan en el host.

El archivo `compose.yaml` utiliza la siguiente configuración:

```yaml
services:
  etl:
    build:
      context: .
      dockerfile: Dockerfile
    container_name: etl_container
    env_file:
      - .env
    extra_hosts:
      - "host.docker.internal:host-gateway"
```

La configuración contiene los siguientes elementos:

- **`build`:** indica que la imagen se construye a partir del `Dockerfile` ubicado en el directorio del proyecto.
- **`container_name`:** establece el nombre del contenedor.
- **`env_file`:** carga las variables de conexión definidas en `.env`.
- **`extra_hosts`:** permite resolver el nombre `host.docker.internal` hacia la puerta de enlace del host en entornos Docker que requieren esta configuración.

Este último parámetro resulta útil cuando MongoDB o SQL Server se ejecutan directamente en el equipo anfitrión. Si las bases de datos se ejecutan en otros contenedores, deberán configurarse las redes y los nombres de servicio correspondientes.

### 9. Ejecución del proceso ETL

Desde el directorio que contiene el archivo de composición, se puede ejecutar el proceso mediante:

```bash
docker compose run --build --rm etl
```

El comando realiza las siguientes acciones:

1. Construye o actualiza la imagen del servicio.
2. Crea un contenedor para ejecutar el ETL.
3. Ejecuta el comando definido en el `Dockerfile`.
4. Elimina el contenedor al finalizar gracias a la opción `--rm`.

La ejecución debe realizarse con el archivo `.env` correctamente configurado y con conectividad disponible hacia MongoDB y SQL Server.

Los mensajes de salida permiten observar las etapas informadas por el script. Para considerar exitosa una ejecución, además de alcanzar el mensaje final, se deben comprobar los registros cargados en las tablas de destino y verificar que no existan errores de conexión, transformación o integridad referencial.

### 10. Cierre del proceso

Al finalizar la carga de las dimensiones y de la tabla de hechos, el script cierra los recursos de conexión y muestra el mensaje de finalización.

El resultado esperado es un almacén de datos técnico actualizado con los registros de auditoría procesados, preparado para las siguientes etapas de análisis multidimensional y elaboración de consultas OLAP.

La validación final debe incluir la comprobación de los registros cargados, la correspondencia entre claves dimensionales y hechos, y la ausencia de duplicados según la clave definida para cada registro.


### 5.5. Resumen del flujo

El flujo documentado hasta esta etapa puede resumirse de la siguiente manera:

1. El middleware de la API registra información de las peticiones HTTP.
2. Los registros se almacenan como documentos en MongoDB.
3. Python y PyMongo extraen los documentos de la colección.
4. Pandas normaliza los documentos y prepara los DataFrames.
5. Los datos transformados se escriben en tablas de staging de SQL Server.
6. Las dimensiones se integran mediante operaciones `MERGE`.
7. Los datos quedan disponibles en el almacén de datos técnico para las etapas posteriores de análisis.

**Nota de alcance:** este documento describe los fragmentos principales del proceso. Las dimensiones restantes, la carga de la tabla de hechos, las validaciones y el procedimiento completo de ejecución deberán documentarse conforme a la implementación definitiva del ETL.

