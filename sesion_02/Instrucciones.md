# Ejercicios PySpark en Databricks

---

## Ejercicio 1 — Lectura de CSV con esquema definido y formato de fecha

### Contexto

Se te proporciona el siguiente dataset en formato **CSV** con **50 registros** y **6 campos**. El archivo contiene información de ventas con una columna de fecha en formato `dd/MM/yyyy`. Tu tarea es subirlo al volume de Databricks y leerlo correctamente usando PySpark.

---

### Instrucciones

**Paso 1 — Subir el archivo al volume**

Sube el archivo `ventas.csv` al volume de Databricks llamado **`vol_landing`**. Puedes hacerlo desde la interfaz de Databricks:

> `Catalog` → `[Tu catálogo]` → `[Tu schema]` → `vol_landing` → botón **Upload**

La ruta del archivo quedará similar a:

```
/Volumes/<catalogo>/<schema>/vol_landing/ventas.csv
```

---

**Paso 2 — Definir el esquema**

En PySpark, **debes definir explícitamente el esquema** del dataset usando `StructType` y `StructField`. No uses inferencia automática de esquema (`inferSchema=True`).

```python
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType

# TODO: Completa el esquema con todos los campos del CSV
schema = StructType([
    StructField("___", ___Type(), ___),
    StructField("___", ___Type(), ___),
    StructField("___", ___Type(), ___),
    StructField("___", ___Type(), ___),
    StructField("___", ___Type(), ___),
    StructField("___", ___Type(), ___),
])
```

> 💡 **Pista:** Identifica qué tipo de dato corresponde a cada columna: `id_venta`, `producto`, `categoria`, `cantidad`, `precio_unitario` y `fecha_venta`.

---

**Paso 3 — Leer el CSV con la opción `dateFormat`**

El campo `fecha_venta` está en formato `dd/MM/yyyy`. Para que PySpark lo interprete correctamente como `DateType`, **debes usar la opción `dateFormat`** al momento de la lectura, agregala con el formato correcto.

```python
# TODO: Define la ruta correcta a tu volume
ruta_archivo = "/Volumes/<catalogo>/<schema>/vol_landing/ventas.csv"

# TODO: Completa las opciones de lectura
df_ventas = spark.read \
    .format("csv") \
    .option("header", ___) \
    .schema(schema) \
    .load(ruta_archivo)

df_ventas.display()

```

---

**Paso 4 — Validaciones**

Una vez leído el archivo, responde las siguientes preguntas usando transformaciones PySpark:

1. ¿Cuántos registros tiene el DataFrame?
2. ¿Cuál es el total de ingresos por categoría? *(Pista: `cantidad * precio_unitario`)*
3. ¿Cuál es la fecha de venta más reciente y la más antigua del dataset?
4. Filtra únicamente las ventas del mes de **febrero de 2024**.

---

## Ejercicio 2 — Lectura de JSON Multilínea con esquema definido

### Contexto

Se te proporciona un archivo **JSON de 50 registros** con estructura **multilinea**. Este tipo de archivo no puede leerse con la configuración por defecto de PySpark. Tu tarea es subirlo al volume y leerlo correctamente usando la opción `multiLine`.

---

### Instrucciones

**Paso 1 — Subir el archivo al volume**

Sube el archivo `empleados.json` al volume de Databricks llamado **`vol_landing`**. Puedes hacerlo desde la interfaz de Databricks:

> `Catalog` → `[Tu catálogo]` → `[Tu schema]` → `vol_landing` → botón **Upload**

La ruta del archivo quedará similar a:

```
/Volumes/<catalogo>/<schema>/vol_landing/empleados.json
```

---

**Paso 2 — Definir el esquema**

En PySpark, **debes definir explícitamente el esquema** del dataset usando `StructType` y `StructField`. No uses inferencia automática de esquema.

```python
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, BooleanType

# TODO: Completa el esquema con todos los campos del JSON
schema = StructType([
    StructField("___", ___Type(), ___),
    StructField("___", ___Type(), ___),
    StructField("___", ___Type(), ___),
    StructField("___", ___Type(), ___),
    StructField("___", ___Type(), ___),
    StructField("___", ___Type(), ___),
])
```

> 💡 **Pista:** Revisa los campos del JSON: `id_empleado`, `nombre`, `departamento`, `cargo`, `salario` y `activo`. Presta atención al tipo booleano.

---

**Paso 3 — Leer el JSON con la opción `multiLine`**

El archivo JSON es de tipo **array multilinea** (comienza con `[` y cierra con `]`). PySpark, por defecto, espera un JSON por línea (formato JSONL). Si intentas leerlo sin la opción correcta, obtendrás un error o un DataFrame vacío.

**Debes usar la opción `multiLine` para leerlo correctamente.**

```python
# TODO: Define la ruta correcta a tu volume
ruta_archivo = "/Volumes/<catalogo>/<schema>/vol_landing/empleados.json"

# TODO: Completa las opciones de lectura
df_empleados = spark.read \
    .format("json") \
    .schema(schema) \
    .load(ruta_archivo)

df_empleados.display()
```

---

**Paso 4 — Validaciones**

Una vez leído el archivo, responde las siguientes preguntas usando transformaciones PySpark:

1. ¿Cuántos empleados están activos y cuántos inactivos?
2. ¿Cuál es el salario promedio por departamento?
3. ¿Cuál es el empleado con el salario más alto de todo el dataset?
4. Lista todos los empleados del departamento de **Tecnología** ordenados por salario de mayor a menor.