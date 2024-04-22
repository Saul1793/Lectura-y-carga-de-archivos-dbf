#instalar las librerias necesarias para el trabajo
import pandas as pd
import os
from sqlalchemy import text
from sqlalchemy import Date
from dbfread import DBF
import time
from datetime import date
import concurrent.futures
from sqlalchemy import create_engine
from sqlalchemy import URL
from datetime import datetime
from dateutil.relativedelta import relativedelta

#se crean mediante una tupla los valores para conectar a la base de datos
url_de_basesql = URL.create(
    "nombre de motos de base de datos",
    username="nombre de usuario",
    password="contraseña",
    host="ip",
    database="nombre de base de datos",)

def motor(opcion):
    engine = create_engine(url_de_basesql)
    if opcion=='conectar':

        return engine
    elif opcion=='desconectar':

        return engine.dispose()

#con esta funcion podemos calcular los dias a partir de cuando vamos a realizar los calculos o carga de datos
def fecha_autom(dias):
    x=(datetime.now())-relativedelta(days=dias)
    x=x.date().strftime('%Y%m%d')
    return x



inicio=time.time()
# Rutas de origen y destino
origen = "" #ruta de origen
# con esta funcion se establece cuantos dias atras se concentrara
fecha_ini=fecha_autom(5)

#se crean variables para crear el diccionario
plazas = []
rutas = []
proceso={}
# Recorremos los archivos en el directorio de origen para mi caso fueron tres niveles a buscar
for carpetas in os.listdir(origen):
    carpetas=os.path.join(origen,carpetas)
    for directorio in os.listdir(carpetas):
        carpeta2=os.path.join(carpetas,directorio)
        if os.path.isdir(carpeta2):
            plazas.append(directorio)
            for tienda in os.listdir(carpeta2):
                carpeta3=os.path.join(carpeta2,tienda)
                ubicacion=carpeta3.split('\\')
                rutas.append(carpeta3)
# se agregan los valores keys al diccionario
for plaza in plazas:
    proceso[plaza]=[]
# se agregan los valores a values a los keys ya insertados si la variable ruta sergmentada por \\ se evalua y se asigna a lña key correspondiente
for ruta in rutas:
    if ruta.split('\\')[3] in proceso:
        proceso[ruta.split('\\')[3]].append(ruta)


# se crea funcion para insertar los datos por cada plaza es decir se abre una ventana de insercion por plaza
def muestra_rutas(plaza):
    print(f"Abriendo proceso para el elemento: {plaza}")
    inicio=time.time()
    for valor in plaza:
        # se crea una variable que contenga el texto de la carpeta 3 para poder obtener despues la plaza y la tienda
        ubicacion=valor.split('\\')
        # se intenta leer el dbf xcorte con un filtro de fecha
        try:
            print(valor)
            db=DBF(f'{valor}\\Nombre de archivo dbf.DBF',encoding='latin1')
            #para no mantener abierto el dbf se usa el metodo with
            with db as table:
                #se carga el dbf a un dataframe ya con el filtro mayor  ala fecha  y la fecha se extrae por indexing
                df_filtrado=[record for record in table if record['FECHA'] >= date(int(fecha_ini[0:4]),int(fecha_ini[4:6]),int(fecha_ini[6:8]))]
                df = pd.DataFrame(df_filtrado, columns=table.field_names, index=None)
                if df['FECHA'].iloc[-1] >= date(int(fecha_ini[0:4]), int(fecha_ini[4:6]), int(fecha_ini[6:8])):
                    #agrego las columnas de tienda y plaza al df opcional
                    df['ctienda']=ubicacion[4]
                    df['plaza']=ubicacion[3]
                    # Copiar los encabezados de las columnas en minúsculas
                    df.columns = map(str.lower, df.columns)
                    try:
                        #una vez listo el df borro primero el registro de la tabla sql en postgresql a partir de la fecha
                        query = f"""se corre una consulta para borrar datos o sobrescribir estos from tabla where campo_fecha >='{fecha_ini}'and ctienda='{ubicacion[4]}' and plaza ='{ubicacion[3]}'"""
                        conectar = motor('conectar').connect()
                        conectar.execute(text(query))
                        conectar.commit()
                        # despues de borrado cargo los datos de la tienda

                    except Exception as e:
                        print(f'el error fue {e}')
                        conectar.close()
                        motor('desconectar')
                    finally:
                        conectar.close()
                        motor('desconectar')
                    try:
                        # Cargar los datos en la tabla, especificando el tipo de datos 'Date' para la columna dcampo6
                        df.to_sql('nombre de la tabla', motor('conectar'), if_exists='append', index=False,dtype={'fecha': Date})

                    except Exception as e:
                        print(f'el error fue {e}')
                        conectar.close()
                        motor('desconectar')
                    finally:
                        conectar.close()
                        motor('desconectar')
                else:
                    pass
        except Exception as e:
            print(f'el error fue {e}, {ubicacion[4], ubicacion[3]}')
            pass
    fin=time.time()
    tiempo = (fin - inicio) / 60
    print(f'el tiempo de ejecucion fue de {tiempo:.2f}')



# con este comando abro un proceso de carga por plaza
with concurrent.futures.ThreadPoolExecutor() as executor:
    # Mapear la función imprimir_elemento a cada elemento de la lista en paralelo
    executor.map(muestra_rutas,proceso.values())

#al finalizar todos los procesos de carga se ejecuta la funcion que crea el resumen de xcorte con las fechas maximas de las tiendas
try:
    query = f"""se ejecuta una funcion mas previamente configurada en mi caso en postgresql"""
    conectar = motor('conectar').connect()
    conectar.execute(text(query))
    conectar.commit()

except Exception as e:
    print(f'el error fue {e}')
    conectar.close()
    motor('desconectar')
finally:
    conectar.close()
    motor('desconectar')


fin=time.time()
tiempo=(fin-inicio)/60
print(f'el tiempo de ejecucion fue de {tiempo:.2f}')
