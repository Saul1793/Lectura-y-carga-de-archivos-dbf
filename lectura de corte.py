from os import getcwd
import subprocess
import requests
from dbfread import DBF
import os.path as path
from datetime import datetime
from dateutil.relativedelta import relativedelta

ruta=getcwd()
ruta=(ruta.split('\\')[0],ruta.split('\\')[1])
ubicacion="\\".join(ruta)
archivo =f"{ubicacion}\\xcorte.dbf"
fecha=datetime.now().date()
fecha2=fecha-relativedelta(days=1)


cadena="""Hola y buenos dias
me podrian ayudar a realizar su corte del dia"""

if path.exists(archivo):
    print(f'si existe el archivo en: {ubicacion}')
    #< fecha.date() - relativedelta(days=1)
    try:
        db=DBF(f'{archivo}',encoding='latin1')
        with db as table:
            registros=[]
            tienda=[]
            for record in table:
                registros.append(record['FECHA'])
                tienda.append(record['TIENDA'])

            fecha_final=registros[-1]
            val_tienda = tienda[-1] if tienda[-1] !='' else tienda[-2]
            if fecha_final < fecha2:
                with open('nota.txt', 'w') as nota:
                    nota.write(cadena)

                subprocess.Popen(['notepad.exe', 'nota.txt'], creationflags=subprocess.CREATE_NO_WINDOW, close_fds=True)
                estado='No_corto'
            else:
                print('se envian datos de corte')
                estado='Si_corto'
    except Exception  as e:
        print(f"el error es {e}")
        estado='hay_error'

    finally:
        # Definir la URL con los parámetros incluidos
        print(val_tienda)
        print(estado)
        url = fr'https://rbf.camposreyeros.com/saulContreras?tienda={val_tienda}&corte={estado}'
        print(url)

        # Realizar la solicitud GET
        respuesta = requests.get(url, verify=False)# se coloca verify false para que no marque erro por certificado

        # Verificar el estado de la solicitud
        if respuesta.status_code == 200:
            print('Solicitud exitosa')
            print('Contenido de la respuesta:')
            print(respuesta.text)
        else:
            print('Error en la solicitud:', respuesta.status_code)
            print(respuesta.text)
else:
    print('La base Xcorte no existe')





