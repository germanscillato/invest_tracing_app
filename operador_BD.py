"""
Armar LOGGING

Generalizar, no hacer 1 clase para cada tabla, sino ver como generalizar con heredar,
args o kwargs


DISEÑO : 1 tabla por fuente de datos.
Generar una tabla completa con datos de portafolio global



"""
# IMPORTANCIONES TEMPORALES; BORRAR una vez debug
from email import header
from wsgiref import headers
from scraper import Source_PPI, Source_IOL

####
import json

from tkinter import messagebox
import datetime

from sqlalchemy import create_engine, update

from sqlalchemy.orm.session import sessionmaker
from sqlalchemy.sql.expression import column
from sqlalchemy.sql import func, desc
from sqlalchemy.sql.schema import PrimaryKeyConstraint
from sqlalchemy.sql.sqltypes import Date, DateTime
from sqlalchemy import Column, Integer, String, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

import pandas as pd
from tabulate import tabulate
import os


# ELIJe EL DIRECTORIO ACTUAL DE TRABAJO.
BD_path = 'sqlite:///' + os.getcwd() + '\BD.db'

try:
    engine = create_engine(
        BD_path,
        echo=True)

except Exception as e:
    print(e)
    messagebox.showinfo("Error", e)





class Agregar_externos():
    """A utilizar 1 vez, actualizar valores solamente"""

    def __init__(self):

        # With statement close connection. Oficial docs. 
        # https://docs.sqlalchemy.org/en/13/core/connections.html
        try: 
        
            with engine.connect() as conn:
                
                df = pd.read_csv("usd_ext.csv",
                            sep=";",
                            dtype={
                                0: str,
                                1: float,
                                2: str,
                                3: str
                            })  # sino los importa como str,parse_dates=True

                sqlite_table = "usd_ext"
            

                df.to_sql(name=sqlite_table, con=conn, if_exists='fail')
        except Exception as e:
            print(e) 

        


################ ON #########################
"""
class Cot_usd_BD():

    def persistir(self, df):

        try:

            session = Session()
            for i in range(0, len(df)):

                bd = Tabla_Cotizacion_Dolar()
                bd.Moneda = df.iloc[i, 0]
                bd.Precio_compra = df.iloc[i, 1]
                bd.Precio_venta = df.iloc[i, 2]
                bd.fecha = pd.to_datetime(df.iloc[i, 3])

                session.add(bd)

            session.commit()
            session.close()

        except Exception as e:
            print(e)
            messagebox.showinfo("Error", e)


class Leer_portafolio():

    def dataframe(self):

        try: 
        
            with engine.connect() as conn:



                df = pd.read_sql('SELECT * FROM Portfolio_api', conn)
    
                return df

        except Exception as e:
            messagebox.showinfo("Error", e)
"""

class Dataframe_BD():
    """ Gestión de datos en DF a BD sqlite3. Para persistir DF, ingresar df y nombre. 
    Realizará append si la tabla ya existe, sino crea la tabla.
    No persiste en BD tiempo de registro
    Para leer DF
    
    Optimizaciones: Query operations haga el doble index
    """
    def __init__(self):

        self.query_list ={
            "operaciones" : '''SELECT tipo,
                            simbolo,
                            fechaOperada,
                            SUM(cantidadOperada),
                            AVG(precioOperado)
                            FROM operaciones_IOL
                            WHERE estado = 'terminada' AND tipo IN ('Compra' , 'Venta')
                            GROUP BY "cantidadOperada", "precioOperado"
                            ORDER BY fechaOperada ASC

                            ''',
            "ticker_Location":'''SELECT DISTINCT ticker
                                FROM insertar_tabla
                                ORDER BY ticker ASC''',
            "price" : '''SELECT ticker, AVG(ultimo_precio), fecha
                        FROM insertar_tabla
                        WHERE ticker IN ticker_list 
                        GROUP BY fecha ,ticker
                        ORDER BY fecha ASC'''
            }
    def str_to_date(self,x):
        """func para convertir str to date en index de dataframe con doble index"""
        return datetime.datetime.fromisoformat(str(x)).date()
    
    
    def persistir_df(self, df, table_name):
        try:
            
        
            with engine.connect() as conn:
                df.to_sql(name=table_name,
                        con=conn,
                        if_exists='append',
                        index=False)
            engine.dispose()    

        except Exception as e:
            print(e)

    def leer_df_basico(self, table_name):
        """ Ingresar nombre tabla. Trae tabla completa sin query"""
        try:
            with engine.connect() as conn:

                self.df = pd.read_sql(sql=table_name, con=conn)

                return self.df
        except Exception as e:
            print(e)



    def table_query(self,query):
        """ Ingresar query a solicitar del listado de query definido en este metodo
        operaciones, ticker_location,......"""
        
        try:
            with engine.connect() as conn:

                self.df = pd.read_sql_query(sql=query, con=conn)

                return self.df
        except Exception as e:
            print(e)

    def ticker_loc_json(self):
        """Trae los ticker UNICOS y graba en que tabla estan
        Realiza loop modificando nombre de la tabla en la query "ticker_Location"
        Graba ticker_list.json un diccionario: keys son los table_name, y values list con ticker.
        """
        securities = ['bond','options','futures','stock','adr']
        ticker_loc = {}
        for sec in securities:
            query = self.query_list['ticker_Location'].replace("insertar_tabla",sec+"_price")
            # de query tomo la unica columna y la convierto en lista, para poder serializarlo con json
            ticker_loc[sec+"_price"] = list(self.table_query(query).iloc[:,0])
        
        
        try:

            with open('data/ticker_list.json', 'w') as json_file:
                json.dump(ticker_loc, json_file)

        except Exception as e:
            print(e)
            # AGREGA LOG
    
    def ticker_loc_check(self,ticker_price):
        """Lectura ticker_list.json. Chequeo de los ticker en ticker_price, a 
        que tabla de la BD corresponden
        
        VER COMO OPTIMIZAR"""
        try:

            with open('data/ticker_list.json') as json_file:
                ticker_list = json.load(json_file)
        except Exception as e:
            print(e)
        # chequeo de lista de ticker a que tabla corresponden.
        # utilizo set para evitar repetir valores
        table_names = set()
        for ticker in ticker_price:

            for key, tick in ticker_list.items():
                if ticker in tick:
                    table_names.add(key)
        return list(table_names)

        
 
    def format_list_sql(self,lista):
        """COnvierte listado en sentencia sql para chequer in
        ('str1','str2',....)
        """
        sql = str("()")
        n = len(lista)
        for val in lista:
            if n >1:
                sql = sql[:1]+",'" +val +"'"+ sql[1:]
            else:
                sql = sql[:1]+"'" +val +"'"+ sql[1:]
            n -=1
        return sql


    def ticker_price_historico(self,ticker):
        """Con listado de ticker, trae precios historicos,
        Devuelve df con AVG(precios) group by Fecha,ticker 
        Los table_name los saca de la metodo ticker_loc_check que devuelve listado de los table_name
        
        """
        #obtengo listado de table_names
        table_names = self.ticker_loc_check(ticker)
        # Trae query desde definición __init__
        query_list = self.query_list["price"]
        # reemplazo ticker list por listado con formato en SQL para sentencia IN
        query_list = query_list.replace('ticker_list' , self.format_list_sql(ticker))
        #Df vacío para agregar resultados
        df = pd.DataFrame()
        # Iteracion sobre los table_names que contienen los ticker.
        for table in table_names:
            # Reemplaza query_list para evitar
            query = query_list.replace("insertar_tabla",table)
            df = pd.concat([df,self.table_query(query)], axis = 0)
        
        df["fecha"]= df["fecha"].apply(lambda x : self.str_to_date(x))


        return df

if __name__ == "__main__":

    df_bd = Dataframe_BD()
    df = df_bd.ticker_price_historico(["AL29" ,"BA37D" ])
    print("\n",tabulate(df,
    headers = 'keys' ,
    tablefmt = "grid")
      )

    print(type(df.loc[0,"fecha"]),
    df.loc[0,"fecha"])
    
    """

    
    df2 = df_bd.table_query(df_bd.query_list["operaciones"])
    
    print("\n",tabulate(df2,
    headers = 'keys' ,
    tablefmt = "grid")
      )
    
    df2.rename(columns={"SUM(cantidadOperada)": "cantidadOperada" ,
                        "AVG(precioOperado)":"precioOperado" } 
                        ,inplace=True)

    operations = pd.pivot_table(data = df2 ,
            values = ["cantidadOperada" ,"precioOperado" ],
            index = ["fechaOperada" , "simbolo"] ,
            aggfunc = {"cantidadOperada" : "sum" ,
             "precioOperado": "mean"} )
    
    print("\n",tabulate(df2,
    headers = 'keys' ,
    tablefmt = "grid")
      )
    mov_capital =  operations['cantidadOperada']*operations['precioOperado']
        #elimino index de simbolo (ya no aplica por los sume, estan por fecha)
    mov_capital= mov_capital.droplevel(level=1,axis=0)
        # Agrupo por fecha
    mov_capital = mov_capital.groupby(by = "fechaOperada").agg("sum")

    mov_capital.name = "ingresoCapital"
    print(type(mov_capital.index[0]),mov_capital.index[0])
    def str_to_date(x):
        "func para convertir str to date en index de dataframe con doble index"
        return datetime.datetime.fromisoformat(str(x)).date()
    
    index_cap = pd.Series(mov_capital.index)
    mov_capital.index = index_cap. \
    apply(lambda x : str_to_date(x))
    print(mov_capital)
    print(type(mov_capital.index[0]),mov_capital.index[0])
    df_bd.ticker_loc_json()
    res = df_bd.ticker_loc_check(["AL29" ,"BA37D" ])
    print(res)

    """