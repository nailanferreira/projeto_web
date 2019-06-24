from pymongo import MongoClient
from lib import DadosAbertos
import schedule
import time
import os
import sys

def coleta():


   # hostname do mongodb, consultado via variavel de ambiente
   server_mongo = 'mongodb'

   # Conexao ao mongoDB
   conn = MongoClient(server_mongo, 27017)

   # Conexao ao database
   banco = conn['projetoDep']

   # Conexao a tabela de banco de dados
   table = banco['deputados']

   # Array que vai receber os dados do deputados
   list_deputados = []

   #Conexao com api dos dados publicos
   obj = DadosAbertos()

   # Listando os deputados
   list_dep = obj.deputados()
    
   ####################################### 

   x = table.find({}, {"_id": 1})
   lista_id_deputados = []
   for item in x:
       lista_id_deputados.append(item['_id'])

   dicionario_discursos = {}
   for id_deputado in lista_id_deputados:
       dicionario_discursos[id_deputado] = len(obj.deputado_discursos(id_deputado))

   #########################################

   for dep in list_dep:
       info = {
             '_id'    : dep['id'],
             'Nome'   : dep['nome'],
             'Partido': dep['siglaPartido'],
             'Foto'   : dep['urlFoto'],
             'qtd_discursos': dicionario_discursos[dep['id']]
       }
       list_deputados.append(info)
   # Inserindo dados no mongodb
   retorno = table.insert_many(list_deputados)
   print(retorno)

#Criando o schedule
schedule.every().day.at("10:30").do(coleta)
schedule.every().minute.at(":17").do(coleta)


while True:
   schedule.run_pending()
   time.sleep(1)
