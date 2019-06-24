from flask import Flask, render_template
from pymongo import MongoClient

app = Flask(__name__)


# Conexao ao mongoDB
conn = MongoClient('mongodb', 27017)
# Conexao ao database 
banco = conn['projetoDep']
# Conexao a tabela de banco de dados
table = banco['deputados']


@app.route("/")
def deputados():
   list_dep = []
   for dep in table.find():
       list_dep.append(dep)
   return render_template('lista.html', listas=list_dep)
   

#@app.route("/discursos/<id>")

if __name__ == "__main__":
    app.run(host='127.0.0.1', debug=True, port=8080)