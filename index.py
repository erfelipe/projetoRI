from flask import Flask
from flask import render_template
import elasticPesquisaTermos as pesq
import json

app = Flask(__name__)

@app.route('/')
def inicio():
    return render_template('index.html')

@app.route("/pesquisa/")
def semPesquisa():
    return "Informe um termo para ser pesquisado."

@app.route("/pesquisa/<termo>/") 
def pesquisa(termo): 
    pesquisa = []
    pesquisa.append(termo)
    resp = pesq.iniciaPesquisaEmAmbasTerminologias(pesquisa)
    return json.dumps(resp, indent=4)

if __name__ == "__main__":
    app.run(debug=True) 

