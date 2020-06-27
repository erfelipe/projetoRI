from flask import Flask
from flask import render_template

app = Flask(__name__)

@app.route('/')
def inicio():
    return render_template('index.html')

@app.route("/pesquisa/")
def semPesquisa():
    return "Informe um termo para ser pesquisado."

@app.route("/pesquisa/<termo>/") 
def pesquisa(termo):  
    return '0'


if __name__ == "__main__":
    app.run(debug=True) 

