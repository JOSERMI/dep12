from flask import Flask, render_template, request
import requests
app = Flask(__name__)

@app.route('/', methods=['GET', 'POST'])
def index():
    # Establecer la conexión a la base de datos
    src_mysql = "72.167.124.128"
    destino_bigquery = "rogerdat-prj"
    
 
    api_url = "https://list-mysql-tables-2j7nour7pq-uc.a.run.app/"
    response = requests.get(api_url)
    resultados = response.json()

    return render_template('form.html', resultados=resultados, mysql=src_mysql, bigquery=destino_bigquery)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=3000)