from flask import Flask, render_template

app = Flask(__name__)

@app.route('/')
def mostrar_html():
    with open('resultados.html', 'r') as f:
        contenido_html = f.read()
    return render_template('resultados.html', contenido_html=contenido_html)

if __name__ == '__main__':
    app.run()