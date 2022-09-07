from flask import Flask, render_template,jsonify,request
import psycopg2

app = Flask(__name__)
app.config["DEBUG"] = True
con = psycopg2.connect(database="60-Project",user="postgres",password="nivos",host="127.0.0.1",port="5432")
cursor = con.cursor()

@app.route("/url_list",methods=['post','get'])
def url_list():
    cursor.execute('SELECT path FROM public."real"')
    result = cursor.fetchall()
    return jsonify(result)
@app.route("/url_info",methods=['post','get'])
def url_info():
    if 'url' in request.args:
        url = str(request.args['url'])
    else:
        return "Error: No info on URL"
    final = []
    cursor.execute('SELECT * FROM public."real"')
    result = cursor.fetchall()
    for i in range(len(result) - 1):
        if result[i][0] == url:
            final.append(result[i])
    # return jsonify(f"status code:{final[0][1]}, last update:{final[0][2]}")
    return jsonify(final)

@app.route("/test_status",methods=['post','get'])
def test_status():
    cursor.execute('SELECT * FROM public."real"')
    result = cursor.fetchall()
    final = []
    for i in range(len(result) - 1):
        if "test" in result[i][0]:
            final.append(result[i])
    return jsonify(final)
@app.route("/show_null",methods=['post','get'])
def show_null():
    cursor.execute('SELECT path FROM public."real" WHERE status_code IS NULL')
    result = cursor.fetchall()
    return jsonify(result)
@app.route("/add",methods=['post','get'])
def add():
    if 'url' in request.args:
        url = str(request.args['url'])
    else:
        return "Error: Please insert URL to add"
    try:
        cursor.execute(f"INSERT INTO public.\"real\" VALUES ('{url}', NULL, NULL);")
        con.commit()
        return jsonify(f"{url} was added!")
    except Exception:
        return jsonify("Duplicate URL")

@app.route("/delete",methods=["DELETE"])
def delete():
    if 'url' in request.args:
        url = str(request.args['url'])
    else:
        return "Error: Please insert URL to delete"
    cursor.execute(f"DELETE FROM public.\"real\" WHERE path = '{url}'")
    con.commit()
    return jsonify(f"{url} deleted")
@app.route("/update_status",methods=['post','get'])
def update_status():
    if 'url' in request.args:
        url = str(request.args['url'])
    else:
        return "Error: Please insert URL to update"
    if 'status_code' in request.args:
        status_code = str(request.args['status_code'])
    else:
        return "Error: Please insert URL to update"
    cursor.execute(f"UPDATE public.\"real\" SET status_code = '{status_code}' WHERE path = '{url}';")
    con.commit()
    return jsonify(f"{url} status code was updated to {status_code}!")
app.run()