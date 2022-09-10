from flask import Flask, jsonify, request
import psycopg2

app = Flask(__name__)  # flask instance
app.config["DEBUG"] = True  # debug code and where it went wrong
con = psycopg2.connect(database="60-Project", user="postgres", password="nivos", host="10.0.0.10",
                       port="5432")  # establish connection to database
TABLE_NAME = 'url_list'  # global variable to make changing table names easier
cursor = con.cursor()


@app.route("/db_data", methods=['post', 'get'])
# Get all data from database
def db_data():
    cursor.execute(f'SELECT * FROM public."{TABLE_NAME}"')
    result = cursor.fetchall()
    return jsonify(result)


@app.route("/get_url_list", methods=['post', 'get'])
# Get all the URL's currently on the database
def get_url_list():
    cursor.execute(f'SELECT url FROM public."{TABLE_NAME}"')
    result = cursor.fetchall()
    return jsonify(result)


@app.route("/url_info", methods=['post', 'get'])
# Get info on specific URL (status code and last update)
def url_info():
    if 'url' in request.args:
        url = str(request.args['url'])
    else:
        return "Error: No info on URL"
    cursor.execute(f"SELECT status_code,last_update FROM public.\"{TABLE_NAME}\" WHERE url = '{url}'")
    result = cursor.fetchall()
    return jsonify(result)


@app.route("/test_status", methods=['post', 'get'])
# Show status code and last updated for urls containing test in route
def test_status():
    cursor.execute(f'SELECT url FROM public."{TABLE_NAME}"')
    result = cursor.fetchall()
    route_list = []
    final = []
    for i in range(len(result)):
        if "test" in str(result[i]):
            final.append(result[i])
    return jsonify(final)


@app.route("/show_null", methods=['post', 'get'])
# show URL's which has a null status code
def show_null():
    cursor.execute(f'SELECT url FROM public."{TABLE_NAME}" WHERE status_code IS NULL')
    result = cursor.fetchall()
    return jsonify(result)


@app.route("/add", methods=['post', 'get'])
# Add URL's to database
def add():
    if 'url' in request.args:
        url = str(request.args['url'])
    else:
        return "Error: Please insert URL to add"
    try:
        cursor.execute(f"INSERT INTO public.\"{TABLE_NAME}\" VALUES ('{url}', NULL, NULL);")
        con.commit()
        return jsonify(f"{url} was added!")
    except Exception:
        return jsonify("Duplicate URL")


@app.route("/delete", methods=["DELETE"])
# Delete URL's from database
def delete():
    if 'url' in request.args:
        url = str(request.args['url'])
    else:
        return "Error: Please insert URL to delete"
    cursor.execute(f"DELETE FROM public.\"{TABLE_NAME}\" WHERE url = '{url}'")
    con.commit()
    return jsonify(f"{url} deleted")


@app.route("/update_status", methods=['post', 'get'])
# Update status code and last update
def update_status():
    if 'url' in request.args and 'status_code' in request.args and 'last_updated' in request.args:
        url = str(request.args['url'])
        status_code = str(request.args['status_code'])
        last_update = str(request.args['last_updated'])
    else:
        return "Error: Please insert URL to update"
    cursor.execute(
        f"UPDATE public.\"{TABLE_NAME}\" SET status_code = '{status_code}' , last_update = '{last_update}' WHERE url = '{url}';")
    con.commit()
    return jsonify(f"{url} status code was updated to {status_code} at {last_update}!")


if __name__ == '__main__':
    app.run(host='0.0.0.0',port=80)
