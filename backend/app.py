from flask import Flask, request
from flask_cors import CORS, cross_origin
from os import walk
import pandas as pd

app = Flask(__name__)
CORS(app)

@app.route("/")
def hello_world():
    return "Hello, World!"

@app.route('/list-raw-assets', methods=['GET'])
def list_raw_assets():
    files = []
    for (dirpath, dirnames, filenames) in walk('raw_assets'):
        full_names = [dirpath + '/' + file for file in filenames]
        files.extend(full_names)
    return files

@app.route('/get-raw-asset', methods=['GET'])
def get_data_asset():
    asset_name = request.args.get('asset')
    data = pd.read_csv(asset_name, skipinitialspace=True)
    return data.fillna('').to_dict('tight')

if __name__ == '__main__':
    app.run(debug = True, host='0.0.0.0', port=3000)
