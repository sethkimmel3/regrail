from flask import Flask, request
from flask_cors import CORS, cross_origin
from os import walk, path, mkdir
import pandas as pd
import json
import networkx as nx
import random

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

# def create_id(type):
#     return type + '-' + ''.join(random.choice('1234567890abcdefghijklmnopqrstuvwxyz') for i in range(13))

def create_directory_if_not_exists(directory):
    if not path.exists(directory):
        mkdir(directory)

def process_block(model_id, block_id, parents, type, properties, data_ref):
    if type == 'data-source':
        raw_data = pd.read_csv(data_ref, skipinitialspace=True)
        # we want to re-store the raw data as a model asset
        directory = '/'.join(['model_assets', model_id])
        create_directory_if_not_exists(directory)
        data_ref = '/'.join([directory, block_id + '.snappy.parquet'])
        raw_data.to_parquet(data_ref)
    elif type == 'join':
        left_block = properties['left_block']
        left_key = properties['left_key']
        right_block = properties['right_block']
        right_key = properties['right_key']
        method = properties['method']

        left_data = pd.read_parquet('/'.join(['model_assets', model_id, left_block + '.snappy.parquet']), engine='fastparquet')
        right_data = pd.read_parquet('/'.join(['model_assets', model_id, right_block + '.snappy.parquet']), engine='fastparquet')

        merged = left_data.merge(right_data, left_on=left_key, right_on=right_key, how=method)

        directory = '/'.join(['model_assets', model_id])
        data_ref = '/'.join([directory, block_id + '.snappy.parquet'])
        merged.to_parquet(data_ref)

    return data_ref

@app.route('/run-model', methods=['GET'])
def run_model():
    model = json.loads(request.args.get('model'))
    model_id = model['model-id']
    blocks = model['blocks']
    edges = model['edges']
    # we want to do a quick top sort to get the ordering for processing
    DG = nx.DiGraph()
    for connector_id, edge in edges.items():
        DG.add_edge(edge['from'], edge['to'])
    block_order = list(nx.topological_sort(DG))

    ref_array = {}
    ref_array[model_id] = {}
    for block_id in block_order:
        data_ref = process_block(model_id, block_id, DG.predecessors(block_id), blocks[block_id]['type'], blocks[block_id]['properties'], blocks[block_id]['data-ref'])
        ref_array[model_id][block_id] = data_ref

    return ref_array 

if __name__ == '__main__':
    app.run(debug = True, host='0.0.0.0', port=3000)
