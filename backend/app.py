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

def prepare_dataframe_for_return(data):
    # TODO: truncate if above a certain size
    return data.fillna('').to_dict('tight')

@app.route('/get-raw-asset', methods=['GET'])
def get_data_asset():
    asset_name = request.args.get('asset')
    data = pd.read_csv(asset_name, skipinitialspace=True)
    return prepare_dataframe_for_return(data)

# def create_id(type):
#     return type + '-' + ''.join(random.choice('1234567890abcdefghijklmnopqrstuvwxyz') for i in range(13))

def create_directory_if_not_exists(directory):
    if not path.exists(directory):
        mkdir(directory)

def process_block(model_id, block_id, parents, type, properties, data_ref):
    if type == 'csv-file':
        if data_ref.split('/')[0] == 'raw_assets':
            # we want to re-store the raw data as a model asset
            raw_data = pd.read_csv(data_ref, skipinitialspace=True)
            directory = '/'.join(['model_assets', model_id])
            create_directory_if_not_exists(directory)
            data_ref = '/'.join([directory, block_id + '.snappy.parquet'])
            raw_data.to_parquet(data_ref)
            data_dict = prepare_dataframe_for_return(raw_data)
        elif data_ref.split('/')[0] == 'model_assets':
            data = pd.read_parquet(data_ref, engine='fastparquet')
            data_dict = prepare_dataframe_for_return(data)
        return data_ref, data_dict, None
    elif type == 'join':
        left_block = properties['left_block']
        left_key = properties['left_key']
        right_block = properties['right_block']
        right_key = properties['right_key']
        method = properties['method']

        left_data = pd.read_parquet('/'.join(['model_assets', model_id, left_block + '.snappy.parquet']), engine='fastparquet')
        right_data = pd.read_parquet('/'.join(['model_assets', model_id, right_block + '.snappy.parquet']), engine='fastparquet')

        try:
            merged = left_data.merge(right_data, left_on=left_key, right_on=right_key, how=method)
        except Exception as e:
            return None, None, str(e)
        else:
            directory = '/'.join(['model_assets', model_id])
            data_ref = '/'.join([directory, block_id + '.snappy.parquet'])
            merged.to_parquet(data_ref)
            data_dict = prepare_dataframe_for_return(merged)
            return data_ref, data_dict, None
    elif type == 'filter-rows':
        filter_column = properties['filter_column']
        operator = properties['operator']
        value = properties['value']

        parent_data = pd.read_parquet('/'.join(['model_assets', model_id, parents[0] + '.snappy.parquet']), engine='fastparquet')

        # try to convert value to filter_column type
        type = parent_data.dtypes[filter_column]
        try:
            # TODO: scope this way more extensively
            if type == 'int64':
                value = int(value)
        except Exception as e:
            return None, None, str(e)

        try:
            if operator == '=':
                filtered = parent_data[parent_data[filter_column] == value]
            elif operator == '<':
                filtered = parent_data[parent_data[filter_column] < value]
            elif operator == '>':
                filtered = parent_data[parent_data[filter_column] > value]
            elif operator == '<=':
                filtered = parent_data[parent_data[filter_column] <= value]
            elif operator == '>=':
                filtered = parent_data[parent_data[filter_column] >= value]
        except Exception as e:
            return None, None, str(e)
        else:
            directory = '/'.join(['model_assets', model_id])
            data_ref = '/'.join([directory, block_id + '.snappy.parquet'])
            filtered.to_parquet(data_ref)
            data_dict = prepare_dataframe_for_return(filtered)
            return data_ref, data_dict, None

@app.route('/run-model', methods=['GET'])
def run_model():
    model = json.loads(request.args.get('model'))
    print(model, flush=True)
    model_id = model['model-id']
    blocks = model['blocks']
    edges = model['edges']
    # we want to do a quick top sort to get the ordering for processing
    DG = nx.DiGraph()
    for connector_id, edge in edges.items():
        DG.add_edge(edge['from'], edge['to'])
    block_order = list(nx.topological_sort(DG))

    return_array = {}
    return_array[model_id] = {}
    for block_id in block_order:
        return_array[model_id][block_id] = {}
        data_ref, data_dict, error = process_block(model_id, block_id, list(DG.predecessors(block_id)), blocks[block_id]['type'], blocks[block_id]['properties'], blocks[block_id]['data-ref'])
        return_array[model_id][block_id]['data-ref'] = data_ref
        return_array[model_id][block_id]['data'] = data_dict
        return_array[model_id][block_id]['error'] = error

    return return_array

if __name__ == '__main__':
    app.run(debug = True, host='0.0.0.0', port=3000)
