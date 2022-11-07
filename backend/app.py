from flask import Flask, request
from flask_cors import CORS, cross_origin
from os import walk, path, mkdir
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import json
import networkx as nx
import random
import numpy as np

app = Flask(__name__)
CORS(app)

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
    asset_type = request.args.get('type')
    print(asset_type, flush=True)
    if asset_type == 'csv':
        data = read_csv_data(asset_name)
    elif asset_type == 'excel':
        data = read_excel_data(asset_name)
    data_dict, summary = prepare_dataframe_for_return(data)
    return {'data_dict': data_dict, 'summary': summary}

def create_id(type):
    return type + '-' + ''.join(random.choice('1234567890abcdefghijklmnopqrstuvwxyz') for i in range(13))

def create_directory_if_not_exists(directory):
    if not path.exists(directory):
        mkdir(directory)

def read_csv_data(data_ref):
    return pd.read_csv(data_ref, encoding='utf-8', skipinitialspace=True)

def read_excel_data(data_ref):
    return pd.read_excel(data_ref)

def write_to_parquet(df, data_ref):
    types = df.dtypes.to_dict()
    mapping = {}
    for k,v in types.items():
        if str(v) == 'object':
            mapping[k] = 'str'
    df = df.astype(mapping, copy=True)

    # Needed to add this due to some funky issue with multiindex in pivot tables
    table = pa.Table.from_pandas(df)
    pq.write_table(table, data_ref)

def read_parquet(data_ref):
    return pd.read_parquet(data_ref, engine='pyarrow')

def prepare_dataframe_for_return(df):
    # TODO: eventually, we'll want to be able to load much larger datasets, but for now we'll limit it to 1,000
    if len(df.index) > 1000:
        data = df.head(1000)
        truncated = True
    else:
        data = df
        truncated = False

    summary = {'truncated': truncated, 'row_count': len(df.index), 'column_types': str(df.dtypes.to_dict())}
    return data.fillna('').to_dict('tight'), summary

class ReGModel:
    def __init__(self, model):
        self.model = model
        self.model_id = model['model-id']
        self.blocks = model['blocks']
        self.edges = model['edges']
        self.graph = self.build_graph()
        self.topological_ordering = self.get_topological_ordering()
        self.run_results = {self.model_id: {}}

    def print_model(self):
        print(self.model, flush=True)

    def build_graph(self):
        DG = nx.DiGraph()
        for connector_id, edge in self.edges.items():
            DG.add_edge(edge['from'], edge['to'])
        return DG

    def get_topological_ordering(self):
        return list(nx.topological_sort(self.graph))

    def process_model(self):
        for block_id in self.topological_ordering:
            RGB = ReGBlock(self.model_id, block_id, list(self.graph.predecessors(block_id)), self.blocks[block_id]['type'], self.blocks[block_id]['properties'], self.blocks[block_id]['data-ref'])
            data_ref, data_dict, summary, error = RGB.process_block()
            self.run_results[self.model_id][block_id] = {'data-ref': data_ref, 'data': data_dict, 'error': error, 'summary': summary}

class ReGBlock:
    def __init__(self, model_id, block_id, parents, type, properties, data_ref):
        self.model_id = model_id
        self.block_id = block_id
        self.parents = parents
        self.type = type
        self.properties = properties
        self.data_ref = data_ref
        self.directory = '/'.join(['model_assets', model_id])
        create_directory_if_not_exists(self.directory)
        self.data_dict = None
        self.summary = None

    def create_data_ref(self):
        self.data_ref = '/'.join([self.directory, self.block_id + '.snappy.parquet'])

    def process_block(self):
        if self.type == 'csv-file':
            if self.data_ref.split('/')[0] == 'raw_assets':
                raw_data = read_csv_data(self.data_ref)
                self.create_data_ref()
                write_to_parquet(raw_data, self.data_ref)
                self.data_dict, self.summary = prepare_dataframe_for_return(raw_data)
            elif self.data_ref.split('/')[0] == 'model_assets':
                data = read_parquet(self.data_ref)
                self.data_dict, self.summary = prepare_dataframe_for_return(data)
            return self.data_ref, self.data_dict, self.summary, None
        elif self.type == 'excel-file':
            if self.data_ref.split('/')[0] == 'raw_assets':
                raw_data = read_excel_data(self.data_ref)
                self.create_data_ref()
                write_to_parquet(raw_data, self.data_ref)
                self.data_dict, self.summary = prepare_dataframe_for_return(raw_data)
            elif self.data_ref.split('/')[0] == 'model_assets':
                data = read_parquet(self.data_ref)
                self.data_dict, self.summary = prepare_dataframe_for_return(data)
            return self.data_ref, self.data_dict, self.summary, None
        elif self.type == 'join':
            left_block = self.properties['left_block']
            left_key = self.properties['left_key']
            right_block = self.properties['right_block']
            right_key = self.properties['right_key']
            method = self.properties['method']

            left_data = read_parquet('/'.join(['model_assets', self.model_id, left_block + '.snappy.parquet']))
            right_data = read_parquet('/'.join(['model_assets', self.model_id, right_block + '.snappy.parquet']))

            try:
                merged = left_data.merge(right_data, left_on=left_key, right_on=right_key, how=method)
            except Exception as e:
                return None, None, None, str(e)
            else:
                self.create_data_ref()
                write_to_parquet(merged, self.data_ref)
                self.data_dict, self.summary = prepare_dataframe_for_return(merged)
                return self.data_ref, self.data_dict, self.summary, None
        elif self.type == 'filter-rows':
            filter_column = self.properties['filter_column']
            operator = self.properties['operator']
            value = self.properties['value']

            parent_data = read_parquet('/'.join(['model_assets', self.model_id, self.parents[0] + '.snappy.parquet']))

            if value[0] == '[' and value[-1] == ']':
                multi = True
                values = value[1:-1].split(',')
                values = [value.strip() for value in values]
            else:
                multi = False

            # try to convert value to filter_column type
            type = parent_data.dtypes[filter_column]
            try:
                # TODO: scope this way more extensively
                if type == 'int64':
                    if multi:
                        values = [int(value) for value in values]
                    else:
                        value = int(value)
            except Exception as e:
                return None, None, None, str(e)

            try:
                if operator == '=':
                    if multi:
                        filtered = parent_data[parent_data[filter_column].isin(values)]
                    else:
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
                return None, None, None, str(e)
            else:
                self.create_data_ref()
                write_to_parquet(filtered, self.data_ref)
                self.data_dict, self.summary = prepare_dataframe_for_return(filtered)
                return self.data_ref, self.data_dict, self.summary, None
        elif self.type == 'select-rows':

            rows = self.properties['rows']

            parent_data = read_parquet('/'.join(['model_assets', self.model_id, self.parents[0] + '.snappy.parquet']))

            # convert row selections to integers
            try:
                row_selections = rows.replace(' ', '').split(',')
                int_selections = []
                for row_selection in row_selections:
                    if ':' not in row_selection:
                        int_selections.append(int(row_selection))
                    else:
                        a,b = row_selection.split(':')
                        # TODO: this could get really innefficent for large ranges. Would be better to use slices, but couldn't seem to combine ints and slices together.
                        # Running separate iloc operations and concatentating the dataframes is an option, but also potentially even slower. 
                        for i in range(int(a),int(b) + 1):
                            int_selections.append(i)
            except Exception as e:
                return None, None, None, str(e)

            try:
                selected = parent_data.iloc[int_selections]
            except Exception as e:
                return None, None, None, str(e)
            else:
                self.create_data_ref()
                write_to_parquet(selected, self.data_ref)
                self.data_dict, self.summary = prepare_dataframe_for_return(selected)
                return self.data_ref, self.data_dict, self.summary, None
        elif self.type == 'order':
            order_columns = self.properties['order_columns']
            asc_desc = self.properties['asc_desc']

            asc_desc_bools = []
            for column in order_columns:
                if asc_desc[column] == 'asc':
                    asc_desc_bools.append(True)
                elif asc_desc[column] == 'desc':
                    asc_desc_bools.append(False)

            parent_data = read_parquet('/'.join(['model_assets', self.model_id, self.parents[0] + '.snappy.parquet']))

            try:
                ordered = parent_data.sort_values(by=order_columns, ascending=asc_desc_bools)
            except Exception as e:
                return None, None, None, str(e)
            else:
                self.create_data_ref()
                write_to_parquet(ordered, self.data_ref)
                self.data_dict, self.summary = prepare_dataframe_for_return(ordered)
                return self.data_ref, self.data_dict, self.summary, None
        elif self.type == 'drop-columns':
            columns = self.properties['columns']

            parent_data = read_parquet('/'.join(['model_assets', self.model_id, self.parents[0] + '.snappy.parquet']))

            try:
                dropped = parent_data.drop(columns=columns)
            except Exception as e:
                return None, None, None, str(e)
            else:
                self.create_data_ref()
                write_to_parquet(dropped, self.data_ref)
                self.data_dict, self.summary = prepare_dataframe_for_return(dropped)
                return self.data_ref, self.data_dict, self.summary, None
        elif self.type == 'group-by':
            columns = self.properties['group_columns']
            agg_functions = self.properties['agg_functions']

            parent_data = read_parquet('/'.join(['model_assets', self.model_id, self.parents[0] + '.snappy.parquet']))

            try:
                grouped = parent_data.groupby(columns, as_index=False).agg(agg_functions)
            except Exception as e:
                return None, None, None, str(e)
            else:
                self.create_data_ref()
                write_to_parquet(grouped, self.data_ref)
                self.data_dict, self.summary = prepare_dataframe_for_return(grouped)
                return self.data_ref, self.data_dict, self.summary, None
        elif self.type == 'pivot-table':
            values_columns = self.properties['values_columns']
            index_columns = self.properties['index_columns']
            new_columns = self.properties['new_columns']
            agg_functions = self.properties['agg_functions']

            parent_data = read_parquet('/'.join(['model_assets', self.model_id, self.parents[0] + '.snappy.parquet']))

            try:
                pivot_table = pd.pivot_table(parent_data, values=values_columns, index=index_columns, columns=new_columns, aggfunc=agg_functions)
            except Exception as e:
                return None, None, None, str(e)
            else:
                self.create_data_ref()
                write_to_parquet(pivot_table, self.data_ref)
                self.data_dict, self.summary = prepare_dataframe_for_return(pivot_table)
                return self.data_ref, self.data_dict, self.summary, None

@app.route('/run-model', methods=['GET'])
def run_model():
    model = json.loads(request.args.get('model'))
    RGM = ReGModel(model)
    RGM.print_model()
    RGM.process_model()
    return RGM.run_results

if __name__ == '__main__':
    app.run(debug = True, host='0.0.0.0', port=3000)
