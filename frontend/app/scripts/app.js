$( document ).ready(function(){

  // user vars
  var model_id;
  var user_id;

  var assets = []; // convention: {path: path/to/file, name: example.csv, scope: user/sample, type:.csv/.xlsx/other}

  // model vars
  var blocks;
  var connectors;
  var edges; // convention: {from: block_id, to: block_id, connector: connector_id}
  var block_data; // convention: {type: source/join/etc., properties: {}, data-ref: ..directory + file, header: Array(...), data: Array(Array()...Array()), error: null/string}

  // vis vars
  var stage;
  var drawing_connector = false;
  var from_block = null;
  var line_layer;
  var line;
  var selected_block_id = null;

  var width = window.innerWidth;
  var height = window.innerHeight;

  const MAX_FILE_UPLOAD_SIZE = 25000000;

  $.fn.displayAssetSelectors = () => {
    var select_value = $('#block-select').val();
      if (select_value == 'csv-file') {
        $('#data-select').show();
        $.fn.displayRawAssets('csv');
        $('#file-input').hide();
      }
      else if (select_value == 'excel-file'){
        $('#data-select').show();
        $.fn.displayRawAssets('xlsx');
        $('#file-input').hide();
      }
      else if (select_value == 'upload-file'){
        $('#data-select').hide();
        $('#file-input').show();
      }
      else {
        $('#data-select').hide();
        $('#file-input').hide();
      }
  }

  $( '#block-select' ).change(function(){
    $.fn.displayAssetSelectors();
  });

  $.fn.blockFactory = function(){
    var block_id = "block-" + Math.random().toString(16).slice(2);

    var layer = new Konva.Layer({
      draggable: true,
      id: block_id,
      name: block_id,
      x: 75,
      y: 75
    });

    var box = new Konva.Rect({
      width: 100,
      height: 50,
      fill: 'lightblue',
      stroke: 'black',
      strokeWidth: 4,
      name: 'box'
    });

    var box_text = new Konva.Text({
      name: 'box_text'
    });

    var x_circle = new Konva.Circle({
      radius: 10,
      fill: 'red',
      stroke: 'black',
      name: 'x_circle'
    });

    var x_text = new Konva.Text({
      text: 'x',
      x: - 3,
      y: - 6,
      name: 'x_text'
    });

    var edge_circle = new Konva.Circle({
      radius: 10,
      x: box.width() / 2,
      y: box.height(),
      fill: 'green',
      stroke: 'black',
      name: 'edge_circle'
    });

    var edge_text = new Konva.Text({
      text: '+',
      x: (box.width() / 2) - 3,
      y: box.height() - 4,
      name: 'edge_text'
    });

    layer.add(box);
    layer.add(x_circle);
    layer.add(x_text);
    layer.add(edge_circle);
    layer.add(edge_text);
    layer.add(box_text);

    return layer;
  }

  $.fn.addBlockEventListeners = (block_id) => {
    blocks[block_id].on('mouseover', function() {
      document.body.style.cursor = 'pointer';
    });

    blocks[block_id].on('mouseout', function() {
      document.body.style.cursor = 'default';
    });

    blocks[block_id].on('dragend', function() {
      $.fn.reRenderConnectors(block_id);
    });

    $.fn.getBlockChild(block_id, 'x_circle').on('click', function() {
      $.fn.removeBlock(block_id);
    });

    $.fn.getBlockChild(block_id, 'x_text').on('click', function() {
      $.fn.removeBlock(block_id);
    });
  }

  $.fn.addStageEventListeners = () => {
    stage.on('mousemove', (e) => {
      if (!drawing_connector){
        return;
      }
      const pos = stage.getPointerPosition();
      const points = line.points().slice();
      points[2] = pos.x;
      points[3] = pos.y;
      line.points(points);
      line_layer.batchDraw();
    });

    stage.on('click', (e) => {
      if (!drawing_connector && (e.target.hasName('edge_circle') || e.target.hasName('edge_text'))) {
        var parent_block_id = $.fn.getBlockIdFromChild(e.target);
        $.fn.startDrawingConnector(parent_block_id);
      }
      else if (drawing_connector && (e.target.hasName('box') || e.target.hasName('box_text'))){
        $.fn.stopDrawingConnector($.fn.getBlockIdFromChild(e.target));
      }
      else if (drawing_connector) {
        line.destroy();
        line_layer.draw();
        line = null;
        line_layer = null;
        drawing_connector = false;
        from_block = null;
      }
      else if (!drawing_connector && (e.target.hasName('box') || e.target.hasName('box_text')) && !e.target.hasName('edge_circle') && !e.target.hasName('edge_text')){
        $.fn.toggleSelectedBlock($.fn.getBlockIdFromChild(e.target));
      }
    })
  }

  $.fn.initializeNewModel = () => {
    model_id = "model-" + Math.random().toString(16).slice(2);

    var url_base = window.location.href.split('?')[0];
    var url_part = window.location.href.split('?')[1];
    var url_params = new URLSearchParams(url_part);
    url_params.set('model', model_id)

    const newrl = url_base + '?' + url_params.toString();
    window.history.replaceState(null, '', newrl);

    stage = new Konva.Stage({
      container: 'flow',
      width: width,
      height: height,
    });

    $.fn.addStageEventListeners();

    blocks = {};
    connectors = {};
    edges = []; // convention: {from: block_id, to: block_id, connector: connector_id}
    block_data = {}; // convention: {type: source/join/etc., properties: {}, data-ref: ..directory + file, header: Array(...), data: Array(Array()...Array()), error: null/string}

    $.fn.saveModel();
  }

  $.fn.startupSequence = () => {
    if(window.localStorage.getItem('user_id') != null){
      user_id = window.localStorage.getItem('user_id');
    } else {
      user_id = "user-" + Math.random().toString(16).slice(2);
      window.localStorage.setItem('user_id', user_id);
    }

    const url_part = window.location.href.split('?')[1];
    const url_params = new URLSearchParams(url_part);
    if (url_params.has('model')) {
      model_id = url_params.get('model');
      if(window.localStorage.getItem(model_id) != null) {
        var model_data = JSON.parse(window.localStorage.getItem(model_id));

        stage = Konva.Node.create(model_data['stage'], 'flow');
        $.fn.addStageEventListeners();

        blocks = {};
        connectors = {};
        edges = model_data['edges'];
        block_data = model_data['block_data'];

        var connector_ids = [];
        for (const edge of edges) {
          if (!connector_ids.includes(edge['connector'])){
            connector_ids.push(edge['connector'])
          }
        }

        for (const child of stage.children) {
          if (Object.keys(block_data).includes(child.attrs.id)){
            blocks[child.attrs.id] = child;
          }
          else if (connector_ids.includes(child.attrs.id)){
            connectors[child.attrs.id] = child;
          }
        }

        selected_block_id = null;

        for (const block_id of Object.keys(blocks)) {
          $.fn.addBlockEventListeners(block_id);
          $.fn.recolorBlock(block_id);
        }
      } else {
        $.fn.initializeNewModel();
      }
    }
    else {
      $.fn.initializeNewModel();
    }
  }

  // TODO: it's unclear if we've added calls to this function in the right places. We should devise a smarter way of automatically saving the state of the model given certain changes.
  $.fn.saveModel = () => {
    var model_array = {
      'stage': stage.toJSON(),
      'edges': edges,
      'block_data': block_data
    }
    window.localStorage.setItem(model_id, JSON.stringify(model_array));
  }

  $.fn.populateSavedModels = () => {
    var saved_models = JSON.parse(window.localStorage.getItem('models'));
    if (saved_models == null) {
      saved_models = {};
    }
    if (!Object.keys(saved_models).includes(model_id)) {
      const today = new Date();
      saved_models[model_id] = {'name': model_id, 'created_at': today.toISOString()};
    }
    window.localStorage.setItem('models', JSON.stringify(saved_models));

    // TODO: this can't be efficient
    var model_created_dates = [];
    for (const item in saved_models) {
      model_created_dates.push(saved_models[item]['created_at']);
    }
    model_created_dates.sort().reverse();

    var model_ids_ordered = [];
    for (const date of model_created_dates){
      for (const item in saved_models){
        if (saved_models[item]['created_at'] == date) {
          model_ids_ordered.push(item);
        }
      }
    }

    for (const mid of model_ids_ordered) {
      var model_option = document.createElement('option');
      model_option.value = mid;
      model_option.id = 'model-select-option-' + mid;
      model_option.innerHTML = saved_models[mid]['name'];
      if (mid == model_id) {
        model_option.setAttribute('selected', true);
      }
      document.getElementById('model-select').appendChild(model_option);
    }
  }

  $('#model-select').on('change', function() {
    var url_base = window.location.href.split('?')[0];
    var direct_to = url_base + '?model=' + this.value;
    window.location.href = direct_to;
  });

  $('#rename-model').on('click', function() {

    var selected_id = document.getElementById('model-select').value;
    var saved_models = JSON.parse(window.localStorage.getItem('models'));
    var selected_name = saved_models[selected_id]['name'];

    var new_name = window.prompt("Choose a new model name.", selected_name);
    if (new_name != null) {
      saved_models[selected_id]['name'] = new_name;
      window.localStorage.setItem('models', JSON.stringify(saved_models));
      document.getElementById('model-select-option-' + selected_id).innerHTML = new_name;
    }
  });

  $('#new-model').click(function(){
    var url_base = window.location.href.split('?')[0];
    window.location.href = url_base;
  });

  $.fn.getBlockChild = (block_id, child_name) => {
    var child_obj = null;
    for (const child of blocks[block_id].children){
      if (child.hasName(child_name)) {
        child_obj = child; // Can probably return from here for more efficiency.
      }
    }
    if (child_obj == null) {
      console.log('Child not found!');
    }
    return child_obj;
  }

  $.fn.getBlockIdFromChild = (child) => {
    return child.parent.attrs.id;
  }

  $.fn.getConnectorChild = (connector_id, child_name) => {
    var child_obj = null;
    for (const child of connectors[connector_id].children){
      if (child.hasName(child_name)) {
        child_obj = child; // Can probably return from here for more efficiency.
      }
    }
    if (child_obj == null) {
      console.log('Child not found!');
    }
    return child_obj;
  }

  $.fn.getBlockParents = (block_id) => {
    var parents = [];
    for (const edge of edges) {
      if (edge['to'] == block_id) {
        if (!parents.includes(edge['from'])) {
          parents.push(edge['from']);
        }
      }
    }
    return parents;
  }

  $.fn.getBlockChildren = (block_id) => {
    var children = [];
    for (const edge of edges) {
      if (edge['from'] == block_id) {
        if (!children.includes(edge['to'])) {
          children.push(edge['to']);
        }
      }
    }
    return children;
  }

  $.fn.getBlockName = (block_id) => {
    return blocks[block_id].attrs.name;
  }

  $.fn.setBlockName = (block_id, name) => {
    blocks[block_id].name(name);
    $.fn.saveModel();
  }

  $.fn.startDrawingConnector = function(block_id){
    var connector_id = "connector-" + Math.random().toString(16).slice(2);

    var edge_circle = $.fn.getBlockChild(block_id, 'edge_circle');

    line_layer = new Konva.Layer({
      draggable: false,
      id: connector_id
    });

    drawing_connector = true;
    from_block = block_id;

    const pos = stage.getPointerPosition();

    line = new Konva.Arrow({
      stroke: 'black',
      listening: false,
      points: [edge_circle.absolutePosition().x, edge_circle.absolutePosition().y, pos.x, pos.y],
      name: 'line',
      id: connector_id
    });

    line_layer.add(line);

    stage.add(line_layer);
  }

  $.fn.stopDrawingConnector = function(block_id){
    if(!drawing_connector){
      return;
    }

    var connector_id = line_layer.attrs.id
    var to_block = block_id;
    var edge = {'from':from_block, 'to':to_block, 'connector': connector_id};

    let found = 0;
    for (const edge of edges) {
      if (edge['from'] == from_block && edge['to'] == to_block){
        found = 1;
      }
    }

    var box = $.fn.getBlockChild(block_id, 'box');

    if(!found){
      edges.push(edge);

      const points = line.points().slice();
      points[2] = box.absolutePosition().x + box.attrs.width / 2;
      points[3] = box.absolutePosition().y;
      line.points(points);
      line_layer.batchDraw();

      connectors[connector_id] = line_layer;
    } else {
      line.destroy();
      line_layer.draw();
      line = null;
      line_layer = null;
    }
    // TODO: handle cycles
    // TODO: can't connect source to source

    drawing_connector = false;
    from_block = null;
    $.fn.saveModel();
  }

  $.fn.reRenderConnector = (edge) => {
    let from_x = $.fn.getBlockChild(edge['from'], 'edge_circle').absolutePosition().x;
    let from_y = $.fn.getBlockChild(edge['from'], 'edge_circle').absolutePosition().y;

    let to_x = $.fn.getBlockChild(edge['to'], 'box').absolutePosition().x + $.fn.getBlockChild(edge['to'], 'box').attrs.width / 2;
    let to_y = $.fn.getBlockChild(edge['to'], 'box').absolutePosition().y;

    $.fn.getConnectorChild(edge['connector'], 'line').points([from_x, from_y, to_x, to_y]);

    connectors[edge['connector']].batchDraw();
  }

  $.fn.reRenderConnectors = (block_id) => {
    for (const edge of edges) {
      if (edge['from'] == block_id || edge['to'] == block_id){
        $.fn.reRenderConnector(edge);
      }
    }
    $.fn.saveModel();
  }

  $( document ).on('click', '#add-block', function(){
    var block_type = $('#block-select').val();

    // throw errors if necessary
    var errors = false;
    switch (block_type) {
      case 'csv-file':
        var data_source = $('#data-select').val();

        if (data_source == undefined || data_source == ""){
          window.alert("You must select a data source.");
          errors = true;
        }
      break;
      case 'excel-file':
        var data_source = $('#data-select').val();

        if (data_source == undefined || data_source == ""){
          window.alert("You must select a data source.");
          errors = true;
        }
      break;
      case 'upload-file':
        var file_input = document.getElementById('file-input').files[0];

        if (file_input == undefined || file_input == ""){
          window.alert("You must select a csv or excel file!");
          errors = true;
        }
        else if (file_input.size > MAX_FILE_UPLOAD_SIZE){
          window.alert("You can't upload a file greater than 25MB yet. Don't worry, we'll get there!");
          errors = true;
        }
      break;
      }

    if (!errors) {
      var block = $.fn.blockFactory();
      blocks[block.attrs.id] = block;
      stage.add(block);
      $.fn.addBlockEventListeners(block.attrs.id);

      block_data[block.attrs.id] = {'type': block_type, 'data-ref': null, 'header': null, 'data': null, 'summary': null, 'error': null};
      switch (block_type) {
        case 'csv-file':
          var data_source = $('#data-select').val();
          block_data[block.attrs.id]['data-ref'] = data_source;
          block_data[block.attrs.id]['properties'] = {};
          var name = data_source.split('/')[data_source.split('/').length - 1];
          $.fn.setBlockName(block.attrs.id, name);
          var asset = $.fn.loadRawAsset(block.attrs.id, data_source, 'csv');
        break;
        case 'excel-file':
          var data_source = $('#data-select').val();
          block_data[block.attrs.id]['data-ref'] = data_source;
          block_data[block.attrs.id]['properties'] = {};
          var name = data_source.split('/')[data_source.split('/').length - 1];
          $.fn.setBlockName(block.attrs.id, name);
          var asset = $.fn.loadRawAsset(block.attrs.id, data_source, 'excel');
        break;
        case 'upload-file':
          var file_input = document.getElementById('file-input').files[0];
          var data_ref = $.fn.saveUserAsset(user_id, file_input);
          block_data[block.attrs.id]['data-ref'] = data_ref;
          var name = file_input.name.split('/')[file_input.name.split('/').length - 1];
          $.fn.setBlockName(block.attrs.id, name);

          var file_type = name.split('.')[name.split('.').length - 1] == 'csv' ? 'csv' : 'excel';
          block_data[block.attrs.id]['properties'] = {'file_type': file_type};
          var asset = $.fn.loadRawAsset(block.attrs.id, data_ref, file_type);
        break;
        case 'join':
          block_data[block.attrs.id]['properties'] = {'left_block': null, 'right_block': null, 'left_key': null, 'right_key': null, 'type': 'left'};
        break;
        case 'filter-rows':
          block_data[block.attrs.id]['properties'] = {'filter_column': null, 'operator': null, 'value': null};
        break;
        case 'select-rows':
          block_data[block.attrs.id]['properties'] = {'rows': []}
        break;
        case 'order':
          block_data[block.attrs.id]['properties'] = {'order_columns': [], 'asc_desc': {}};
        break;
        case 'count-items':
          block_data[block.attrs.id]['properties'] = {'count_column': null, 'delimiter': null};
        break;
        case 'drop-columns':
          block_data[block.attrs.id]['properties'] = {'columns': []};
        break;
        case 'group-by':
          block_data[block.attrs.id]['properties'] = {'group_columns': []};
        break;
        case 'sum-columns':
          block_data[block.attrs.id]['properties'] = {};
        case 'pivot-table':
          block_data[block.attrs.id]['properties'] = {'values_columns': [], 'index_columns': [], 'new_columns': [], 'agg_functions': {}};
        break;
      }

      $.fn.blockDataUpdated(block.attrs.id);
      $.fn.saveModel();
    }
  });

  $.fn.removeBlock = (block_id) => {
    var indices_to_remove = [];
    var index = 0;
    // first loop through the edges to destroy the connector and track
    // which edges to remove from the array
    for (const edge of edges){
      if (edge['to'] == block_id || edge['from'] == block_id){
        connectors[edge['connector']].destroy();
        delete connectors[edge['connector']];
        indices_to_remove.push(index);
      }
      index += 1;
    }
    for (var i = indices_to_remove.length - 1; i >= 0; i--) {
      edges.splice(indices_to_remove[i], 1);
    }

    if (block_id == selected_block_id) {
      selected_block_id = null;
      document.getElementById('selected-block').innerHTML = 'Selected Block: ' ;
      $.fn.clearTable();
      $.fn.hideBlockProps();
    }

    // now remove the block
    blocks[block_id].destroy();

    delete blocks[block_id];
    delete block_data[block_id];

    document.body.style.cursor = 'default';
    $.fn.saveModel();
  }

  $.fn.updateBlockText = (block_id) => {
    var type = block_data[block_id]['type'];
    switch (type) {
      case 'csv-file':
        var text = $.fn.getBlockName(block_id);
      break;
      case 'excel-file':
        var text = $.fn.getBlockName(block_id);
      break;
      case 'upload-file':
        var text = $.fn.getBlockName(block_id);
      break;
      case 'join':
        if ( ('left_block' in block_data[block_id]['properties'] && block_data[block_id]['properties']['left_block'] != null) && ('right_block' in block_data[block_id]['properties'] && block_data[block_id]['properties']['right_block'] != null)) {
          let left_block = block_data[block_id]['properties']['left_block'];
          let left_table = $.fn.getBlockName(left_block);

          let right_block = block_data[block_id]['properties']['right_block'];
          let right_table = $.fn.getBlockName(right_block);

          let left_key = block_data[block_id]['properties']['left_key'];
          let right_key = block_data[block_id]['properties']['right_key'];

          let method = block_data[block_id]['properties']['method'];

          var text = `
          Join:
          Left Table: ${left_table}
          Right Table: ${right_table}
          Left Key: ${left_key}
          Right Key: ${right_key}
          Method: ${method}
          `;
        }
        else {
          var text = 'join';
        }
      break;
      case 'filter-rows':
        if ( ('filter_column' in block_data[block_id]['properties'] && block_data[block_id]['properties']['filter_column'] != null) && ('operator' in block_data[block_id]['properties'] && block_data[block_id]['properties']['operator'] != null) && ('value' in block_data[block_id]['properties'] && block_data[block_id]['properties']['value'] != null)) {
          let filter_column = block_data[block_id]['properties']['filter_column'];

          let operator = block_data[block_id]['properties']['operator'];

          let value = block_data[block_id]['properties']['value'];

          var text = `
          Row Filter:
          Filter Column: ${filter_column}
          Operator: ${operator}
          Value: ${value}
          `;
        }
        else {
          var text = 'row filter';
        }
      break;
      case 'select-rows':
        if ( block_data[block_id]['properties']['rows'] != null && block_data[block_id]['properties']['rows'] != '' ) {
          let rows = block_data[block_id]['properties']['rows'];

          var text = `
          Row Select:
          Rows: ${rows}
          `;
        }
        else {
          var text = 'row select';
        }
      break;
      case 'order':
        if ( ('order_columns' in block_data[block_id]['properties'] && block_data[block_id]['properties']['order_columns'].length != 0) && ('asc_desc' in block_data[block_id]['properties'] && Object.keys(block_data[block_id]['properties']['asc_desc']).length != 0) ) {
          let asc_desc = block_data[block_id]['properties']['asc_desc'];

          let asc_desc_items = [];
          for (const key of Object.keys(asc_desc)) {
            asc_desc_items.push(key + ': ' + asc_desc[key])
          }

          let asc_desc_text = asc_desc_items.join(', ');

          var text = `
          Order By:
          ${asc_desc_text}
          `;
        }
        else {
          var text = 'order by';
        }
      break;
      case 'count-items':
        if ( block_data[block_id]['properties']['count_column'] != null && block_data[block_id]['properties']['delimiter'] != null && block_data[block_id]['properties']['delimiter'] != '' ) {
          let count_column = block_data[block_id]['properties']['count_column'];

          let delimiter = block_data[block_id]['properties']['delimiter'];

          var text = `
          Count Items:
          Column: ${count_column}
          Delimiter: ${delimiter}
          `;
        }
        else {
          var text = 'count items';
        }
      break;
      case 'drop-columns':
        if ( ('columns' in block_data[block_id]['properties'] && block_data[block_id]['properties']['columns'].length != 0) ) {
          let columns = block_data[block_id]['properties']['columns'];

          let columns_text = columns.join(', ');

          var text = `
          Drop Columns:
          ${columns_text}
          `;
        }
        else {
          var text = 'drop columns';
        }
      break;
      case 'group-by':
        if ( block_data[block_id]['properties']['group_columns'].length != 0 && block_data[block_id]['properties']['agg_columns'].length != 0 && Object.keys(block_data[block_id]['properties']['agg_functions']).length != 0) {
          let group_columns = block_data[block_id]['properties']['group_columns'];

          let agg_columns = block_data[block_id]['properties']['agg_columns'];

          let agg_functions = block_data[block_id]['properties']['agg_functions'];

          let agg_items = [];
          for (const key of Object.keys(agg_functions)) {
            agg_items.push(key + ': ' + agg_functions[key]);
          }

          let agg_text = agg_items.join(', ');

          var text = `
          Group By: ${group_columns}
          ${agg_text}
          `;
        }
        else {
          var text = 'group by';
        }
      break;
      case 'sum-columns':
        var text = 'sum columns';
      break;
      case 'pivot-table':
        if ( block_data[block_id]['properties']['index_columns'].length != 0 && block_data[block_id]['properties']['new_columns'].length != 0) {
          let values_columns = block_data[block_id]['properties']['values_columns'];
          let index_columns = block_data[block_id]['properties']['index_columns'];
          let new_columns = block_data[block_id]['properties']['new_columns'];
          let agg_functions = block_data[block_id]['properties']['agg_functions'];

          let agg_items = [];
          for (const key of Object.keys(agg_functions)) {
            agg_items.push(key + ': ' + agg_functions[key]);
          }

          let agg_text = agg_items.join(', ');

          var text = `
          Pivot Table:
          Values: ${values_columns}
          Index: ${index_columns}
          Columns: ${new_columns}
          Aggregate On:
          ${agg_text}
          `;
        }
        else {
          var text = 'pivot table';
        }
      break;
    }

    $.fn.getBlockChild(block_id, 'box_text').text(text)
    $.fn.centerBlockText(block_id);
    $.fn.saveModel();
  }

  $.fn.recolorBlock = (block_id) => {
    if ( block_data[block_id]['error'] != null ){
      var color = '#FFCCCB';
    }
    else if ( block_data[block_id]['error'] == null && (block_data[block_id]['header'] != null || block_data[block_id]['data'] != null) ) {
      var color = '#CCFFCD';
    }
    else if ( block_data[block_id]['error'] == null && block_data[block_id]['header'] == null && block_data[block_id]['data'] == null ) {
      var color = 'lightblue';
    }
    if (selected_block_id != block_id) {
      $.fn.setBlockStroke(block_id, 'black');
    }

    $.fn.getBlockChild(block_id, 'box').fill(color);
    $.fn.saveModel();
  }

  $.fn.blockDataUpdated = (block_id) => {
    $.fn.updateBlockText(block_id);
    $.fn.recolorBlock(block_id);
    $.fn.saveModel();
  }

  $.fn.blockPropsUpdated = (block_id) => {
    block_data[block_id]['header'] = null;
    block_data[block_id]['data'] = null;
    block_data[block_id]['summary'] = null;
    $.fn.clearTable();

    $.fn.updateBlockText(block_id);
    $.fn.recolorBlock(block_id);

    // we also want to update all downstream blocks as their data is no longer valid, so we'll do that recursively
    var block_children = $.fn.getBlockChildren(block_id);
    for (const block_id of block_children){
      $.fn.blockPropsUpdated(block_id);
    }

    $.fn.saveModel();
  }

  $.fn.centerBlockText = (block_id) => {
    var box_width = $.fn.getBlockChild(block_id, 'box').attrs.width;
    var box_height = $.fn.getBlockChild(block_id, 'box').attrs.height;
    var text_width = $.fn.getBlockChild(block_id, 'box_text').textWidth;
    var text_lines = $.fn.getBlockChild(block_id, 'box_text').attrs.text.split('\n').length;
    var text_height = $.fn.getBlockChild(block_id, 'box_text').textHeight * text_lines;

    if (text_width > (box_width * 0.9)) {
      box_width = Math.ceil(text_width/(box_width * 0.9)) * 100;
    }
    if (text_height > (box_height * 0.9)) {
      box_height = Math.ceil(text_height/(height * 0.9)) * 100;
    }

    var new_x = (box_width - text_width) / 2;
    var new_y = (box_height - text_height) / 2;

    $.fn.getBlockChild(block_id, 'box').width(box_width);
    $.fn.getBlockChild(block_id, 'box').height(box_height);

    // #TODO: Might need better centering method. Doesn't look right when box is resized.
    $.fn.getBlockChild(block_id, 'box_text').x(new_x);
    $.fn.getBlockChild(block_id, 'box_text').y(new_y);
    $.fn.getBlockChild(block_id, 'box_text').zIndex(1);

    $.fn.getBlockChild(block_id, 'edge_circle').x(box_width/2);
    $.fn.getBlockChild(block_id, 'edge_circle').y(box_height);
    $.fn.getBlockChild(block_id, 'edge_circle').zIndex(2);

    $.fn.getBlockChild(block_id, 'edge_text').x((box_width/2) - 3);
    $.fn.getBlockChild(block_id, 'edge_text').y(box_height - 4);
    $.fn.getBlockChild(block_id, 'edge_text').zIndex(3);

    $.fn.reRenderConnectors(block_id);
  }

  $.fn.displayBlockState = (block_id) => {
    if ( block_data[block_id]['error'] != null ) {
      document.getElementById('error-message').innerHTML = block_data[block_id]['error'];
    } else {
      document.getElementById('error-message').innerHTML = '';
    }
    $.fn.populateTable(block_data[block_id]['header'], block_data[block_id]['data'], block_data[block_id]['summary']);
  }

  $.fn.populateTable = (header, data, summary) => {
    document.getElementById('data-table').replaceChildren();
    if (header != null || data != null) {
      $('#data-table').show();
      if (header != null) {
        var headerRow = document.createElement('tr');
        var cell = document.createElement('th');
        cell.innerHTML = 'Row Number';
        headerRow.appendChild(cell);
        for (const item of header){
          var cell = document.createElement('th');
          cell.innerHTML = item;
          headerRow.appendChild(cell);
        }
        document.getElementById('data-table').appendChild(headerRow);
      }
      if (data != null) {
        for (var i = 0; i < data.length; i++){
          var row = data[i];
          var tableRow = document.createElement('tr');
          var cell = document.createElement('td');
          cell.innerHTML = i;
          tableRow.append(cell);
          for (const item of Object.values(row)){
            var cell = document.createElement('td');
            cell.innerHTML = item;
            tableRow.append(cell);
          }
          document.getElementById('data-table').appendChild(tableRow);
        }
      }
    }

    $('#data-summary').html('');
    if (summary != null) {
      var summary_text = 'Row Count: ';
      if (summary['truncated']){
        summary_text += summary['row_count'] + ' (only displaying 1,000 rows)';
      } else {
        summary_text += summary['row_count'];
      }
      $('#data-summary').html(summary_text);
    }
  }

  $.fn.clearTable = () => {
    document.getElementById('data-table').replaceChildren();
    $('#data-table').hide();
    $('#data-summary').html('');
  }

  $.fn.hideBlockProps = () => {
    document.getElementById('transform-props').replaceChildren();
  }

  $.fn.transformPropFactory = (type, label_val, value, id, innerHTML, select_options=null) => {
    var span = document.createElement('span');
    var label = document.createElement('label');
    label.innerHTML = label_val;
    span.appendChild(label);
    switch (type) {
      case 'select':
        var elem = document.createElement('select');
        for (const select_key of Object.keys(select_options)) {
          var option = document.createElement('option');
          option.value = select_key;
          option.innerHTML = select_options[select_key];
          elem.appendChild(option);
        }
        elem.id = id;
        span.appendChild(elem);
      break;
      case 'multi-select':
        var elem = document.createElement('select');
        for (const select_key of Object.keys(select_options)) {
          var option = document.createElement('option');
          option.value = select_key;
          option.innerHTML = select_options[select_key];
          elem.appendChild(option);
        }
        elem.id = id;
        span.appendChild(elem);
        var add_button = document.createElement('button');
        add_button.id = id + '-add';
        add_button.innerHTML = 'add';
        span.appendChild(add_button);
        var remove_button = document.createElement('button');
        remove_button.id = id + '-remove';
        remove_button.innerHTML = 'remove';
        span.appendChild(remove_button);
        span.appendChild(document.createElement('BR'));
        var selected_label = document.createElement('label');
        selected_label.innerHTML = 'Selected: ';
        span.appendChild(selected_label);
        var selected_list = document.createElement('span');
        selected_list.id = id + '-selected';
        span.appendChild(selected_list);
      break;
      case 'text-input':
        var elem = document.createElement('input');
        elem.type = 'text';
        elem.id = id;
        elem.innerHTML = innerHTML;
        span.appendChild(elem);
      break;
    }
    return span;
  }

  $.fn.showBlockProps = (block_id) => {
    document.getElementById('transform-props').replaceChildren();
    var type = block_data[block_id]['type'];

    // TODO: Dynamically build config picker here
    switch (type) {
      case 'join':
        var parents = $.fn.getBlockParents(block_id);
        if (parents.length != 2){
          window.alert('Joins must have two parents!')
        }
        else if ((block_data[parents[0]]['header'] == null || block_data[parents[0]]['data'] == null) || (block_data[parents[1]]['header'] == null || block_data[parents[1]]['data'] == null)){
          window.alert('You must run parent blocks before choosing join properties.');
        }
        else {
          var left_block_select_options = {};
          left_block_select_options[parents[0]] = $.fn.getBlockName(parents[0]);
          left_block_select_options[parents[1]] = $.fn.getBlockName(parents[1]);

          var left_block_select = $.fn.transformPropFactory('select', 'Left Table: ', '', 'left-block-select', '', left_block_select_options);

          document.getElementById('transform-props').appendChild(left_block_select);
          document.getElementById('transform-props').appendChild(document.createElement('BR'));

          document.getElementById('left-block-select').value = Object.keys(left_block_select_options)[1];
          if (block_data[block_id]['properties'] != null && Object.keys(left_block_select_options).includes(block_data[block_id]['properties']['left_block'])) {
              document.getElementById('left-block-select').value = block_data[block_id]['properties']['left_block'];
          }

          var right_block_select_options = {};
          right_block_select_options[parents[0]] = $.fn.getBlockName(parents[0]);
          right_block_select_options[parents[1]] = $.fn.getBlockName(parents[1]);

          var right_block_select = $.fn.transformPropFactory('select', 'Right Table: ', '', 'right-block-select', '', right_block_select_options);

          document.getElementById('transform-props').appendChild(right_block_select);
          document.getElementById('transform-props').appendChild(document.createElement('BR'));

          document.getElementById('right-block-select').value = Object.keys(right_block_select_options)[1];
          if (block_data[block_id]['properties'] != null && Object.keys(right_block_select_options).includes(block_data[block_id]['properties']['right_block'])){
              document.getElementById('right-block-select').value = block_data[block_id]['properties']['right_block'];
          }

          var left_key_select_options = {};
          if ( block_data[document.getElementById('left-block-select').value]['header'] != null) {
            for (const header_item of block_data[document.getElementById('left-block-select').value]['header']){
              left_key_select_options[header_item] = header_item;
            }
          }

          var left_key_select = $.fn.transformPropFactory('select', 'Left Key: ', '', 'left-key-select', '', left_key_select_options);

          document.getElementById('transform-props').appendChild(left_key_select);
          document.getElementById('transform-props').appendChild(document.createElement('BR'));

          document.getElementById('left-key-select').addEventListener('change', function() {
            block_data[block_id]['properties']['left_key'] = this.value;
            $.fn.blockPropsUpdated(block_id);
          });

          document.getElementById('left-block-select').addEventListener('change', function(){
            left_key_select_options = [];
            document.getElementById('left-key-select').replaceChildren();
            for (const header_item of block_data[this.value]['header']){
              left_key_select_options[header_item] = header_item;
              var left_key_select_option = document.createElement('option');
              left_key_select_option.value = header_item;
              left_key_select_option.innerHTML = header_item;
              document.getElementById('left-key-select').appendChild(left_key_select_option);
            }
            block_data[block_id]['properties']['left_block'] = this.value;
            $.fn.blockPropsUpdated(block_id);
          });

          document.getElementById('left-key-select').value = Object.keys(left_key_select_options)[0];
          if (block_data[block_id]['properties'] != null && Object.keys(left_key_select_options).includes(block_data[block_id]['properties']['left_key'])){
              document.getElementById('left-key-select').value = block_data[block_id]['properties']['left_key'];
          }

          var right_key_select_options = {};
          if ( block_data[document.getElementById('right-block-select').value]['header'] != null) {
            for (const header_item of block_data[document.getElementById('right-block-select').value]['header']){
              right_key_select_options[header_item] = header_item;
            }
          }

          var right_key_select = $.fn.transformPropFactory('select', 'Right Key: ', '', 'right-key-select', '', right_key_select_options);

          document.getElementById('transform-props').appendChild(right_key_select);
          document.getElementById('transform-props').appendChild(document.createElement('BR'));

          document.getElementById('right-key-select').addEventListener('change', function() {
            block_data[block_id]['properties']['right_key'] = this.value;
            $.fn.blockPropsUpdated(block_id);
          });

          document.getElementById('right-block-select').addEventListener('change', function(){
            right_key_select_options = [];
            document.getElementById('right-key-select').replaceChildren();
            for (const header_item of block_data[this.value]['header']){
              right_key_select_options[header_item] = header_item;
              var right_key_select_option = document.createElement('option');
              right_key_select_option.value = header_item;
              right_key_select_option.innerHTML = header_item;
              document.getElementById('right-key-select').appendChild(right_key_select_option);
            }
            block_data[block_id]['properties']['right_block'] = this.value;
            $.fn.blockPropsUpdated(block_id);
          });

          document.getElementById('right-key-select').value = Object.keys(right_key_select_options)[0];
          if (block_data[block_id]['properties'] != null && Object.keys(right_key_select_options).includes(block_data[block_id]['properties']['right_key'])){
              document.getElementById('right-key-select').value = block_data[block_id]['properties']['right_key'];
          }

          var method_select_options = {'left':'left', 'right':'right', 'outer':'outer', 'inner':'inner'};
          var method_select = $.fn.transformPropFactory('select', 'Method: ', '', 'method-select', '', method_select_options);

          document.getElementById('transform-props').appendChild(method_select);

          document.getElementById('method-select').value = Object.keys(method_select_options)[0];
          if (block_data[block_id]['properties'] != null && Object.keys(method_select_options).includes(block_data[block_id]['properties']['method'])){
              document.getElementById('method-select').value = block_data[block_id]['properties']['method'];
          }

          document.getElementById('method-select').addEventListener('change', function(){
            block_data[block_id]['properties']['method'] = this.value;
            $.fn.blockPropsUpdated(block_id);
          })

          block_data[block_id]['properties']['left_block'] = document.getElementById('left-block-select').value;
          block_data[block_id]['properties']['right_block'] = document.getElementById('right-block-select').value;
          block_data[block_id]['properties']['left_key'] = document.getElementById('left-key-select').value;
          block_data[block_id]['properties']['right_key'] = document.getElementById('right-key-select').value;
          block_data[block_id]['properties']['method'] = document.getElementById('method-select').value;
          $.fn.blockPropsUpdated(block_id);
        }
      break;
      case 'filter-rows':
        var parents = $.fn.getBlockParents(block_id);
        if (parents.length != 1){
          window.alert('Row filter must have one parent!');
        }
        else if (block_data[parents[0]]['header'] == null || block_data[parents[0]]['data'] == null){
          window.alert('You must run parent block before choosing row filter properties.');
        }
        else {
          var parent_id = parents[0];

          var column_select_options = {};
          if ( block_data[parent_id]['header'] != null) {
            for (const header_item of block_data[parent_id]['header']){
              column_select_options[header_item] = header_item;
            }
          }


          // TODO: sort by actual column order
          var column_select = $.fn.transformPropFactory('select', 'Column: ', '', 'column-select', '', column_select_options);

          document.getElementById('transform-props').appendChild(column_select);
          document.getElementById('transform-props').appendChild(document.createElement('BR'));

          document.getElementById('column-select').value = Object.keys(column_select_options)[0];
          if (block_data[block_id]['properties'] != null && Object.keys(column_select_options).includes(block_data[block_id]['properties']['filter_column'])) {
            document.getElementById('column-select').value = block_data[block_id]['properties']['filter_column'];
          }

          var operator_select_options = {};
          operator_select_options['='] = '=';
          operator_select_options['<'] = '<';
          operator_select_options['>'] = '>';
          operator_select_options['<='] = '<=';
          operator_select_options['>='] = '>=';

          var operator_select = $.fn.transformPropFactory('select', 'Operator: ', '', 'operator-select', '', operator_select_options);

          document.getElementById('transform-props').appendChild(operator_select);
          document.getElementById('transform-props').appendChild(document.createElement('BR'));

          document.getElementById('operator-select').value = Object.keys(operator_select_options)[0];
          if (block_data[block_id]['properties'] != null && Object.keys(operator_select_options).includes(block_data[block_id]['properties']['operator'])) {
            document.getElementById('operator-select').value = block_data[block_id]['properties']['operator'];
          }

          var value_input = $.fn.transformPropFactory('text-input', 'Value (use comma separated vals in [] for multiple): ', '', 'value-select', '');

          document.getElementById('transform-props').appendChild(value_input);
          document.getElementById('transform-props').appendChild(document.createElement('BR'));

          document.getElementById('value-select').value = '';
          if (block_data[block_id]['properties'] != null && block_data[block_id]['properties']['value'] != null) {
            document.getElementById('value-select').value = block_data[block_id]['properties']['value'];
          }

          document.getElementById('column-select').addEventListener('change', function() {
            block_data[block_id]['properties']['filter_column'] = this.value;
            $.fn.blockPropsUpdated(block_id);
          });

          document.getElementById('operator-select').addEventListener('change', function() {
            block_data[block_id]['properties']['operator'] = this.value;
            $.fn.blockPropsUpdated(block_id);
          });

          document.getElementById('value-select').addEventListener('keyup', function() {
            block_data[block_id]['properties']['value'] = this.value;
            $.fn.blockPropsUpdated(block_id);
          });

          block_data[block_id]['properties']['filter_column'] = document.getElementById('column-select').value;
          block_data[block_id]['properties']['operator'] = document.getElementById('operator-select').value;
          block_data[block_id]['properties']['value'] = document.getElementById('value-select').value;
        }
      break;
      case 'select-rows':
        var parents = $.fn.getBlockParents(block_id);
        if (parents.length != 1){
          window.alert('Row select must have one parent!');
        }
        else if (block_data[parents[0]]['header'] == null || block_data[parents[0]]['data'] == null){
          window.alert('You must run parent block before choosing row order properties.');
        }
        else {
          var parent_id = parents[0];

          var row_input = $.fn.transformPropFactory('text-input', 'Row numbers (separate with commas, use start:end for ranges): ', '', 'row-select', '');

          document.getElementById('transform-props').appendChild(row_input);
          document.getElementById('transform-props').appendChild(document.createElement('BR'));

          document.getElementById('row-select').value = '';
          if (block_data[block_id]['properties'] != null && block_data[block_id]['properties']['rows'] != null) {
            document.getElementById('row-select').value = block_data[block_id]['properties']['rows'];
          }

          document.getElementById('row-select').addEventListener('keyup', function() {
            block_data[block_id]['properties']['rows'] = this.value;
            $.fn.blockPropsUpdated(block_id);
          });

        }
      break;
      case 'order':
        var parents = $.fn.getBlockParents(block_id);
        if (parents.length != 1){
          window.alert('Row order must have one parent!');
        }
        else if (block_data[parents[0]]['header'] == null || block_data[parents[0]]['data'] == null){
          window.alert('You must run parent block before choosing row order properties.');
        }
        else {
          var parent_id = parents[0];

          var column_select_options = {};
          if ( block_data[parent_id]['header'] != null) {
            for (const header_item of block_data[parent_id]['header']){
              column_select_options[header_item] = header_item;
            }
          }

          // TODO: sort by actual column order
          var column_select = $.fn.transformPropFactory('multi-select', 'Select Column(s) to Order By: ', '', 'column-select', '', column_select_options);

          document.getElementById('transform-props').appendChild(column_select);
          document.getElementById('transform-props').appendChild(document.createElement('BR'));

          document.getElementById('column-select').value = Object.keys(column_select_options)[0];

          document.getElementById('column-select-selected').innerHTML = block_data[block_id]['properties']['order_columns'];

          document.getElementById('column-select-add').addEventListener('click', function(){
            var column_select_val = document.getElementById('column-select').value;
            if(!block_data[block_id]['properties']['order_columns'].includes(column_select_val)) {
              block_data[block_id]['properties']['order_columns'].push(column_select_val);
              $.fn.blockPropsUpdated(block_id);
            }
            if(!column_select_val in block_data[block_id]['properties']['asc_desc']) {
              block_data[block_id]['properties']['asc_desc'][column_select_val] = null;
              $.fn.blockPropsUpdated(block_id);
            }
            document.getElementById('column-select-selected').innerHTML = block_data[block_id]['properties']['order_columns'];
            $.fn.createAscDescElems();
          });

          document.getElementById('column-select-remove').addEventListener('click', function(){
            var column_select_val = document.getElementById('column-select').value;
            if (block_data[block_id]['properties']['order_columns'].includes(column_select_val)){
              var index = block_data[block_id]['properties']['order_columns'].indexOf(column_select_val);
              block_data[block_id]['properties']['order_columns'].splice(index, 1);
              $.fn.blockPropsUpdated(block_id);
            }
            if(column_select_val in block_data[block_id]['properties']['asc_desc']) {
              delete block_data[block_id]['properties']['asc_desc'][column_select_val];
              $.fn.blockPropsUpdated(block_id);
            }
            document.getElementById('column-select-selected').innerHTML = block_data[block_id]['properties']['order_columns'];
            $.fn.createAscDescElems();
          });

          $.fn.createAscDescElems = () => {
            if (!document.getElementById('asc-desc-elems')) {
              var asc_desc_elems = document.createElement('div');
              asc_desc_elems.id = 'asc-desc-elems';
              document.getElementById('transform-props').appendChild(asc_desc_elems);
            } else {
              document.getElementById('asc-desc-elems').replaceChildren();
            }

            for (const column of block_data[block_id]['properties']['order_columns']) {
              var asc_desc_options = {};
              asc_desc_options['asc'] = 'asc';
              asc_desc_options['desc'] = 'desc';
              var asc_desc = $.fn.transformPropFactory('select', column + ': ', '', 'asc-desc-select-' + column, '', asc_desc_options);
              document.getElementById('asc-desc-elems').appendChild(asc_desc);
              document.getElementById('asc-desc-elems').appendChild(document.createElement('BR'));

              if ( column in block_data[block_id]['properties']['asc_desc'] ) {
                document.getElementById('asc-desc-select-' + column).value = block_data[block_id]['properties']['asc_desc'][column];
              } else{
                block_data[block_id]['properties']['asc_desc'][column] = Object.keys(asc_desc_options)[0];
                $.fn.blockPropsUpdated(block_id);
                document.getElementById('asc-desc-select-' + column).value = block_data[block_id]['properties']['asc_desc'][column];
              }

              document.getElementById('asc-desc-select-' + column).addEventListener('change', function(){
                block_data[block_id]['properties']['asc_desc'][column] = this.value;
                $.fn.blockPropsUpdated(block_id);
              });
            }
          }

          $.fn.createAscDescElems();
        }
      break;
      case 'count-items':
        var parents = $.fn.getBlockParents(block_id);
        if (parents.length != 1){
          window.alert('Row filter must have one parent!');
        }
        else if (block_data[parents[0]]['header'] == null || block_data[parents[0]]['data'] == null){
          window.alert('You must run parent block before choosing drop column properties.');
        }
        else {
          var parent_id = parents[0];

          var column_select_options = {};
          if ( block_data[parent_id]['header'] != null) {
            for (const header_item of block_data[parent_id]['header']){
              column_select_options[header_item] = header_item;
            }
          }

          // TODO: sort by actual column order
          var column_select = $.fn.transformPropFactory('select', 'Column: ', '', 'column-select', '', column_select_options);

          document.getElementById('transform-props').appendChild(column_select);
          document.getElementById('transform-props').appendChild(document.createElement('BR'));

          document.getElementById('column-select').value = Object.keys(column_select_options)[0];

          if ( block_data[block_id]['properties']['count_column'] != null ) {
            document.getElementById('column-select').value = block_data[block_id]['properties']['count_column'];
          }

          document.getElementById('column-select').addEventListener('change', function() {
            block_data[block_id]['properties']['count_column'] = this.value;
            $.fn.blockPropsUpdated(block_id);
          })

          var delimiter_input = $.fn.transformPropFactory('text-input', 'Delimiter: ', '', 'delimiter-input', '');

          document.getElementById('transform-props').appendChild(delimiter_input);
          document.getElementById('transform-props').appendChild(document.createElement('BR'));

          document.getElementById('delimiter-input').value = '';
          if (block_data[block_id]['properties']['delimiter'] != null){
            document.getElementById('delimiter-input').value = block_data[block_id]['properties']['delimiter'];
          }

          document.getElementById('delimiter-input').addEventListener('keyup', function() {
            block_data[block_id]['properties']['delimiter'] = this.value;
            $.fn.blockPropsUpdated(block_id);
          });

          block_data[block_id]['properties']['count_column'] = document.getElementById('column-select').value;
          block_data[block_id]['properties']['delimiter'] = document.getElementById('delimiter-input').value;
        }
      break;
      case 'drop-columns':
        var parents = $.fn.getBlockParents(block_id);
        if (parents.length != 1){
          window.alert('Row filter must have one parent!');
        }
        else if (block_data[parents[0]]['header'] == null || block_data[parents[0]]['data'] == null){
          window.alert('You must run parent block before choosing drop column properties.');
        }
        else {
          var parent_id = parents[0];

          var column_select_options = {};
          if ( block_data[parent_id]['header'] != null) {
            for (const header_item of block_data[parent_id]['header']){
              column_select_options[header_item] = header_item;
            }
          }

          // TODO: sort by actual column order
          var column_select = $.fn.transformPropFactory('multi-select', 'Select Column(s) to Drop: ', '', 'column-select', '', column_select_options);

          document.getElementById('transform-props').appendChild(column_select);
          document.getElementById('transform-props').appendChild(document.createElement('BR'));

          document.getElementById('column-select').value = Object.keys(column_select_options)[0];
          document.getElementById('column-select-selected').innerHTML = block_data[block_id]['properties']['columns'];

          document.getElementById('column-select-add').addEventListener('click', function(){
            var column_select_val = document.getElementById('column-select').value;
            if(!block_data[block_id]['properties']['columns'].includes(column_select_val)) {
              block_data[block_id]['properties']['columns'].push(column_select_val);
              $.fn.blockPropsUpdated(block_id);
            }
            document.getElementById('column-select-selected').innerHTML = block_data[block_id]['properties']['columns'];
          });

          document.getElementById('column-select-remove').addEventListener('click', function(){
            var column_select_val = document.getElementById('column-select').value;
            if (block_data[block_id]['properties']['columns'].includes(column_select_val)){
              var index = block_data[block_id]['properties']['columns'].indexOf(column_select_val);
              block_data[block_id]['properties']['columns'].splice(index, 1);
              $.fn.blockPropsUpdated(block_id);
            }
            document.getElementById('column-select-selected').innerHTML = block_data[block_id]['properties']['columns'];
          });
        }
      break;
      case 'group-by':
        var parents = $.fn.getBlockParents(block_id);
        if (parents.length != 1){
          window.alert('Row filter must have one parent!');
        }
        else if (block_data[parents[0]]['header'] == null || block_data[parents[0]]['data'] == null){
          window.alert('You must run parent block before choosing group by properties.');
        }
        else {
          var parent_id = parents[0];

          var column_select_options = {};
          if ( block_data[parent_id]['header'] != null) {
            for (const header_item of block_data[parent_id]['header']){
              column_select_options[header_item] = header_item;
            }
          }

          // TODO: sort by actual column order
          var group_column_select = $.fn.transformPropFactory('multi-select', 'Select Column(s) to Group By: ', '', 'group-select', '', column_select_options);

          document.getElementById('transform-props').appendChild(group_column_select);
          document.getElementById('transform-props').appendChild(document.createElement('BR'));

          document.getElementById('group-select').value = Object.keys(column_select_options)[0];
          document.getElementById('group-select-selected').innerHTML = block_data[block_id]['properties']['group_columns'];

          document.getElementById('group-select-add').addEventListener('click', function(){
            var group_select_val = document.getElementById('group-select').value;
            if(!block_data[block_id]['properties']['group_columns'].includes(group_select_val)) {
              block_data[block_id]['properties']['group_columns'].push(group_select_val);
              $.fn.blockPropsUpdated(block_id);
            }
            document.getElementById('group-select-selected').innerHTML = block_data[block_id]['properties']['group_columns'];
          });

          document.getElementById('group-select-remove').addEventListener('click', function(){
            var group_select_val = document.getElementById('group-select').value;
            if (block_data[block_id]['properties']['group_columns'].includes(group_select_val)){
              var index = block_data[block_id]['properties']['group_columns'].indexOf(group_select_val);
              block_data[block_id]['properties']['group_columns'].splice(index, 1);
              $.fn.blockPropsUpdated(block_id);
            }
            document.getElementById('group-select-selected').innerHTML = block_data[block_id]['properties']['group_columns'];
          });

          var agg_column_select = $.fn.transformPropFactory('multi-select', 'Select Column(s) to Aggregate On: ', '', 'agg-select', '', column_select_options);

          document.getElementById('transform-props').appendChild(agg_column_select);
          document.getElementById('transform-props').appendChild(document.createElement('BR'));

          document.getElementById('agg-select').value = Object.keys(column_select_options)[0];
          document.getElementById('agg-select-selected').innerHTML = block_data[block_id]['properties']['agg_columns'];

          document.getElementById('agg-select-add').addEventListener('click', function(){
            var agg_select_val = document.getElementById('agg-select').value;
            if(!block_data[block_id]['properties']['agg_columns'].includes(agg_select_val)) {
              block_data[block_id]['properties']['agg_columns'].push(agg_select_val);
              $.fn.blockPropsUpdated(block_id);
            }
            if(!agg_select_val in block_data[block_id]['properties']['agg_columns']) {
              block_data[block_id]['properties']['agg_functions'][agg_select_val] = null;
              $.fn.blockPropsUpdated(block_id);
            }
            document.getElementById('agg-select-selected').innerHTML = block_data[block_id]['properties']['agg_columns'];
            $.fn.createAggElems();
          });

          document.getElementById('agg-select-remove').addEventListener('click', function(){
            var agg_select_val = document.getElementById('agg-select').value;
            if (block_data[block_id]['properties']['agg_columns'].includes(agg_select_val)){
              var index = block_data[block_id]['properties']['agg_columns'].indexOf(agg_select_val);
              block_data[block_id]['properties']['agg_columns'].splice(index, 1);
              $.fn.blockPropsUpdated(block_id);
            }
            if(agg_select_val in block_data[block_id]['properties']['agg_functions']) {
              delete block_data[block_id]['properties']['agg_functions'][agg_select_val];
              $.fn.blockPropsUpdated(block_id);
            }
            document.getElementById('agg-select-selected').innerHTML = block_data[block_id]['properties']['agg_columns'];
            $.fn.createAggElems();
          });

          $.fn.createAggElems = () => {
            if (!document.getElementById('agg-elems')) {
              var agg_elems = document.createElement('div');
              agg_elems.id = 'agg-elems';
              document.getElementById('transform-props').appendChild(agg_elems);
            } else {
              document.getElementById('agg-elems').replaceChildren();
            }

            for (const column of block_data[block_id]['properties']['agg_columns']) {
              var agg_options = {};
              agg_options['count'] = 'count';
              agg_options['sum'] = 'sum';
              agg_options['mean'] = 'mean';
              agg_options['min'] = 'min';
              agg_options['max'] = 'max';
              var agg = $.fn.transformPropFactory('select', column + ': ', '', 'agg-select-' + column, '', agg_options);
              document.getElementById('agg-elems').appendChild(agg);
              document.getElementById('agg-elems').appendChild(document.createElement('BR'));

              if ( column in block_data[block_id]['properties']['agg_functions'] ) {
                document.getElementById('agg-select-' + column).value = block_data[block_id]['properties']['agg_functions'][column];
              } else{
                block_data[block_id]['properties']['agg_functions'][column] = Object.keys(agg_options)[0];
                $.fn.blockPropsUpdated(block_id);
                document.getElementById('agg-select-' + column).value = block_data[block_id]['properties']['agg_functions'][column];
              }

              document.getElementById('agg-select-' + column).addEventListener('change', function(){
                block_data[block_id]['properties']['agg_functions'][column] = this.value;
                $.fn.blockPropsUpdated(block_id);
              });
            }
          }

          $.fn.createAggElems();
        }
      break;
      case 'sum-columns':
        var parents = $.fn.getBlockParents(block_id);
        if (parents.length != 1){
          window.alert('Sum columns must have one parent!');
        }
      break;
      case 'pivot-table':
        var parents = $.fn.getBlockParents(block_id);
        if (parents.length != 1){
          window.alert('Row filter must have one parent!');
        }
        else if (block_data[parents[0]]['header'] == null || block_data[parents[0]]['data'] == null){
          window.alert('You must run parent block before choosing pivot table properties.');
        }
        else {
          var parent_id = parents[0];

          var column_select_options = {};
          if ( block_data[parent_id]['header'] != null) {
            for (const header_item of block_data[parent_id]['header']){
              column_select_options[header_item] = header_item;
            }
          }

          // TODO: sort by actual column order
          var values_column_select = $.fn.transformPropFactory('multi-select', 'Select Value Column(s): ', '', 'values-columns-select', '', column_select_options);

          document.getElementById('transform-props').appendChild(values_column_select);
          document.getElementById('transform-props').appendChild(document.createElement('BR'));

          document.getElementById('values-columns-select').value = Object.keys(column_select_options)[0];
          document.getElementById('values-columns-select-selected').innerHTML = block_data[block_id]['properties']['values_columns'];

          document.getElementById('values-columns-select-add').addEventListener('click', function(){
            var values_column_select_val = document.getElementById('values-columns-select').value;
            if(!block_data[block_id]['properties']['values_columns'].includes(values_column_select_val)) {
              block_data[block_id]['properties']['values_columns'].push(values_column_select_val);
              $.fn.blockPropsUpdated(block_id);
            }
            if (!values_column_select_val in block_data[block_id]['properties']['agg_functions']) {
              block_data[block_id]['properties']['agg_functions'][values_column_select_val] = null;
              $.fn.blockPropsUpdated(block_id);
            }
            document.getElementById('values-columns-select-selected').innerHTML = block_data[block_id]['properties']['values_columns'];
            $.fn.createAggElems();
          });

          document.getElementById('values-columns-select-remove').addEventListener('click', function(){
            var values_column_select_val = document.getElementById('values-columns-select').value;
            if (block_data[block_id]['properties']['values_columns'].includes(values_column_select_val)){
              var index = block_data[block_id]['properties']['values_columns'].indexOf(values_column_select_val);
              block_data[block_id]['properties']['values_columns'].splice(index, 1);
              $.fn.blockPropsUpdated(block_id);
            }
            if (values_column_select_val in block_data[block_id]['properties']['agg_functions']) {
              delete block_data[block_id]['properties']['agg_functions'][values_column_select_val];
              $.fn.blockPropsUpdated(block_id);
            }
            document.getElementById('values-columns-select-selected').innerHTML = block_data[block_id]['properties']['values_columns'];
            $.fn.createAggElems();
          });

          // TODO: sort by actual column order
          var index_column_select = $.fn.transformPropFactory('multi-select', 'Select New Index(es): ', '', 'index-columns-select', '', column_select_options);

          document.getElementById('transform-props').appendChild(index_column_select);
          document.getElementById('transform-props').appendChild(document.createElement('BR'));

          document.getElementById('index-columns-select').value = Object.keys(column_select_options)[0];
          document.getElementById('index-columns-select-selected').innerHTML = block_data[block_id]['properties']['index_columns'];

          document.getElementById('index-columns-select-add').addEventListener('click', function(){
            var index_column_select_val = document.getElementById('index-columns-select').value;
            if(!block_data[block_id]['properties']['index_columns'].includes(index_column_select_val)) {
              block_data[block_id]['properties']['index_columns'].push(index_column_select_val);
              $.fn.blockPropsUpdated(block_id);
            }
            document.getElementById('index-columns-select-selected').innerHTML = block_data[block_id]['properties']['index_columns'];
          });

          document.getElementById('index-columns-select-remove').addEventListener('click', function(){
            var index_column_select_val = document.getElementById('index-columns-select').value;
            if (block_data[block_id]['properties']['index_columns'].includes(index_column_select_val)){
              var index = block_data[block_id]['properties']['index_columns'].indexOf(index_column_select_val);
              block_data[block_id]['properties']['index_columns'].splice(index, 1);
              $.fn.blockPropsUpdated(block_id);
            }
            document.getElementById('index-columns-select-selected').innerHTML = block_data[block_id]['properties']['index_columns'];
          });

          // TODO: sort by actual column order
          var new_column_select = $.fn.transformPropFactory('multi-select', 'Select New Column(s): ', '', 'new-columns-select', '', column_select_options);

          document.getElementById('transform-props').appendChild(new_column_select);
          document.getElementById('transform-props').appendChild(document.createElement('BR'));

          document.getElementById('new-columns-select').value = Object.keys(column_select_options)[0];
          document.getElementById('new-columns-select-selected').innerHTML = block_data[block_id]['properties']['new_columns'];

          document.getElementById('new-columns-select-add').addEventListener('click', function(){
            var new_column_select_val = document.getElementById('new-columns-select').value;
            if(!block_data[block_id]['properties']['new_columns'].includes(new_column_select_val)) {
              block_data[block_id]['properties']['new_columns'].push(new_column_select_val);
              $.fn.blockPropsUpdated(block_id);
            }
            document.getElementById('new-columns-select-selected').innerHTML = block_data[block_id]['properties']['new_columns'];
          });

          document.getElementById('new-columns-select-remove').addEventListener('click', function(){
            var new_column_select_val = document.getElementById('new-columns-select').value;
            if (block_data[block_id]['properties']['new_columns'].includes(new_column_select_val)){
              var index = block_data[block_id]['properties']['new_columns'].indexOf(new_column_select_val);
              block_data[block_id]['properties']['new_columns'].splice(index, 1);
              $.fn.blockPropsUpdated(block_id);
            }
            document.getElementById('new-columns-select-selected').innerHTML = block_data[block_id]['properties']['new_columns'];
          });

          $.fn.createAggElems = () => {
            if (!document.getElementById('agg-elems')) {
              var agg_elems = document.createElement('div');
              agg_elems.id = 'agg-elems';
              document.getElementById('transform-props').appendChild(agg_elems);
            } else {
              document.getElementById('agg-elems').replaceChildren();
            }

            for (const column of block_data[block_id]['properties']['values_columns']) {
              var agg_options = {};
              agg_options['count'] = 'count';
              agg_options['sum'] = 'sum';
              agg_options['mean'] = 'mean';
              agg_options['min'] = 'min';
              agg_options['max'] = 'max';
              var agg = $.fn.transformPropFactory('select', column + ': ', '', 'agg-select-' + column, '', agg_options);
              document.getElementById('agg-elems').appendChild(agg);
              document.getElementById('agg-elems').appendChild(document.createElement('BR'));

              if ( column in block_data[block_id]['properties']['agg_functions'] ) {
                document.getElementById('agg-select-' + column).value = block_data[block_id]['properties']['agg_functions'][column];
              } else{
                block_data[block_id]['properties']['agg_functions'][column] = Object.keys(agg_options)[0];
                $.fn.blockPropsUpdated(block_id);
                document.getElementById('agg-select-' + column).value = block_data[block_id]['properties']['agg_functions'][column];
              }

              document.getElementById('agg-select-' + column).addEventListener('change', function(){
                block_data[block_id]['properties']['agg_functions'][column] = this.value;
                $.fn.blockPropsUpdated(block_id);
              });
            }
          }

          $.fn.createAggElems();
        }
      break;
    }
  }

  $.fn.setBlockStroke = function(block_id, color){
    $.fn.getBlockChild(block_id, 'box').stroke(color);
  }

  $.fn.toggleSelectedBlock = function(block_id) {
    if (selected_block_id != block_id){
      if (selected_block_id != null && selected_block_id in blocks){
        $.fn.setBlockStroke(selected_block_id, 'black');
      }
      $.fn.setBlockStroke(block_id, 'yellow');
      selected_block_id = block_id;
      $.fn.displayBlockState(block_id);
      $.fn.showBlockProps(block_id);
    }
    else {
      $.fn.setBlockStroke(block_id, 'black');
      selected_block_id = null;
      $.fn.displayBlockState(block_id);
      $.fn.clearTable();
      $.fn.hideBlockProps();
    }

    if (selected_block_id == null) {
      var name = '';
    }
    else {
      var name = $.fn.getBlockName(selected_block_id);
    }
    document.getElementById('selected-block').innerHTML = 'Selected Block: ' + name;
  }

  $.fn.validateBlocks = function(block_set) {
    var block_errors = {};
    for (const block_id of Object.keys(block_set)) {
      var type = block_data[block_id]['type'];
      switch (type) {
        case 'join':
          if (block_data[block_id]['properties'] == null || block_data[block_id]['properties']['left_block'] == null || block_data[block_id]['properties']['left_key'] == null || block_data[block_id]['properties']['right_block'] == null || block_data[block_id]['properties']['right_key'] == null) {
            block_errors[block_id] = 'You must select join properties.';
          }
        break;
        case 'filter-rows':
          if (block_data[block_id]['properties'] == null || block_data[block_id]['properties']['filter_column'] == null || block_data[block_id]['properties']['operator'] == null || block_data[block_id]['properties']['value'] == null) {
            block_errors[block_id] = 'You must select row filter properties.';
          }
          else if (block_data[block_id]['properties']['operator'] != '=' && (block_data[block_id]['properties']['value'].charAt(0) == '[' && block_data[block_id]['properties']['value'].charAt(block_data[block_id]['properties']['value'].length - 1) == ']')){
            block_errors[block_id] = 'You cannot select multiple values unless using = operator.';
          }
        break;
        case 'select-rows':
          if (block_data[block_id]['properties'] == null || block_data[block_id]['properties']['rows'] == null || block_data[block_id]['properties']['rows'] == '') {
            block_errors[block_id] = 'You must choose row select properties.';
          }
        break;
        case 'order':
          if (block_data[block_id]['properties'] == null || (block_data[block_id]['properties']['order_columns'] == null || block_data[block_id]['properties']['order_columns'].length == 0) || (block_data[block_id]['properties']['asc_desc'] == null || Object.keys(block_data[block_id]['properties']['asc_desc']).length == 0)) {
            block_errors[block_id] = 'You must select row filter properties.';
          }
        break;
        case 'count-items':
          if (block_data[block_id]['properties']['delimiter'] == null || block_data[block_id]['properties'] == null || block_data[block_id]['properties']['delimiter'] == null || block_data[block_id]['properties']['delimiter'] == '') {
            block_errors[block_id] = 'You must select item count properties.';
          }
        break;
        case 'drop-columns':
          if (block_data[block_id]['properties'] == null || block_data[block_id]['properties']['columns'].length == 0) {
            block_errors[block_id] = 'You must select drop column properties.';
          }
        break;
        case 'pivot-table':
          if (block_data[block_id]['properties'] == null || block_data[block_id]['properties']['index_columns'].length == 0 || block_data[block_id]['properties']['new_columns'].length == 0) {
            block_errors[block_id] = 'You must select pivot table properties.';
          }
        break;
      }
    }
    return block_errors;
  }

  $.fn.compileModel = (block_set=null, edge_set=null) => {
    // we want to be able to pass model subsets to the compiler
    if (block_set == null) {
      block_set = blocks;
    }
    if (edge_set == null) {
      edge_set = edges;
    }
    var block_errors = $.fn.validateBlocks(block_set);
    if ( Object.keys(block_errors).length > 0 ) {
      // TODO: we want to change this behavior, likely updating the blocks error message
      var alert_text = '';
      for ( const block_id of Object.keys(block_errors) ) {
        alert_text += $.fn.getBlockName(block_id) + ': ' + block_errors[block_id] + '\n';
      }
      window.alert(alert_text);
      return null;
    } else {
      var blocks_array = {};
      for (const block_id of Object.keys(block_set)) {
        blocks_array[block_id] = {
          'type': block_data[block_id]['type'],
          'properties': block_data[block_id]['properties'],
          'data-ref': block_data[block_id]['data-ref']
        }
      }

      var edges_array = {};
      for (const edge of edge_set) {
        edges_array[edge['connector']] = {
          'from': edge['from'],
          'to': edge['to']
        }
      }

      let model = {
        'model-id': model_id,
        'blocks': blocks_array,
        'edges': edges_array
      };

      return model;
    }
  }

  $( document ).on('click', '#compile-model', function() {
    var model = $.fn.compileModel();
    if ( model != null ) {
      $.fn.runModel(model);
    }
  })

  $.fn.displayRawAssets = (type) => {
    document.getElementById('data-select').replaceChildren();

    var user_file_label = document.createElement('option');
    user_file_label.disabled = true;
    user_file_label.innerHTML = 'User Files'
    document.getElementById('data-select').append(user_file_label);

    var user_label_sep = document.createElement('option');
    user_label_sep.disabled = true;
    user_label_sep.innerHTML = '--------------------';
    document.getElementById('data-select').append(user_label_sep);

    for (var i = 0; i < assets.length; i++){
      if (assets[i].type == type && assets[i].scope == 'user') {
        var data_option = document.createElement('option');
        data_option.value = assets[i].path;
        data_option.innerHTML = assets[i].name;
        document.getElementById('data-select').append(data_option);
      }
    }

    var sample_file_label = document.createElement('option');
    sample_file_label.disabled = true;
    sample_file_label.innerHTML = 'Sample Files'
    document.getElementById('data-select').append(sample_file_label);

    var sample_label_sep = document.createElement('option');
    sample_label_sep.disabled = true;
    sample_label_sep.innerHTML = '--------------------';
    document.getElementById('data-select').append(sample_label_sep);

    for (var i = 0; i < assets.length; i++){
      if (assets[i].type == type && assets[i].scope == 'sample') {
        var data_option = document.createElement('option');
        data_option.value = assets[i].path;
        data_option.innerHTML = assets[i].name;
        document.getElementById('data-select').append(data_option);
      }
    }
  }

  $.fn.fetchRawAssets = () => {
    $.get( API_BASE_URL + '/list-raw-assets', function(data) {
        for (var i = 0; i < data.length; i++){
          var path = data[i];
          var name = data[i].split('/')[data[i].split('/').length - 1];
          var scope = data[i].split('/')[data[i].split('/').length - 2] == 'sample_assets' ? 'sample' : 'user';
          var type = data[i].split('.')[data[i].split('.').length - 1];
          if ( name != '.DS_Store' ) {
            assets.push({'path': path, 'name': name, 'scope': scope, 'type': type});
          }
        }
        $.fn.displayAssetSelectors(); // TODO: I'd rather not have this here.
    })
  }

  $.fn.loadRawAsset = (block_id, asset, type) => {
    $.get( API_BASE_URL + '/get-raw-asset', {'asset': asset, 'type': type}, function(data) {
      block_data[block_id]['header'] = data['data_dict']['columns'];
      block_data[block_id]['data'] = data['data_dict']['data'];
      block_data[block_id]['summary'] = data['summary'];
      $.fn.blockDataUpdated(block_id);
    });
  }

  $.fn.saveUserAsset = (user_id, file_input) => {
    var formData = new FormData()

    formData.append('user_id', user_id)
    formData.append('file_input', file_input);

    var data_ref = '';

    $.ajax({
      url: API_BASE_URL + '/save-user-file',
      type: 'POST',
      data: formData,
      processData: false,
      contentType: false,
      async: false,
      success: function(data){
        data_ref = data;
      },
      error: function(err) {
        console.log("Error uploading file:")
        console.log(err);
      }
    });

    return data_ref;
  }

  $.fn.runModel = (model) => {
    $.get( API_BASE_URL + '/run-model', { 'model': JSON.stringify(model) }, function(data) {
      for (const block_id in data[model_id]) {
        if(data[model_id][block_id]['error'] != null){
          block_data[block_id]['data-ref'] = null;
          block_data[block_id]['header'] = null;
          block_data[block_id]['data'] = null;
          block_data[block_id]['summary'] = null;
          block_data[block_id]['error'] = data[model_id][block_id]['error'];
        }
        else {
          block_data[block_id]['data-ref'] = data[model_id][block_id]['data-ref'];
          block_data[block_id]['header'] = data[model_id][block_id]['data']['columns'];
          block_data[block_id]['data'] = data[model_id][block_id]['data']['data'];
          block_data[block_id]['summary'] = data[model_id][block_id]['summary'];
          block_data[block_id]['error'] = null;
        }
        $.fn.blockDataUpdated(block_id);
      }
      if (selected_block_id != null ){
        $.fn.displayBlockState(selected_block_id);
      }
    });
  }

  // this grabs the blocks and edges upstream from (and including) the block id selected, and compiles the sub-model in the same way the full model is compiled
  $.fn.getUpstreamModel = (block_id) => {
    var blocks_to_include = {};
    blocks_to_include[block_id] = blocks[block_id];
    var edges_to_include = [];

    var block_ids_to_process = [block_id];
    var processing;
    while (block_ids_to_process.length > 0) {
      processing = block_ids_to_process.pop();
      for (const edge of edges) {
        if (edge['to'] == processing) {
          if (!edges_to_include.includes(edge)) {
            edges_to_include.push(edge);
          }
          if (!(edge['from'] in blocks_to_include)) {
            blocks_to_include[edge['from']] = blocks[edge['from']];
            block_ids_to_process.push(edge['from']);
          }
        }
      }
    }

    var compiled_model = $.fn.compileModel(blocks_to_include, edges_to_include);
    return compiled_model;
  }

  // this gets the downstream block ids of the block id passed (not inclusive)
  $.fn.getDownstreamBlockIds = (block_id) => {
    var downstream_block_ids = [];
    var block_ids_to_process = [block_id];
    var processing;
    while (block_ids_to_process.length > 0) {
      processing = block_ids_to_process.pop();
      for (const edge of edges) {
        if (edge['from'] == processing) {
          if(!downstream_block_ids.includes(edge['to'])) {
            downstream_block_ids.push(edge['to']);
            block_ids_to_process.push(edge['to']);
          }
        }
      }
    }
    return downstream_block_ids;
  }

  $( document ).on('click', '#run-to-selected', function() {
    if (selected_block_id == null){
      window.alert("No block selected!");
    }
    else {
      var model = $.fn.getUpstreamModel(selected_block_id);
      if ( model != null ) {
        $.fn.runModel(model);
      }
    }
  });

  $.fn.startupSequence();
  $.fn.populateSavedModels();
  $.fn.fetchRawAssets();
});
