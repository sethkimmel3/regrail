# regrail

## What? 
Regrail is a visual data querying and transformation tool. It's meant to make shaping tabular data easier and more intuitive. 

## Why?
Data literacy is fundamental for human progress. While there are over a billion spreadsheet users, far fewer than 1% of them can write code. 

Regrail abstracts the primitives of data engineering to allow users to visually and procedurally work with data. The block-based, stateful editor allows users to see the changes they are making along the way. The end result is a model that can be used repeatably on new data like a script or query.

It's named after [GRAIL](https://www.rand.org/pubs/research_memoranda/RM6001.html), a visual data manipulation language created over 50 years ago (slightly ahead of its time). I learned about GRAIL in a [talk from Bret Victor](https://youtu.be/8pTEmbeENF4?t=1098) who also argues that [creators need to able to see changes in real time as they make them](https://youtu.be/PUv66718DII?t=825).

## How?
### Usage
Regrail is a browser application. It can be run locally or used as a cloud service at [regrail.io](https://regrail.io). 

To run it locally, install docker-compose, and run the following command in the terminal:

`docker-compose -f docker-compose.dev.yml -f docker-compose.yml up -d`

The application should be usable in the browser on `localhost`. You can upload your own csv and excel files to experiment. 

### Design

It currently uses [Konva](https://konvajs.org/) to render graphics on the frontend and [pandas](https://pandas.pydata.org/) to process models on the backend. When a user creates and runs a model, it compiles down to a JSON representation of the graph with references to the data objects. Each block represents both a step and a state. For example: 

```
{
    "model-id": "model-290212a7a9775",
    "blocks": {
        "block-22dfcf7cc8569": {
            "type": "csv-file",
            "properties": {},
            "data-ref": "model_assets/model-290212a7a9775/block-22dfcf7cc8569.snappy.parquet"
        },
        "block-a534ac022fb69": {
            "type": "filter-rows",
            "properties": {
                "filter_column": "1958",
                "operator": ">",
                "value": "350"
            },
            "data-ref": "model_assets/model-290212a7a9775/block-a534ac022fb69.snappy.parquet"
        },
        "block-39f4839d70ea8": {
            "type": "order",
            "properties": {
                "order_columns": [
                    "1958"
                ],
                "asc_desc": {
                    "1958": "asc"
                }
            },
            "data-ref": "model_assets/model-290212a7a9775/block-39f4839d70ea8.snappy.parquet"
        },
        "block-0135f85be80f1": {
            "type": "drop-columns",
            "properties": {
                "columns": [
                    "1959",
                    "1960"
                ]
            },
            "data-ref": "model_assets/model-290212a7a9775/block-0135f85be80f1.snappy.parquet"
        }
    },
    "edges": {
        "connector-4b258496feec3": {
            "from": "block-22dfcf7cc8569",
            "to": "block-a534ac022fb69"
        },
        "connector-4aae48c4bb12f": {
            "from": "block-a534ac022fb69",
            "to": "block-39f4839d70ea8"
        },
        "connector-486c358131ed": {
            "from": "block-39f4839d70ea8",
            "to": "block-0135f85be80f1"
        }
    }
}
```

The backend maps the model to the corresponding set of transformation steps and persists the intermediate data objects, passing them or samples back to the user.

This decoupled design is very intentional. The frontend is not particularly pretty or usable, and the backend can abstract any number of arbitrarily powerful, code-based data tools. These include vectorized/OLAP processing engines, serverless compute/storage resources, and machine learning/AI models. In other words, a much more powerful system can be built on this foundation.

## Future
I consider Regrail to be a rough prototype to demonstrate some ideas around how we can create data tools for the masses. 

I'm not sure that language models will ever bridge the gap between the capabilities of a spreadsheet user and a data scientist/engineer. On the other hand, I would expect spatial computing to become ubiqituous making visual data tooling more compelling for all types of users. Regrail also doesn't need to stop at tabular data, but can be used for stateful manipulation of other types of data, API's, and computing resources.

I think that a fully-featured, open-source, and free version of a tool like Regrail should be available to the public with more powerful features available for enterprises. If you're interested in contributing to or using some of the ideas found here, please reach out to me at [seth.kimmel3@gmail.com](mailto:seth.kimmel3@gmail.com). 