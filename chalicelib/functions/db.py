import boto3
from boto3.dynamodb.conditions import Key
from pprint import pprint

dynamodb = boto3.resource("dynamodb")
client = boto3.client("dynamodb")
paginator = client.get_paginator('scan')
query_paginator = client.get_paginator('query')

page_lengths = {
    'coconuts': 12,
    'bananas': 50,
    'default': 20
}

def scan(table):
    return dynamodb.Table(table).scan().get("Items", None)

def pscan(table, lastEvaluatedKey=None, per_page=None):
    if per_page:
        pageLength = int(per_page);
    elif table in page_lengths.keys():
        pageLength = page_lengths[table]
    else:
        pageLength = page_lengths['default']

    pagination_config = {
        'MaxItems': pageLength,
        'PageSize': pageLength
    }
    if lastEvaluatedKey:
        page_iterator = paginator.paginate(TableName=table, PaginationConfig=pagination_config, ExclusiveStartKey={"_id": {"S": lastEvaluatedKey}})
    else:
        page_iterator = paginator.paginate(TableName=table, PaginationConfig=pagination_config)

    data = {}
    for thing in page_iterator:
        data = thing
    return data

def qscan(table, parameters):
    kce = "category=:categoryAND_id=:_id"

    eav = {
        ':category': {
            "S": 'Research'
        },
        ':_id': {
            'NULL': False
        }
    }

    # kce = getKeyConditionExpression(parameters)
    print(kce)
    print("*"*40)
    page_iterator = query_paginator.paginate(TableName=table, KeyConditionExpression=kce, ExpressionAttributeValues=eav)

    data = {}
    for p in page_iterator:
        data = p
    return data


# Get single item from db
def get(table, id):
    return dynamodb.Table(table).query(KeyConditionExpression=Key("_id").eq(id)).get("Items", None)

# Post single item to db
def post(table, data):
    import uuid
    item = {'_id': uuid.uuid4().hex[:16]} # Make a unique id for the thing
    for key in data.keys():
        item[key] = data[key]
    try:
        dynamodb.Table(table).put_item(Item=item)
        return {'message':'success'}
    except Exception as e:
        return {'message': str(e)}

# Update single item in db. Note: One of the fields passed in the data needs to be '_id' because this function will parse it out and use it.
def update(table, data):
    updateExpression = ""
    expressionAttributeValues = {}
    for key in data.keys():
        if key != "_id":
            updateExpression += f"{key}=:{key},"
            expressionAttributeValues[":" + key] = data[key]
    try:
        dynamodb.Table(table).update_item(
            Key={"_id": data['_id']},
            UpdateExpression=f"set {updateExpression[:-1]}",
            ExpressionAttributeValues=expressionAttributeValues,
            ReturnValues="UPDATED_NEW")
        return {'message': "success"}
    except Exception as e:
        return {"message": str(e)}

# Delete single item in db
def delete(table, data):
    if not isinstance(data, list):
        try:
            dynamodb.Table(table).delete_item(Key={"_id": data['_id']})
            return {"message": "success"}
        except Exception as e:
            return {"message": str(e)}
    else:
        for item in data:
            try:
                dynamodb.Table(table).delete_item(Key={"_id": item['_id']})
            except Exception as e:
                return {"message": str(e)}
        return {'message':'success'}

# Get key conditions. Used for queries and filtering
def getKeyConditionExpression(data):
    if len(data) == 0:
        return None
    count = 0
    kce = ""
    for key in data.keys():
        if count <= 0:
            kce += f"{key} = :{data[key]}"
        else:
            kce += f"AND {key} = :{data[key]}"
    return kce

def getExpressionAttributeValues(data):
    return data
    