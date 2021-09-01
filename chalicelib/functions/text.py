import json

def url_encoded_to_json(text):
    text = text.decode().replace('=','":"').replace("&", '","').replace("'", '"')
    jsondata = '{"' + text + '"}'
    return json.loads(jsondata)

def get_request_data(req):
    if req.json_body is None:
        return url_encoded_to_json(req.raw_body)
    else:
        return req.json_body