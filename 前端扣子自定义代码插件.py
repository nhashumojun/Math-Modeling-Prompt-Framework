
# 数模智能体地址：https://www.coze.cn/store/project/7548130644564688906
# QQ群：905027256

# 这是携带图片的模板，不携带图片就把image_urls删掉

from runtime import Args
from typings.tongyong1.tongyong1 import Input, Output
import requests
import json
import re

headers = {"content-type":"application/json"}

def handler(args: Args[Input])->Output:
    tishi = args.input.tishi
    text = args.input.text
    image_urls = args.input.image_urls
    ziduan = args.input.ziduan
    kami = args.input.kami
    TD = args.input.TD
    ku = args.input.ku
    address = args.input.address
    sk = args.input.sk
    model = args.input.model
    maxtoken = args.input.maxtoken

    url = "http://localhost:2011/get-ai-response"

    data = {
        "tishi":tishi,
        "user_content":text,
        "image_urls":image_urls,
        "ziduan":ziduan,
        "kami":kami,
        "TD":TD,
        "ku":ku,
        "address":address,
        "sk":sk,
        "model":model,
        "maxtoken":maxtoken
    }

    response = requests.post(url, headers=headers, data=json.dumps(data),timeout=590)
    result = response.json()

    out = result.get('data')
    usage = result.get('usage')

    try:
        out = re.sub(r'<think>.*?</think>', '', out, flags=re.DOTALL)
    except:
        ''
    try:
        out = re.sub(r'[\U00010000-\U0010FFFF]', '', out)
    except:
        ''

    ret: Output = {
        "out": out,
        "usage": usage
    }
    return ret
