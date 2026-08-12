
# 数模智能体地址：https://www.coze.cn/store/project/7548130644564688906
# QQ群：905027256


from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
import json
from openai import AsyncOpenAI
from anthropic import AsyncAnthropic
import httpx
import base64
import mysql.connector
from starlette.concurrency import run_in_threadpool
import re
import asyncio
from datetime import datetime

# 数据库配置
db_config = {
    'host': '',
    'database': '',
    'user': '',
    'password': '',
    'charset': 'utf8mb4'
}

def execute_db_update(result, kami, TD, ziduan, model_name, ku):
    connection = None
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        # 动态字段名，用反引号包裹防止关键字冲突
        query = f"""
        UPDATE `{ku}`
        SET time = NOW(),
        `{ziduan}` = %s
        WHERE kami = %s AND TD = %s;
        """
        cursor.execute(query, (result, kami, TD))
        connection.commit()
        cursor.close()
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{current_time}] 内容已成功存入数据库 - 模型: {model_name}, 数据库: {ku}, 字段: {ziduan}, 长度：{len(result)}, 卡密: {kami}")
    finally:
        if connection and connection.is_connected():
            connection.close()

app = FastAPI()

timeout = 600

async def process_ai_task(address, sk, model, maxtoken, tishi, user_content, image_urls, kami, TD, ziduan, ku):
    try:
        is_claude = 'claude' in model.lower()

        # 在后端自动构造 messages 列表
        messages = []
        if tishi and not is_claude:
            messages.append({"role": "system", "content": tishi})
        if user_content or image_urls:
            messages.append({"role": "user", "content": user_content or ""})


        if is_claude:
            base_url = address
            if base_url.endswith('/v1'):
                base_url = base_url[:-3]
            elif base_url.endswith('/v1/'):
                base_url = base_url[:-4]
                
            client = AsyncAnthropic(
                api_key=sk,
                base_url=base_url
            )
        else:
            client = AsyncOpenAI(
                api_key=sk,
                base_url=address
            )

        if image_urls:
            image_content = []
            if is_claude:
                async with httpx.AsyncClient() as http_client:
                    for url in image_urls:
                        try:
                            response = await http_client.get(url,timeout=timeout)
                            response.raise_for_status()
                            media_type = response.headers.get('content-type', 'image/jpeg')
                            if not media_type.startswith('image/'):
                                media_type = 'image/jpeg'
                            base64_data = base64.b64encode(response.content).decode('utf-8')
                            image_content.append({
                                "type": "text",
                                "text": f"以下图片的链接/路径为: {url}"
                            })
                            image_content.append({
                                "type": "image",
                                "source": {
                                    "type": "base64",
                                    "media_type": media_type,
                                    "data": base64_data
                                }
                            })
                        except Exception as e:
                            print(f"获取图片失败: {url}, 错误: {e}")
            else:
                for url in image_urls:
                    image_content.append({
                        "type": "text",
                        "text": f"以下图片的链接/路径为: {url}"
                    })
                    image_content.append({
                        "type": "image_url",
                        "image_url": {
                            "url": url
                        }
                    })
            
            if image_content:
                # 因为前端保证传来的 user_content 必然是字符串，我们直接包装
                # 刚才在上面 messages.append 添加了最后一条提问，把它弹出来
                last_message = messages.pop()
                last_content = last_message.get("content", "")
                
                # 直接将图片和文字拼接成大模型需要的列表结构
                new_content = image_content + [{"type": "text", "text": last_content}]

                messages.append({
                    "role": "user",
                    "content": new_content
                })

        kwargs = {
            "model": model,
            "messages": messages,
            "max_tokens": maxtoken,
            "timeout": 1800
        }

        if 'claude' not in model.lower():
            kwargs["temperature"] = 0.1

        if 'kimi' in model.lower():
            kwargs["temperature"] = 1

        if is_claude and tishi:
            kwargs["system"] = tishi

        if is_claude:
            kwargs["stream"] = True
            completion = await client.messages.create(**kwargs)
            result = ""
            input_tokens = 0
            output_tokens = 0
            async for event in completion:
                if event.type == 'message_start' and hasattr(event, 'message') and hasattr(event.message, 'usage'):
                    input_tokens = getattr(event.message.usage, 'input_tokens', 0)
                elif event.type == 'content_block_delta' and hasattr(event, 'delta'):
                    result += getattr(event.delta, 'text', '')
                elif event.type == 'message_delta' and hasattr(event, 'usage'):
                    output_tokens = getattr(event.usage, 'output_tokens', 0)
        else:
            kwargs["stream"] = True
            kwargs["stream_options"] = {"include_usage": True}
            completion = await client.chat.completions.create(**kwargs)
            result = ""
            input_tokens = 0
            output_tokens = 0
            async for chunk in completion:
                if chunk.choices and len(chunk.choices) > 0:
                    delta_content = getattr(chunk.choices[0].delta, 'content', '')
                    if delta_content:
                        result += delta_content
                if getattr(chunk, 'usage', None):
                    input_tokens = getattr(chunk.usage, 'prompt_tokens', 0)
                    output_tokens = getattr(chunk.usage, 'completion_tokens', 0)

        result = re.sub(r'<think>.*?</think>', '', result, flags=re.DOTALL)
        result = re.sub(r'[\U00010000-\U0010FFFF]', '', result)
        result = result + "\n" + f"输入token: {input_tokens}, 输出token: {output_tokens}"

        # 数据库操作：请求成功后执行
        await run_in_threadpool(execute_db_update, result, kami, TD, ziduan, model, ku)
        return {
            "content": result,
            "usage": {
                "input_tokens": input_tokens,
                "output_tokens": output_tokens
            }
        }
    except Exception as e:
        print(f"后台任务执行出错: {e}")
        # 这里可以添加更多的错误处理逻辑，比如记录日志
        raise e

@app.post('/get-ai-response')
async def get_ai_response(request: Request):
    try:
        request_data = await request.json()  # 异步获取请求体
        tishi = request_data.get('tishi', '')
        user_content = request_data.get('user_content', '')
        image_urls = request_data.get('image_urls', [])
        ziduan = request_data.get('ziduan')
        ku = request_data.get('ku')
        kami = request_data.get('kami')
        TD = request_data.get('TD')
        address = request_data.get('address')
        sk = request_data.get('sk')
        model = request_data.get('model')
        maxtoken = request_data.get('maxtoken')
        
        # 创建任务
        task = asyncio.create_task(process_ai_task(address, sk, model, maxtoken, tishi, user_content, image_urls, kami, TD, ziduan, ku))
        # 等待任务完成，超时设置为 170 秒
        done, pending = await asyncio.wait([task], timeout=170.0)

        if task in done:
            # 任务在 170s 内完成
            try:
                task_result = task.result()
                return JSONResponse({
                    "code": 200,
                    "message": "请求成功",
                    "data": task_result["content"],
                    "usage": task_result["usage"]
                })
            except TimeoutError:
                return JSONResponse(
                    status_code=400,
                    content={"code": 400, "message": "超时", "data": None}
                )
            except Exception as e:
                return JSONResponse(
                    status_code=500,
                    content={"code": 500, "message": f"错误：{e}", "data": None}
                )
        else:
            # 任务超时（超过 170s），仍在后台运行
            # 前端断开连接，后端不再返回 AI 结果，但任务继续
            return JSONResponse(
                status_code=200,
                content={"code": 200, "message": "请求处理时间较长，已转入后台执行", "data": ""}
            )

    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"code": 500, "message": f"错误：{e}", "data": None}
        )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=2011)
