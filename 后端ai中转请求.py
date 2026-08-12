
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

# 数据库配置（需在实际部署时填入真实凭证）
db_config = {
    'host': '',
    'database': '',
    'user': '',
    'password': '',
    'charset': 'utf8mb4'
}

def execute_db_update(result, kami, TD, ziduan, model_name, ku):
    """
    同步数据库更新函数：在 AI 请求完成后，将生成的内容及相关元数据写回 MySQL。
    使用 %s 参数化查询防止 SQL 注入。
    """
    connection = None
    try:
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        # 动态表名和字段名使用反引号包裹，避免与 MySQL 内部保留关键字发生冲突
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
        # 确保数据库连接资源被正确释放
        if connection and connection.is_connected():
            connection.close()

app = FastAPI()

# HTTP 客户端请求图片的全局超时限制
timeout = 600

async def process_ai_task(address, sk, model, maxtoken, tishi, user_content, image_urls, kami, TD, ziduan, ku):
    """
    核心 AI 处理协程：负责处理大模型 API 请求、图片下载及格式转换、流式结果接收以及后续落库。
    """
    try:
        # 根据项目记忆约束，判断是否为 Claude 模型需同时检查名称及 address 中不包含 'keylinkclub'
        is_claude = ('claude' in model.lower()) and ('keylinkclub' not in (address or ''))

        # 构造对话历史（兼容 OpenAI 和 Anthropic 的规范）
        messages = []
        if tishi and not is_claude:
            # 非 Claude 模型的 System Prompt 通过 role: system 传入
            messages.append({"role": "system", "content": tishi})
        if user_content or image_urls:
            messages.append({"role": "user", "content": user_content or ""})

        # 初始化对应的异步 SDK 客户端
        if is_claude:
            base_url = address
            # Anthropic SDK 不接受 /v1 结尾的 base_url，需在此进行兼容修剪
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

        # 处理多模态图片输入
        if image_urls:
            image_content = []
            if is_claude:
                # Claude 需要以 Base64 格式接收图片，因此需先从 URL 下载图片并编码
                async with httpx.AsyncClient() as http_client:
                    for url in image_urls:
                        try:
                            response = await http_client.get(url, timeout=timeout)
                            response.raise_for_status()
                            media_type = response.headers.get('content-type', 'image/jpeg')
                            if not media_type.startswith('image/'):
                                media_type = 'image/jpeg'
                            base64_data = base64.b64encode(response.content).decode('utf-8')
                            
                            # Claude 规范的多模态结构：包含文本描述和 base64 编码的图片源
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
                # OpenAI 兼容接口直接支持传入 image_url
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
                # 弹出刚才添加的最后一个文本提问，将文字和图片组合在同一个 message 结构中
                last_message = messages.pop()
                last_content = last_message.get("content", "")
                
                new_content = image_content + [{"type": "text", "text": last_content}]
                messages.append({
                    "role": "user",
                    "content": new_content
                })

        # 统一的生成参数字典
        kwargs = {
            "model": model,
            "messages": messages,
            "max_tokens": maxtoken,
            "timeout": 1800  # API 请求层面设置的长超时时间
        }

        # 针对不同模型提供特定的 Temperature 设置策略
        if 'claude' not in model.lower():
            kwargs["temperature"] = 0.1
        if 'kimi' in model.lower():
            kwargs["temperature"] = 1

        # Claude 的 System Prompt 需作为顶级参数传入，而非 messages 内
        if is_claude and tishi:
            kwargs["system"] = tishi

        # 发起流式请求并收集 Token 统计与生成文本
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
            kwargs["stream_options"] = {"include_usage": True} # 确保流式返回中包含 usage 统计
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

        # 针对包含推理过程的模型 (如 DeepSeek-R1)，剥离 <think> 标签及其内容
        result = re.sub(r'<think>.*?</think>', '', result, flags=re.DOTALL)
        # 清理超出 BMP 的特殊字符（如 Emoji 等），防止旧版或配置不当的数据库报错
        result = re.sub(r'[\U00010000-\U0010FFFF]', '', result)
        # 追加 Token 消耗统计
        result = result + "\n" + f"输入token: {input_tokens}, 输出token: {output_tokens}"

        # 异步转移阻塞型 IO 任务至线程池执行数据库更新，防止阻塞主事件循环
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
        raise e

@app.post('/get-ai-response')
async def get_ai_response(request: Request):
    """
    API 路由：接收前端 AI 生成请求。采用 170 秒等待策略处理长耗时任务。
    如果 170 秒内生成完毕则正常返回结果；如果超时，则任务转后台继续执行，并提前告知前端。
    """
    try:
        request_data = await request.json()
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
        
        # 将实际处理逻辑包装为 asyncio Task，方便做超时管控
        task = asyncio.create_task(process_ai_task(address, sk, model, maxtoken, tishi, user_content, image_urls, kami, TD, ziduan, ku))
        
        # 等待任务完成，若耗时超过 170 秒则触发 timeout，但不取消任务
        done, pending = await asyncio.wait([task], timeout=170.0)

        if task in done:
            # 任务在 170s 内正常完成
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
            # 任务超时（超过 170s），前端避免长连接断开，先返回确认响应，任务在后台不受影响继续进行和落库
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
    # 启动 FastAPI 服务，监听 2011 端口
    uvicorn.run(app, host="0.0.0.0", port=2011)
