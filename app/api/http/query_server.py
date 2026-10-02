from fastapi import FastAPI,BackgroundTasks
from fastapi.requests import Request
from fastapi.responses import FileResponse,StreamingResponse
from pathlib import Path

from app.api.schemas.query_shema import AsyncQueryResponseSchema, SyncQueryResponseSchema
from app.api.schemas.query_shema import QueryRequestSchema

from app.shared.runtime.logger import logger,PROJECT_ROOT
from app.shared.utils.sse_utils import sse_generator, push_to_session, SSEEvent
from datetime import datetime
from mimetypes import guess_type
from app.shared.utils.task_utils import get_done_task_list, update_task_status, TASK_STATUS_PROCESSING, \
    TASK_STATUS_COMPLETED, TASK_STATUS_FAILED, clear_task

from app.process.query.agent.state import QueryGraphState,create_query_default_state
from app.process.query.agent.main_graph import query_graph_app
import json
from app.shared.utils.sse_utils import create_sse_queue

app = FastAPI()

# 接口1: 健康状态检查
@app.get("/health")
def health():
    logger.info(f"{datetime.now()}进行服务状态健康检查!!")
    return {
        "code" : 0
    }

# 接口2: 返回chat.html页面
@app.get("/html")
def return_html():
    html_path_obj:Path = PROJECT_ROOT / "app" / "resources" / "htmls" / "chat.html"
    return FileResponse(
        path=str(html_path_obj),
        media_type=guess_type(html_path_obj.name)[0]
    )

# 接口3: 流式响应接口
@app.get("/stream/{session_id}")
def stream(session_id:str,request:Request):
    logger.info(f"{session_id}建立了流式响应通道!!")
    return StreamingResponse(
        # session_id -> {session_id : [ ]} -> while True
        # request -> fastapi -> request  -> 请求包 -> 包装成的对象 -> 1.request手动获取各种参数[请求报文] 2.监控链接是否断开  is_disconnected  ==  event_source . close()
        sse_generator(session_id,request),
        media_type="text/event-stream"
    )

# 同步函数 执行查询图的过程
def invoke_query_graph(original_query:str,session_id:str,is_stream:bool):
    try:
        # 为什么要添加is_stream?
        # 流式 :  invoke_query_graph is_stream = True ->  task_utils ->  -> sse队列
        # 非流式: invoke_query_graph is_stream = False -> done_list -> task_utils
        update_task_status(session_id,TASK_STATUS_PROCESSING,is_stream)
        # 1.创建图需要的state参数
        state: QueryGraphState = create_query_default_state(
            session_id=session_id,
            original_query=original_query,
            is_stream=is_stream
        )
        logger.info(f"开始测试查询图流程,传入参数为:\n {json.dumps(state, indent=4, ensure_ascii=False)}")
        # 2. 调用图对象
        result = query_graph_app.invoke(state)
        update_task_status(session_id, TASK_STATUS_COMPLETED, is_stream)

        # 流式 + 正常结束
        if is_stream:
            push_to_session(
                session_id,
                SSEEvent.FINAL,
                {
                    "answer": result.get("answer"),
                    "status": "completed",
                    "image_urls": result.get("image_urls", [])
                }
            )

        logger.info(f"测试结束查询图流程,查询结果为:\n {json.dumps(result, indent=4, ensure_ascii=False)}")
        return result
    except Exception as e:
        logger.exception(f"查询图执行出现错误!{e}")
        update_task_status(session_id, TASK_STATUS_FAILED, is_stream)
        push_to_session(session_id, SSEEvent.ERROR, data={"error": f"查询:{original_query}流程报错!错误信息:{str(e)}"})


# 接口4: 查询问题接口
@app.post("/query")
def query_question(task:BackgroundTasks,param:QueryRequestSchema):
    # 1.获取参数
    session_id:str = param.session_id
    query:str      = param.query
    is_stream:bool = param.is_stream
    # 2.判断是不是流式
    if is_stream:
        # 创建一个队列
        # 清空之前的session_id对应的列表 避免重复添加队列之前的列表信息
        clear_task(session_id)
        create_sse_queue(session_id)
        task.add_task(invoke_query_graph,is_stream=is_stream,original_query=query,session_id=session_id)
        # 3.流式的异步执行
        return AsyncQueryResponseSchema(
            message=f"{query}问题正在查询和处理中...",
            session_id=session_id
        )
    else:
        # 4.非流式的同步执行
        result:QueryGraphState = invoke_query_graph(original_query=query,session_id=session_id,is_stream=is_stream)
        done_list =  get_done_task_list(session_id)
        return SyncQueryResponseSchema(
            message=f"{query}问题,查询成功!",
            session_id=session_id,
            answer=result.get("answer"),
            done_list=done_list,
            image_urls=result.get("image_urls",[])
        )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app,host="0.0.0.0",port=8001)



