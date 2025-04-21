from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
import uvicorn
import io
import gzip
import json
from fastapi import HTTPException
from model_loader import download_model

app = FastAPI()

# Настройка CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Разрешить запросы с любого источника
    allow_credentials=True,
    allow_methods=["*"],  # Разрешить все HTTP-методы
    allow_headers=["*"],  # Разрешить все заголовки
)


@app.get("/model_{model_id}/")
async def get_model_data(model_id: int):
    try:
        # Формирование имени файла на основе переданного номера модели
        download_model(model_id)

        file_path = f"data/model_{model_id}.json"

        # Чтение данных из файла
        with open(file_path, "r", encoding="utf-8") as file:
            data = json.load(file)

        # Проверка размера данных перед сжатием
        if len(json.dumps(data).encode("utf-8")) > 500:  # Пример порога
            print(f"Сжатие для model_{model_id}")
            return create_gzipped_response(data)

        print(f"БЕЗ сжатия для model_{model_id}")
        return data

    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"Файл {file_path} не найден.")
    except json.JSONDecodeError:
        raise HTTPException(
            status_code=400, detail=f"Ошибка декодирования JSON в файле {file_path}."
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Неизвестная ошибка: {str(e)}")


def create_gzipped_response(data):
    gzip_buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=gzip_buffer, mode="wb") as f:
        f.write(json.dumps(data, ensure_ascii=False).encode("utf-8"))
    gzip_buffer.seek(0)

    return StreamingResponse(
        iter([gzip_buffer.getvalue()]),
        media_type="application/json",
        headers={
            "Content-Encoding": "gzip",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type",
        },
    )


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)


# import sys
# import os
# import io
# import logging
# import traceback
# import json
# import pandas as pd
# import httpx
# import time
# from aw_client import Session
# from dotenv import load_dotenv
# from fastapi import FastAPI
# from fastapi.responses import StreamingResponse

# logger = logging.getLogger(__name__)
# app = FastAPI()


# def load_model_with_retries(session, model_id, max_retries=3, retry_delay=5):
#     for attempt in range(max_retries + 1):
#         try:
#             logger.info(f"Attempt {attempt + 1} to load model")
#             df = session.load_model(model_id=model_id)
#             return df
#         except Exception as e:
#             logger.error(f"Error loading model (attempt {attempt + 1}): {e}")
#             logger.error(traceback.format_exc())

#             if attempt < max_retries:
#                 logger.info(f"Retrying in {retry_delay} seconds...")
#                 time.sleep(retry_delay)
#             else:
#                 logger.error("Max retries reached.  Failed to load model.")
#                 raise


# @app.get("/model")
# def get_model():
#     try:
#         logger.info("START READ TOKEN")
#         load_dotenv()

#         AW_DATA_TOKEN = os.getenv("AW_DATA_TOKEN")
#         URL = os.getenv("URL")
#         MODEL_ID = os.getenv("MODEL_ID")

#         if not AW_DATA_TOKEN:
#             raise ValueError("AW_DATA_TOKEN environment variable not set.")
#         if not URL:
#             raise ValueError("URL environment variable not set.")
#         if not MODEL_ID:
#             raise ValueError("MODEL_ID environment variable not set.")

#         # Create an httpx client with verify=False
#         client = httpx.Client(verify=False)

#         logger.info("START OPEN SESSION")
#         # Pass the client to the Session
#         session = Session(
#             token=AW_DATA_TOKEN, aw_url=URL, client=client
#         )  # Pass the httpx client

#         logger.info("START LOAD MODEL")
#         df = load_model_with_retries(session, MODEL_ID)
#         logger.info(df)
#         return df
#     except ValueError as ve:
#         logger.error(f"Environment variable error: {ve}")
#         return {"error": str(ve)}
#     except Exception as e:
#         logger.error(f"An unexpected error occurred: {e}")
#         logger.error(traceback.format_exc())
#         return {"error": "An unexpected error occurred."}
