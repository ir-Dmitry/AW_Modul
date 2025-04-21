import logging
import traceback
import requests
import json
import os
import time
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Настройка логирования
LOG_FILE = "log/model_loader.log"
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s]: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    encoding="utf-8",
)

# Загрузка переменных окружения
load_dotenv()

# Константы
LOGIN = os.getenv("login")
PASSWORD = os.getenv("password")
URL_LOGIN = os.getenv("URL") + "api/user/login"
URL_DATA = os.getenv("URL") + "api/model/data"


def get_auth_token():
    """Получает новый токен авторизации."""
    payload = {"username": LOGIN, "password": PASSWORD}
    headers = {"Content-Type": "application/json", "Accept": "application/json"}

    try:
        response = requests.post(URL_LOGIN, json=payload, verify=False, headers=headers)
        response.raise_for_status()
        data = response.json()

        if not data.get("success"):
            raise ValueError("Авторизация неуспешна")

        token = data.get("data", {}).get("token")
        if not token:
            raise ValueError("Токен отсутствует в ответе")

        logging.info("Успешно получен токен.")
        return token

    except requests.exceptions.RequestException as e:
        logging.error("Ошибка при запросе токена: %s", e)
        logging.debug("Детали ошибки:\n%s", traceback.format_exc())
    except ValueError as e:
        logging.error("Ошибка обработки ответа: %s", e)
        logging.debug("Детали ошибки:\n%s", traceback.format_exc())
    except Exception as e:
        logging.error("Неизвестная ошибка при получении токена: %s", e)
        logging.debug("Детали ошибки:\n%s", traceback.format_exc())

    return None


def download_model(model_id):
    """Загружает данные модели по ID."""
    if not os.path.exists("data"):
        os.makedirs("data")

    token = get_auth_token()
    logging.info("Получен токен для загрузки модели ID %s: %s", model_id, token)

    params = {"id": model_id}
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    payload = {"limit": 3, "full_preview": True}

    while True:
        try:
            response = requests.post(
                URL_DATA,
                params=params,
                json=payload,
                headers=headers,
                verify=False,
                stream=True,
            )
            response.raise_for_status()
            data = response.json()
            filename = f"data/model_{model_id}.json"

            if not os.path.exists(filename):
                logging.info("Создание файла %s.", filename)
            else:
                logging.info("Перезаписывание файла %s.", filename)

            with open(filename, "w", encoding="utf-8") as file:
                json.dump(data, file, ensure_ascii=False, indent=4)
            logging.info("Файл модели ID %s успешно сохранен.", model_id)
            break

        except requests.exceptions.RequestException as e:
            logging.error("Ошибка при загрузке модели ID %s: %s", model_id, e)
            logging.debug("Детали ошибки:\n%s", traceback.format_exc())
            time.sleep(5)

        except json.JSONDecodeError as e:
            logging.error("Ошибка парсинга JSON для модели ID %s: %s", model_id, e)
            logging.debug("Детали ошибки:\n%s", traceback.format_exc())
            time.sleep(5)

        except Exception as e:
            logging.error(
                "Неизвестная ошибка при загрузке модели ID %s: %s", model_id, e
            )
            logging.debug("Детали ошибки:\n%s", traceback.format_exc())
            time.sleep(5)


def merge_model_files(model_ids, output_file="merged_output"):
    """Компилирует данные моделей в единый файл."""
    output_file = "data/" + output_file + ".json"
    combined_data = []

    if not os.path.exists("data"):
        os.makedirs("data")

    for model_id in model_ids:
        download_model(model_id)
        filename = f"data/model_{model_id}.json"
        try:
            with open(filename, "r", encoding="utf-8") as file:
                model_data = json.load(file)
                rows_data = model_data.get("data", {}).get("rows", [])
                combined_data.extend(rows_data)
            logging.info(
                "Данные из модели ID %s успешно добавлены в список на объединение.",
                model_id,
            )
        except Exception as e:
            logging.error("Ошибка при чтении файла %s: %s", filename, e)
            logging.debug("Детали ошибки:\n%s", traceback.format_exc())

    try:
        with open(output_file, "w", encoding="utf-8") as file:
            json.dump(combined_data, file, ensure_ascii=False, indent=4)
        logging.info("Данные успешно сохранены в файл %s.", output_file)
    except Exception as e:
        logging.error("Ошибка при сохранении скомпилированных данных: %s", e)
        logging.debug("Детали ошибки:\n%s", traceback.format_exc())

    delete_model_files(model_ids)


def delete_model_files(model_ids):
    """Удаляет файлы моделей по их идентификаторам."""
    for model_id in model_ids:
        file_path = f"data/model_{model_id}.json"
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
                logging.info("Файл %s успешно удален.", file_path)
            else:
                logging.warning("Файл %s не найден для удаления.", file_path)
        except Exception as e:
            logging.error("Ошибка при удалении файла %s: %s", file_path, e)
            logging.debug("Детали ошибки:\n%s", traceback.format_exc())


if __name__ == "__main__":
    logging.info(
        "//////////////////////////////////////////Начало выполнения скрипта.//////////////////////////////////////////"
    )
    models_list = [1001]

    try:
        merge_model_files(models_list)
    except Exception as e:
        logging.critical("Критическая ошибка при выполнении скрипта: %s", e)
        logging.debug("Детали ошибки:\n%s", traceback.format_exc())
    logging.info("Завершение выполнения скрипта.")
