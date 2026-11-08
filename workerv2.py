import base64      # Для кодирования сгенерированных картинок (байтов) в текстовую строку base64
import json        # Для упаковки/распаковки задач и ответов в формат JSON
import os          # Для проверки существования файла шаблона workflow_api.json на диске
import time        # Для пауз (sleep) и замера таймаутов при ожидании загрузки моделей
import uuid        # Для генерации уникальных ID клиента WebSocket и случайных сидов (seed)
import docker      # Официальный SDK для управления контейнерами Docker из Python (docker-py)
import redis       # Клиент для работы с очередями задач и результатов на Master-сервере Redis
import requests    # Для отправки HTTP-запросов к API llama-server (порт 8080) и ComfyUI (порт 8188)
import websocket   # Из пакета websocket-client: для прослушивания прогресса генерации в ComfyUI

# =====================================================================
# БЛОК КОНФИГУРАЦИИ
# =====================================================================

# ВАЖНО: Укажи здесь реальный IP-адрес твоей Master VM, где запущен Redis!
# Узнать его на мастере можно командой: hostname -I или ip -br a
REDIS_HOST = "192.168.0.20"  # <-- ПРОВЕРЬ И ЗАМЕНИ НА ТОЧНЫЙ IP МАСТЕРА!
REDIS_PORT = 6379            # Стандартный порт Redis

# Имена списков (очередей) в Redis. Должны совпадать с тем, куда пишет Мастер!
TASK_QUEUE = "tasks_queue"       # Очередь входящих задач (Мастер пишет -> Воркер читает)
RESULT_QUEUE = "results_queue"   # Очередь готовых ответов (Воркер пишет -> Мастер читает)

# Названия Docker-контейнеров (как они указаны в container_name в docker-compose.yml)
LLAMA_CONTAINER_NAME = "llama_qwen"
COMFY_CONTAINER_NAME = "comfy_nova"

# Локальные адреса сервисов внутри LXC-контейнера воркера
LLAMA_URL = "http://127.0.0.1:8080"
COMFY_URL = "http://127.0.0.1:8188"
COMFY_WS = "ws://127.0.0.1:8188/ws"

# Настройки графа ComfyUI (файл должен лежать рядом с worker.py, экспортированный через "Save (API Format)")
WORKFLOW_FILE = "workflow_api.json"
PROMPT_NODE_ID = "6"     # ID ноды CLIPTextEncode (куда подставляется положительный промпт)
KSAMPLER_NODE_ID = "3"   # ID ноды KSampler (куда подставляется случайный seed)


class GPUWorker:
    def __init__(self):
        print(f"[INIT] Инициализация воркера. Подключение к Redis ({REDIS_HOST}:{REDIS_PORT})...")

        # Создаем клиент Redis.
        # decode_responses=True автоматически переводит байты из Redis в обычные строки str.
        # socket_connect_timeout=5 не даст скрипту зависнуть надолго, если IP указан неверно.
        self.rds = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            decode_responses=True,
            socket_connect_timeout=5
        )

        # Сразу проверяем связь с Redis при запуске скрипта
        try:
            self.rds.ping()
            print("[INIT] Успешное подключение к Redis!")
        except redis.exceptions.RedisError as e:
            raise RuntimeError(
                f"[FATAL] Не удалось подключиться к Redis по адресу {REDIS_HOST}:{REDIS_PORT}. "
                f"Проверь IP-адрес, статус redis-server и файрвол! Детали: {e}"
            )

        # Подключаемся к локальному демону Docker через сокет /var/run/docker.sock
        self.docker_client = docker.from_env()

        # Получаем объект контейнера llama_qwen
        try:
            self.llama_container = self.docker_client.containers.get(LLAMA_CONTAINER_NAME)
        except docker.errors.NotFound:
            raise RuntimeError(f"[FATAL] Контейнер '{LLAMA_CONTAINER_NAME}' не найден! Создай его через docker compose.")

        # Получаем объект контейнера comfy_nova (если он создан)
        try:
            self.comfy_container = self.docker_client.containers.get(COMFY_CONTAINER_NAME)
        except docker.errors.NotFound:
            print(f"[WARN] Контейнер '{COMFY_CONTAINER_NAME}' не найден. Генерация картинок будет недоступна.")
            self.comfy_container = None

        # Изначально ставим None (мы еще не знаем, какая модель реально в памяти),
        # чтобы метод ensure_qwen_loaded() честно проверил статус контейнера и /health!
        self.active_model = None

        # Загружаем дефолтную текстовую модель Qwen при старте воркера
        self.ensure_qwen_loaded()

    # =====================================================================
    # БЛОК УПРАВЛЕНИЯ ВИДЕОПАМЯТЬЮ (VRAM) И КОНТЕЙНЕРАМИ
    # =====================================================================

    def wait_for_llama_ready(self, timeout=180):
        """
        Опрашивает эндпоинт /health у llama-server каждые 2 секунды,
        пока веса модели полностью не загрузятся в видеопамять MI50.
        """
        print(f"[VRAM] Ожидание готовности llama-server (таймаут {timeout} сек)...")
        start = time.time()

        while time.time() - start < timeout:
            try:
                # Запрашиваем статус здоровья сервера
                r = requests.get(f"{LLAMA_URL}/health", timeout=2)
                # Статус 200 и {"status": "ok"} означают, что модель полностью в VRAM и готова к генерации.
                # Пока модель грузится, сервер возвращает код 503 (Loading model).
                if r.status_code == 200 and r.json().get("status") == "ok":
                    print("[VRAM] Qwen 27B полностью загружен и готов к работе!")
                    return True
            except requests.RequestException:
                # Если контейнер только что запустился, веб-сервер может еще не принимать соединения
                pass
            time.sleep(2)

        raise RuntimeError(f"Таймаут ({timeout} сек): llama-server не вышел в статус готовности!")

    def wait_for_comfy_ready(self, timeout=60):
        """
        Ждет, пока веб-сервер ComfyUI на порту 8188 начнет отвечать на HTTP-запросы.
        """
        print(f"[VRAM] Ожидание готовности интерфейса ComfyUI (таймаут {timeout} сек)...")
        start = time.time()
        while time.time() - start < timeout:
            try:
                r = requests.get(f"{COMFY_URL}/system_stats", timeout=2)
                if r.status_code == 200:
                    print("[VRAM] Сервер ComfyUI готов принимать задачи!")
                    return True
            except requests.RequestException:
                pass
            time.sleep(1)

        raise RuntimeError(f"Таймаут ({timeout} сек): ComfyUI не отвечает на порту 8188!")

    def free_comfy_vram(self):
        """
        Отправляет POST-запрос в ComfyUI с приказом выгрузить чекпоинты из видеопамяти MI50,
        не убивая сам процесс ComfyUI (если он запущен).
        """
        try:
            requests.post(
                f"{COMFY_URL}/free",
                json={"unload_models": True, "free_memory": True},
                timeout=5
            )
            print("[VRAM] ComfyUI успешно выгрузил модели из VRAM.")
        except requests.RequestException as e:
            # Если контейнер ComfyUI остановлен или еще не поднимался — это нормально, просто пишем лог
            print(f"[INFO] ComfyUI не ответил на /free (вероятно, уже остановлен): {e}")

    def ensure_qwen_loaded(self):
        """
        Гарантирует, что видеопамять свободна от ComfyUI, контейнер llama_qwen запущен,
        а модель Qwen находится в VRAM и отвечает 200 OK.
        """
        # Обновляем состояние объекта контейнера из Docker демона
        self.llama_container.reload()

        # Если мы уже в режиме qwen и контейнер реально работает — ничего не делаем
        if self.active_model == "qwen" and self.llama_container.status == "running":
            return

        print("[SWITCH] Переключение VRAM: Освобождаем ComfyUI -> Загружаем Qwen 27B...")

        # 1. Просим ComfyUI выгрузить модели из памяти
        self.free_comfy_vram()

        # 2. Если контейнер llama_qwen остановлен — запускаем его
        if self.llama_container.status != "running":
            print(f"[DOCKER] Запуск контейнера {LLAMA_CONTAINER_NAME}...")
            self.llama_container.start()

        # 3. Ждем, пока веса загрузятся в MI50
        self.wait_for_llama_ready()

        # 4. Фиксируем текущую активную модель
        self.active_model = "qwen"

    def ensure_comfy_loaded(self):
        """
        Останавливает контейнер Qwen (чтобы гарантированно освободить все 16 ГБ VRAM MI50)
        и проверяет, что контейнер ComfyUI запущен и готов к рендеру.
        """
        if self.active_model == "comfy":
            return

        print("[SWITCH] Переключение VRAM: Останавливаем Qwen 27B -> Готовим ComfyUI...")

        # 1. Останавливаем контейнер llama_qwen
        self.llama_container.reload()
        if self.llama_container.status == "running":
            print(f"[DOCKER] Остановка контейнера {LLAMA_CONTAINER_NAME}...")
            self.llama_container.stop(timeout=5)

        # 2. Проверяем, запущен ли контейнер ComfyUI, и запускаем при необходимости
        if self.comfy_container is not None:
            self.comfy_container.reload()
            if self.comfy_container.status != "running":
                print(f"[DOCKER] Запуск контейнера {COMFY_CONTAINER_NAME}...")
                self.comfy_container.start()
            self.wait_for_comfy_ready()

        self.active_model = "comfy"
        print("[VRAM] VRAM освобождена и передана под ComfyUI.")

    # =====================================================================
    # БЛОК ГЕНЕРАЦИИ ТЕКСТА (LLAMA.CPP)
    # =====================================================================

    def generate_text(self, prompt: str) -> str:
        # Убеждаемся, что Qwen загружен в видеопамять
        self.ensure_qwen_loaded()

        # Формируем запрос в формате OpenAI Chat Completions
        payload = {
            "messages": [
                {"role": "system", "content": "Ты полезный ИИ-ассистент. Отвечай прямо, информативно и по делу на русском языке. Избегай избыточных зацикленных рассуждений."},
                {"role": "user", "content": prompt}
            ],
            "temperature": 0.7,
            "max_tokens": 2048
        }

        print("[LLM] Отправка запроса в llama-server...")
        resp = requests.post(f"{LLAMA_URL}/v1/chat/completions", json=payload, timeout=180)
        resp.raise_for_status()  # Выбросит исключение, если код ответа 4xx или 5xx

        # Извлекаем сгенерированный текст из ответа
        msg = resp.json()["choices"][0]["message"]
        # Берем content, а если он пустой — берем reasoning_content
        answer = msg.get("content") or msg.get("reasoning_content") or ""
        print(f"[LLM] Ответ успешно получен ({len(answer)} символов).")
        return answer

    # =====================================================================
    # БЛОК ГЕНЕРАЦИИ ИЗОБРАЖЕНИЙ (COMFYUI)
    # =====================================================================

    def generate_image(self, prompt: str) -> str:
        # Выгружаем Qwen и поднимаем ComfyUI
        self.ensure_comfy_loaded()

        # Проверяем наличие файла графа перед чтением
        if not os.path.exists(WORKFLOW_FILE):
            raise FileNotFoundError(f"Файл воркфлоу '{WORKFLOW_FILE}' не найден в рабочей папке!")

        with open(WORKFLOW_FILE, "r", encoding="utf-8") as f:
            workflow = json.load(f)

        # Проверяем, что нужные ID нод реально существуют в JSON-файле
        if PROMPT_NODE_ID not in workflow:
            raise KeyError(f"Нода промпта с ID '{PROMPT_NODE_ID}' отсутствует в {WORKFLOW_FILE}!")
        if KSAMPLER_NODE_ID not in workflow:
            raise KeyError(f"Нода KSampler с ID '{KSAMPLER_NODE_ID}' отсутствует в {WORKFLOW_FILE}!")

        # Подставляем текст промпта и генерируем случайный 48-битный seed
        workflow[PROMPT_NODE_ID]["inputs"]["text"] = prompt
        workflow[KSAMPLER_NODE_ID]["inputs"]["seed"] = int.from_bytes(uuid.uuid4().bytes[:6], "big")

        # Уникальный идентификатор сессии для отслеживания статуса по WebSocket
        client_id = str(uuid.uuid4())
        ws = websocket.WebSocket()
        ws.settimeout(300)  # Таймаут 5 минут на случай зависания генерации
        ws.connect(f"{COMFY_WS}?clientId={client_id}")

        try:
            # Отправляем граф в очередь исполнения ComfyUI
            print("[COMFY] Отправка графа на рендер...")
            resp = requests.post(
                f"{COMFY_URL}/prompt",
                json={"prompt": workflow, "client_id": client_id},
                timeout=10
            )
            resp.raise_for_status()
            prompt_id = resp.json()["prompt_id"]
            print(f"[COMFY] Задача принята в ComfyUI (prompt_id: {prompt_id}). Ожидание рендера...")

            # Слушаем события от ComfyUI по WebSocket до завершения нашего prompt_id
            while True:
                out = ws.recv()
                if isinstance(out, str):
                    message = json.loads(out)
                    if message.get("type") == "executing":
                        data = message.get("data", {})
                        # Когда node == None для нашего prompt_id, генерация всего графа завершена
                        if data.get("node") is None and data.get("prompt_id") == prompt_id:
                            print("[COMFY] Генерация изображения завершена!")
                            break
        finally:
            # Всегда закрываем сокет, даже если внутри блока произошла ошибка
            ws.close()

        # Запрашиваем историю выполнения по нашему prompt_id, чтобы достать имя файла картинки
        history_resp = requests.get(f"{COMFY_URL}/history/{prompt_id}", timeout=10)
        history_resp.raise_for_status()
        history = history_resp.json().get(prompt_id, {})

        # Ищем выходную ноду SaveImage / PreviewImage, содержащую массив "images"
        for node_id, node_output in history.get("outputs", {}).items():
            if "images" in node_output and len(node_output["images"]) > 0:
                img_info = node_output["images"][0]
                # Скачиваем саму картинку в виде байтов через эндпоинт /view
                img_resp = requests.get(
                    f"{COMFY_URL}/view",
                    params={
                        "filename": img_info["filename"],
                        "subfolder": img_info["subfolder"],
                        "type": img_info["type"]
                    },
                    timeout=15
                )
                img_resp.raise_for_status()
                # Кодируем байты картинки в base64-строку для удобной передачи через JSON в Redis
                return base64.b64encode(img_resp.content).decode("utf-8")

        raise RuntimeError("ComfyUI завершил работу, но не вернул ни одного изображения в выходных нодах!")

    # =====================================================================
    # ГЛАВНЫЙ ЦИКЛ ОЧЕРЕДИ (REDIS LOOP)
    # =====================================================================

    def next_task_is_image(self) -> bool:
        """
        Подглядывает в конец очереди (индекс -1 при использовании LPUSH + BLPOP),
        чтобы узнать, не является ли следующая задача тоже генерацией картинки.
        Это позволяет не перезагружать Qwen зря, если подряд идут две картинки.
        """
        # Примечание: если Мастер кладет задачи через LPUSH, а воркер забирает через BLPOP (слева),
        # то следующая задача лежит под индексом 0.
        next_raw = self.rds.lindex(TASK_QUEUE, 0)
        if not next_raw:
            return False
        try:
            return json.loads(next_raw).get("type") == "image"
        except Exception:
            return False

    def run(self):
        print(f"[WORKER] Воркер активен! Слушаем очередь '{TASK_QUEUE}'...")
        while True:
            try:
                # Блокирующее чтение из очереди с таймаутом 5 секунд
                item = self.rds.blpop(TASK_QUEUE, timeout=5)
            except redis.exceptions.RedisError as e:
                print(f"[REDIS ERROR] Потеряна связь с Redis: {e}. Повтор через 5 сек...")
                time.sleep(5)
                continue

            # Если за 5 секунд задач не поступило:
            if not item:
                # Если мы оставались в режиме ComfyUI, а задач больше нет — заранее возвращаем Qwen в VRAM
                if self.active_model != "qwen":
                    print("[IDLE] Очередь пуста. Возвращаем дефолтную модель Qwen 27B в VRAM...")
                    try:
                        self.ensure_qwen_loaded()
                    except Exception as e:
                        print(f"[ERROR] Не удалось вернуть Qwen в режиме простоя: {e}")
                continue

            # Распаковываем полученную задачу (item — это кортеж: (имя_очереди, строка_json))
            _, raw_task = item
            try:
                task = json.loads(raw_task)
            except json.JSONDecodeError:
                print(f"[ERROR] Получен битый JSON в очереди: {raw_task}")
                continue

            task_id = task.get("task_id", "unknown")
            task_type = task.get("type")
            prompt = task.get("prompt", "")

            print(f"\n[TASK {task_id}] Получена задача | Тип: {task_type} | Промпт: {prompt[:60]}...")

            try:
                if task_type == "text":
                    result_data = self.generate_text(prompt)
                elif task_type == "image":
                    result_data = self.generate_image(prompt)
                    # Если следующая задача в очереди НЕ картинка — сразу возвращаем Qwen в память
                    if not self.next_task_is_image():
                        self.ensure_qwen_loaded()
                else:
                    raise ValueError(f"Неизвестный тип задачи: '{task_type}' (ожидается 'text' или 'image')")

                response_payload = {
                    "task_id": task_id,
                    "channel_id": task.get("channel_id"),
                    "type": task_type,
                    "status": "ok",
                    "data": result_data
                }
                print(f"[TASK {task_id}] Успешно выполнено! Отправка ответа в '{RESULT_QUEUE}'.")

            except Exception as e:
                print(f"[ERROR] Ошибка выполнения задачи {task_id}: {e}")
                response_payload = {
                    "task_id": task_id,
                    "channel_id": task.get("channel_id"),
                    "type": task_type,
                    "status": "error",
                    "data": str(e)
                }

            # Отправляем результат в очередь ответов (ensure_ascii=False сохраняет нормальную кириллицу)
            self.rds.rpush(RESULT_QUEUE, json.dumps(response_payload, ensure_ascii=False))


if __name__ == "__main__":
    worker = GPUWorker()
    worker.run()