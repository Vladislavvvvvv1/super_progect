import base64      # Для кодирования сгенерированных картинок (байтов) в текстовую строку base64
import json        # Для упаковки/распаковки задач и ответов в формат JSON
import os          # Для проверки существования файла шаблона workflow_api.json и чтения env
import re          # Для очистки <think>...</think> блоков при необходимости
import time        # Для пауз (sleep) и замера таймаутов при ожидании загрузки моделей
import uuid        # Для генерации уникальных ID клиента WebSocket и случайных сидов (seed)
import docker      # Официальный SDK для управления контейнерами Docker из Python (docker-py)
import redis       # Клиент для работы с очередями задач и результатов на Master-сервере Redis
import requests    # Для отправки HTTP-запросов к API llama-server (порт 8080) и ComfyUI (порт 8188)
import websocket   # Из пакета websocket-client: для прослушивания прогресса генерации в ComfyUI

# =====================================================================
# БЛОК КОНФИГУРАЦИИ
# =====================================================================

REDIS_HOST = os.getenv("REDIS_HOST", "192.168.0.20")  # Проверь IP-адрес Master VM
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", None)    # Если на мастере включен requirepass

# Имена списков (очередей) в Redis
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

# Опция: убирать ли блок рассуждений <think>...</think> из финального текста для Discord
STRIP_THINK_TAGS = True


class GPUWorker:
    def __init__(self):
        print(f"[INIT] Инициализация воркера. Подключение к Redis ({REDIS_HOST}:{REDIS_PORT})...")

        self.rds = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            password=REDIS_PASSWORD,
            decode_responses=True,
            socket_connect_timeout=5,
            socket_timeout=15,
            health_check_interval=30
        )

        # Проверяем связь с Redis при запуске скрипта
        try:
            self.rds.ping()
            print("[INIT] Успешное подключение к Redis!")
        except redis.exceptions.RedisError as e:
            raise RuntimeError(
                f"[FATAL] Не удалось подключиться к Redis по адресу {REDIS_HOST}:{REDIS_PORT}. "
                f"Проверь IP-адрес, пароль, статус redis-server и файрвол! Детали: {e}"
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

        self.active_model = None

        # Загружаем дефолтную текстовую модель Qwen при старте воркера
        self.ensure_qwen_loaded()

    # =====================================================================
    # БЛОК УПРАВЛЕНИЯ ВИДЕОПАМЯТЬЮ (VRAM) И КОНТЕЙНЕРАМИ
    # =====================================================================

    def wait_for_llama_ready(self, timeout=180):
        """
        Опрашивает эндпоинт /health у llama-server каждые 2 секунды,
        контролируя, что контейнер не упал (например, из-за нехватки VRAM).
        """
        print(f"[VRAM] Ожидание готовности llama-server (таймаут {timeout} сек)...")
        start = time.time()

        while time.time() - start < timeout:
            self.llama_container.reload()
            if self.llama_container.status in ("exited", "dead"):
                raise RuntimeError(
                    f"Контейнер '{LLAMA_CONTAINER_NAME}' неожиданно остановился "
                    f"(статус: {self.llama_container.status}). Проверь логи Docker на OOM!"
                )

            try:
                r = requests.get(f"{LLAMA_URL}/health", timeout=2)
                if r.status_code == 200 and r.json().get("status") == "ok":
                    print("[VRAM] Qwen 27B полностью загружен и готов к работе!")
                    return True
            except (requests.RequestException, ValueError):
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
            if self.comfy_container is not None:
                self.comfy_container.reload()
                if self.comfy_container.status in ("exited", "dead"):
                    raise RuntimeError(
                        f"Контейнер '{COMFY_CONTAINER_NAME}' упал при запуске "
                        f"(статус: {self.comfy_container.status})!"
                    )

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
        Отправляет POST-запрос в ComfyUI с приказом выгрузить чекпоинты из видеопамяти MI50.
        """
        try:
            requests.post(
                f"{COMFY_URL}/free",
                json={"unload_models": True, "free_memory": True},
                timeout=5
            )
            print("[VRAM] ComfyUI успешно выгрузил модели из VRAM.")
        except requests.RequestException as e:
            print(f"[INFO] ComfyUI не ответил на /free (вероятно, уже остановлен): {e}")

    def ensure_qwen_loaded(self):
        """
        Освобождает память от ComfyUI, запускает контейнер llama_qwen и ждет готовности /health.
        """
        self.llama_container.reload()

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
        Останавливает контейнер Qwen и проверяет, что контейнер ComfyUI реально запущен и отвечает.
        """
        if self.comfy_container is None:
            raise RuntimeError(f"Контейнер '{COMFY_CONTAINER_NAME}' отсутствует в системе, генерация картинок невозможна!")

        self.comfy_container.reload()
        if self.active_model == "comfy" and self.comfy_container.status == "running":
            return

        print("[SWITCH] Переключение VRAM: Останавливаем Qwen 27B -> Готовим ComfyUI...")

        # 1. Останавливаем контейнер llama_qwen
        self.llama_container.reload()
        if self.llama_container.status == "running":
            print(f"[DOCKER] Остановка контейнера {LLAMA_CONTAINER_NAME}...")
            self.llama_container.stop(timeout=10)

        # 2. Запускаем контейнер ComfyUI, если он не запущен
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

    def generate_text(self, prompt: str, llm_payload: dict | None = None) -> str:
        # Убеждаемся, что Qwen загружен в видеопамять
        self.ensure_qwen_loaded()

        # Если бот прислал готовый llm_payload — используем его, иначе собираем дефолтный
        if llm_payload and isinstance(llm_payload, dict) and "messages" in llm_payload:
            payload = llm_payload
        else:
            payload = {
                "messages": [
                    {
                        "role": "system",
                        "content": (
                            "Ты полезный ИИ-ассистент. Отвечай прямо, информативно и по делу "
                            "на русском языке. Избегай избыточных зацикленных рассуждений."
                        )
                    },
                    {"role": "user", "content": prompt}
                ],
                "temperature": 0.7,
                "max_tokens": 4096
            }

        print("[LLM] Отправка запроса в llama-server...")
        resp = requests.post(f"{LLAMA_URL}/v1/chat/completions", json=payload, timeout=180)
        resp.raise_for_status()

        msg = resp.json()["choices"][0]["message"]
        answer = (msg.get("content") or "").strip()

        # Если модель вернула <think>...</think> внутри content, аккуратно убираем черновые мысли
        if STRIP_THINK_TAGS and "<think>" in answer and "</think>" in answer:
            cleaned = re.sub(r"<think>.*?</think>", "", answer, flags=re.DOTALL).strip()
            if cleaned:
                answer = cleaned

        # Фоллбэк на reasoning_content, если основной content оказался пустым
        if not answer:
            answer = (msg.get("reasoning_content") or "").strip()

        print(f"[LLM] Ответ успешно получен ({len(answer)} символов).")
        return answer

    # =====================================================================
    # БЛОК ГЕНЕРАЦИИ ИЗОБРАЖЕНИЙ (COMFYUI)
    # =====================================================================

    def _load_and_prepare_workflow(self, prompt: str) -> dict:
        """Проверяет и подготавливает JSON воркфлоу ДО переключения контейнеров и VRAM."""
        if not os.path.exists(WORKFLOW_FILE):
            raise FileNotFoundError(f"Файл воркфлоу '{WORKFLOW_FILE}' не найден в рабочей папке!")

        with open(WORKFLOW_FILE, "r", encoding="utf-8") as f:
            workflow = json.load(f)

        if PROMPT_NODE_ID not in workflow:
            raise KeyError(f"Нода промпта с ID '{PROMPT_NODE_ID}' отсутствует в {WORKFLOW_FILE}!")
        if KSAMPLER_NODE_ID not in workflow:
            raise KeyError(f"Нода KSampler с ID '{KSAMPLER_NODE_ID}' отсутствует в {WORKFLOW_FILE}!")

        workflow[PROMPT_NODE_ID]["inputs"]["text"] = prompt
        workflow[KSAMPLER_NODE_ID]["inputs"]["seed"] = int.from_bytes(uuid.uuid4().bytes[:6], "big")
        return workflow

    def generate_image(self, prompt: str) -> str:
        # 1. Сначала валидируем файл графа, чтобы не выгружать Qwen зря при ошибке конфигурации
        workflow = self._load_and_prepare_workflow(prompt)

        # 2. Выгружаем Qwen и поднимаем ComfyUI
        self.ensure_comfy_loaded()

        client_id = str(uuid.uuid4())
        ws = websocket.WebSocket()
        ws.settimeout(300)  # Таймаут 5 минут на случай зависания генерации
        ws.connect(f"{COMFY_WS}?clientId={client_id}")

        try:
            print("[COMFY] Отправка графа на рендер...")
            resp = requests.post(
                f"{COMFY_URL}/prompt",
                json={"prompt": workflow, "client_id": client_id},
                timeout=10
            )
            resp.raise_for_status()
            prompt_id = resp.json()["prompt_id"]
            print(f"[COMFY] Задача принята в ComfyUI (prompt_id: {prompt_id}). Ожидание рендера...")

            while True:
                out = ws.recv()
                if not isinstance(out, str):
                    continue

                message = json.loads(out)
                msg_type = message.get("type")
                data = message.get("data", {})

                # Перехватываем ошибки выполнения внутри самого ComfyUI
                if msg_type == "execution_error" and data.get("prompt_id") == prompt_id:
                    err_msg = data.get("exception_message", "Неизвестная ошибка выполнения графа ComfyUI")
                    node_type = data.get("node_type", "UnknownNode")
                    raise RuntimeError(f"Ошибка в ноде {node_type}: {err_msg}")

                if msg_type == "execution_interrupted" and data.get("prompt_id") == prompt_id:
                    raise RuntimeError("Генерация в ComfyUI была прервана (execution_interrupted).")

                if msg_type == "executing":
                    if data.get("node") is None and data.get("prompt_id") == prompt_id:
                        print("[COMFY] Генерация изображения завершена!")
                        break
        finally:
            ws.close()

        # Запрашиваем историю выполнения по нашему prompt_id
        history_resp = requests.get(f"{COMFY_URL}/history/{prompt_id}", timeout=10)
        history_resp.raise_for_status()
        history = history_resp.json().get(prompt_id, {})

        for node_id, node_output in history.get("outputs", {}).items():
            images = node_output.get("images")
            if images:
                img_info = images[0]
                img_resp = requests.get(
                    f"{COMFY_URL}/view",
                    params={
                        "filename": img_info["filename"],
                        "subfolder": img_info.get("subfolder", ""),
                        "type": img_info.get("type", "output")
                    },
                    timeout=15
                )
                img_resp.raise_for_status()
                return base64.b64encode(img_resp.content).decode("utf-8")

        raise RuntimeError("ComfyUI завершил работу, но не вернул ни одного изображения в выходных нодах!")

    # =====================================================================
    # ГЛАВНЫЙ ЦИКЛ ОЧЕРЕДИ (REDIS LOOP)
    # =====================================================================

    def next_task_is_image(self) -> bool:
        """
        Проверяет первую задачу в очереди (индекс 0 при связке RPUSH на боте + BLPOP на воркере),
        чтобы не перезагружать Qwen зря, если следом идет еще одна картинка.
        """
        try:
            next_raw = self.rds.lindex(TASK_QUEUE, 0)
            if not next_raw:
                return False
            return json.loads(next_raw).get("type") == "image"
        except Exception:
            return False

    def push_result_safely(self, payload: dict):
        """Отправляет ответ в Redis с повторами, чтобы не потерять готовый результат при сбое сети."""
        raw_json = json.dumps(payload, ensure_ascii=False)
        for attempt in range(1, 4):
            try:
                self.rds.rpush(RESULT_QUEUE, raw_json)
                return
            except redis.exceptions.RedisError as e:
                print(f"[REDIS ERROR] Не удалось отправить результат (попытка {attempt}/3): {e}")
                time.sleep(2)
        print(f"[ERROR] Результат задачи {payload.get('task_id')} потерян после 3 попыток записи в Redis.")

    def run(self):
        print(f"[WORKER] Воркер активен! Слушаем очередь '{TASK_QUEUE}'...")
        while True:
            try:
                item = self.rds.blpop(TASK_QUEUE, timeout=5)
            except redis.exceptions.RedisError as e:
                print(f"[REDIS ERROR] Потеряна связь с Redis: {e}. Повтор через 5 сек...")
                time.sleep(5)
                continue

            # Если за 5 секунд задач не поступило:
            if not item:
                if self.active_model != "qwen":
                    print("[IDLE] Очередь пуста. Возвращаем дефолтную модель Qwen 27B в VRAM...")
                    try:
                        self.ensure_qwen_loaded()
                    except Exception as e:
                        print(f"[ERROR] Не удалось вернуть Qwen в режиме простоя: {e}")
                continue

            _, raw_task = item
            try:
                task = json.loads(raw_task)
            except json.JSONDecodeError:
                print(f"[ERROR] Получен битый JSON в очереди: {raw_task}")
                continue

            task_id = task.get("task_id", "unknown")
            channel_id = task.get("channel_id")
            task_type = task.get("type")
            prompt = task.get("prompt", "")

            print(f"\n[TASK {task_id}] Получена задача | Тип: {task_type} | Промпт: {prompt[:60]}...")

            try:
                if task_type == "text":
                    result_data = self.generate_text(
                        prompt=prompt,
                        llm_payload=task.get("llm_payload")
                    )
                elif task_type == "image":
                    result_data = self.generate_image(prompt)
                else:
                    raise ValueError(f"Неизвестный тип задачи: '{task_type}' (ожидается 'text' или 'image')")

                response_payload = {
                    "task_id": task_id,
                    "channel_id": channel_id,
                    "type": task_type,
                    "status": "ok",
                    "data": result_data
                }
                print(f"[TASK {task_id}] Успешно выполнено! Отправка ответа в '{RESULT_QUEUE}'.")

            except Exception as e:
                print(f"[ERROR] Ошибка выполнения задачи {task_id}: {e}")
                response_payload = {
                    "task_id": task_id,
                    "channel_id": channel_id,
                    "type": task_type,
                    "status": "error",
                    "data": str(e)
                }

            # 1. Сначала сразу отправляем готовый ответ (или ошибку) пользователю в Discord
            self.push_result_safely(response_payload)

            # 2. И только ПОСЛЕ отправки картинки проверяем, нужно ли заранее вернуть Qwen в VRAM
            if task_type == "image" and not self.next_task_is_image():
                try:
                    self.ensure_qwen_loaded()
                except Exception as e:
                    print(f"[WARN] Не удалось предварительно загрузить Qwen после рендера: {e}")


if __name__ == "__main__":
    worker = GPUWorker()
    worker.run()