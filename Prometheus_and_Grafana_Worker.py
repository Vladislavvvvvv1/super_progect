import base64
import json
import time
import uuid
import docker
import redis
import requests
import websocket
from prometheus_client import start_http_server, Counter, Gauge, Histogram

#                                                              --- КОНФИГ ---
REDIS_HOST = "192.168.1.10"  # МЕНЯЙ IP своей Master VM --------------------------------------------------------------------
REDIS_PORT = 6379
TASK_QUEUE = "tasks_queue"
RESULT_QUEUE = "results_queue"
METRICS_PORT = 8000  # Порт для сбора метрик Prometheus (/metrics)

LLAMA_CONTAINER_NAME = "llama_qwen"
LLAMA_URL = "http://127.0.0.1:8080"
COMFY_URL = "http://127.0.0.1:8188"
COMFY_WS = "ws://127.0.0.1:8188/ws"
WORKFLOW_FILE = "workflow_api.json"
PROMPT_NODE_ID = "6"  # ID ноды CLIPTextEncode (Positive Prompt) в твоем workflow_api.json
KSAMPLER_NODE_ID = "3"  # ID ноды KSampler для рандомизации seed

# --- ПРОМЕТЕУС МЕТРИКИ ---
METRIC_TASKS_TOTAL = Counter(
    "ai_tasks_total",
    "Общее количество обработанных задач",
    ["type", "status"]
)
METRIC_TASK_DURATION = Histogram(
    "ai_task_duration_seconds",
    "Время выполнения задач генерации в секундах",
    ["type"],
    buckets=(1.0, 2.5, 5.0, 10.0, 15.0, 30.0, 60.0, 120.0, 180.0)
)
METRIC_SWITCH_DURATION = Histogram(
    "ai_model_switch_duration_seconds",
    "Время переключения моделей в VRAM (сек)",
    ["target_model"],
    buckets=(0.5, 1.0, 1.5, 2.0, 3.0, 5.0, 10.0)
)
METRIC_ACTIVE_MODEL = Gauge(
    "ai_active_model",
    "Текущая модель в VRAM MI50 (1 = Qwen 27B, 2 = Nova ComfyUI, 0 = пусто)"
)
METRIC_QUEUE_LENGTH = Gauge(
    "ai_queue_length",
    "Текущее количество невыполненных задач в очереди Redis"
)


class GPUWorker:
    def __init__(self):
        self.rds = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        self.docker_client = docker.from_env()
        self.llama_container = self.docker_client.containers.get(LLAMA_CONTAINER_NAME)
        self.active_model = "qwen"  # По дефолту в VRAM всегда Qwen ------------
        METRIC_ACTIVE_MODEL.set(1)
        self.ensure_qwen_loaded()

    # --- УПРАВЛЕНИЕ VRAM ---

    def wait_for_llama_ready(self, timeout=30):
        """Ждет, пока llama-server загрузит веса из RAM-кэша в VRAM."""
        start = time.time()
        while time.time() - start < timeout:
            try:
                r = requests.get(f"{LLAMA_URL}/health", timeout=1)
                if r.status_code == 200 and r.json().get("status") == "ok":
                    print("[VRAM] Qwen 27B готов к работе в VRAM.")
                    return True
            except requests.RequestException:
                pass
            time.sleep(0.5)
        raise RuntimeError("Таймаут запуска llama-server!")

    def free_comfy_vram(self):
        """Приказывает ComfyUI полностью выгрузить чекпоинт из VRAM."""
        try:
            requests.post(
                f"{COMFY_URL}/free",
                json={"unload_models": True, "free_memory": True},
                timeout=5
            )
            print("[VRAM] ComfyUI очистил VRAM.")
        except requests.RequestException as e:
            print(f"[WARN] Не удалось очистить ComfyUI: {e}")

    def ensure_qwen_loaded(self):
        """Гарантирует, что ComfyUI выгружен, а Qwen загружен в VRAM."""
        if self.active_model == "qwen" and self.llama_container.status == "running":
            return

        switch_start = time.time()
        print("[SWITCH] Переключение VRAM: ComfyUI -> Qwen 27B...")
        self.free_comfy_vram()
        self.llama_container.reload()
        if self.llama_container.status != "running":
            self.llama_container.start()
        self.wait_for_llama_ready()

        self.active_model = "qwen"
        METRIC_ACTIVE_MODEL.set(1)
        METRIC_SWITCH_DURATION.labels(target_model="qwen").observe(time.time() - switch_start)

    def ensure_comfy_loaded(self):
        """Останавливает Qwen (веса остаются в Linux RAM Cache), освобождая VRAM под ComfyUI."""
        if self.active_model == "comfy":
            return

        switch_start = time.time()
        print("[SWITCH] Переключение VRAM: Qwen 27B -> ComfyUI Nova Anime XL ...")
        self.llama_container.reload()
        if self.llama_container.status == "running":
            self.llama_container.stop(timeout=2)

        self.active_model = "comfy"
        METRIC_ACTIVE_MODEL.set(2)
        METRIC_SWITCH_DURATION.labels(target_model="comfy").observe(time.time() - switch_start)
        print("[VRAM] VRAM освобождена под ComfyUI.")

    # --- ГЕНЕРАЦИЯ ТЕКСТА ЛАМА ---

    def generate_text(self, prompt: str) -> str:
        self.ensure_qwen_loaded()  # состояние памяти
        payload = {
            "messages": [
                {"role": "system", "content": "Ты полезный ИИ-ассистент."},
                {"role": "user", "content": prompt}  # Отправка
            ],
            "temperature": 0.7,  # Послушание
            "max_tokens": 1024  # выходные токены
        }
        resp = requests.post(f"{LLAMA_URL}/v1/chat/completions", json=payload, timeout=180)
        resp.raise_for_status()
        return resp.json()["choices"][0]["message"]["content"]  # распаковывает многослойный JSON-ответ

    # --- ГЕНЕРАЦИЯ ИЗОБРАЖЕНИЙ (COMFYUI) ---

    def generate_image(self, prompt: str) -> str:
        self.ensure_comfy_loaded()

        with open(WORKFLOW_FILE, "r", encoding="utf-8") as f:
            workflow = json.load(f)

        # Подставляем промт и случайный сид в граф ComfyUI
        workflow[PROMPT_NODE_ID]["inputs"]["text"] = prompt
        workflow[KSAMPLER_NODE_ID]["inputs"]["seed"] = int.from_bytes(uuid.uuid4().bytes[:6], "big")

        client_id = str(uuid.uuid4())
        ws = websocket.WebSocket()
        ws.connect(f"{COMFY_WS}?clientId={client_id}")

        # Отправляем задачу в ComfyUI
        resp = requests.post(
            f"{COMFY_URL}/prompt",
            json={"prompt": workflow, "client_id": client_id},
            timeout=10
        )
        resp.raise_for_status()
        prompt_id = resp.json()["prompt_id"]

        # Ждем завершения генерации по WebSocket
        while True:
            out = ws.recv()
            if isinstance(out, str):
                message = json.loads(out)
                if message["type"] == "executing":
                    data = message["data"]
                    if data["node"] is None and data["prompt_id"] == prompt_id:
                        break
        ws.close()

        # Забираем сгенерированную картинку из истории ComfyUI
        history = requests.get(f"{COMFY_URL}/history/{prompt_id}", timeout=10).json()[prompt_id]
        for node_id, node_output in history["outputs"].items():
            if "images" in node_output:
                img_info = node_output["images"][0]
                img_bytes = requests.get(
                    f"{COMFY_URL}/view",
                    params={
                        "filename": img_info["filename"],
                        "subfolder": img_info["subfolder"],
                        "type": img_info["type"]
                    },
                    timeout=10
                ).content
                return base64.b64encode(img_bytes).decode("utf-8")

        raise RuntimeError("ComfyUI не вернул изображение!(,,>﹏<,,💢) b-baka!)")

    # --- ЦИКЛ ОБРАБОТКИ ОЧЕРЕДИ ---

    def next_task_is_image(self) -> bool:
        """Проверяет первый элемент в очереди без его удаления (LINDEX)."""
        next_raw = self.rds.lindex(TASK_QUEUE, 0)
        if not next_raw:
            return False
        try:
            return json.loads(next_raw).get("type") == "image"
        except Exception:
            return False

    def run(self):
        # Запускаем метрик-сервер Prometheus на порту 8000
        start_http_server(METRICS_PORT)
        print(f"[METRICS] Экспортер Prometheus запущен на http://0.0.0.0:{METRICS_PORT}/metrics")
        print("[WORKER] Воркер запущен и слушает очередь Redis...")

        while True:
            # Обновляем длину очереди в Redis для мониторинга в Grafana
            try:
                queue_len = self.rds.llen(TASK_QUEUE)
                METRIC_QUEUE_LENGTH.set(queue_len)
            except Exception:
                pass

            # Ждем задачу с таймаутом 5 секунд
            item = self.rds.blpop(TASK_QUEUE, timeout=5)

            if not item:
                # Если очередь пуста, а в памяти осталась Nova — возвращаем дефолтный Qwen💢
                if self.active_model != "qwen":
                    print("[IDLE] Очередь пуста. Возвращаем дефолтную модель Qwen в VRAM...")
                    self.ensure_qwen_loaded()
                continue

            _, raw_task = item
            task = json.loads(raw_task)
            task_id = task.get("task_id")
            task_type = task.get("type")
            prompt = task.get("prompt", "")

            print(f"[TASK {task_id}] Тип: {task_type} | Промт: {prompt[:50]}...")

            task_start_time = time.time()
            try:
                if task_type == "text":
                    result_data = self.generate_text(prompt)
                elif task_type == "image":
                    result_data = self.generate_image(prompt)
                    # Если следующая задача в очереди НЕ картинка — сразу возвращаем Qwen в VRAM
                    if not self.next_task_is_image():
                        self.ensure_qwen_loaded()
                else:
                    raise ValueError(f"Неизвестный тип задачи: {task_type}")

                # Успешное выполнение: записываем время и инкрементим счетчик
                task_duration = time.time() - task_start_time
                METRIC_TASK_DURATION.labels(type=task_type).observe(task_duration)
                METRIC_TASKS_TOTAL.labels(type=task_type, status="ok").inc()

                response_payload = {
                    "task_id": task_id,
                    "channel_id": task.get("channel_id"),
                    "type": task_type,
                    "status": "ok",
                    "data": result_data
                }
            except Exception as e:
                # Ошибка выполнения: инкрементим счетчик с меткой error
                METRIC_TASKS_TOTAL.labels(type=task_type, status="error").inc()
                print(f"[ERROR] Ошибка выполнения задачи {task_id}: {e}")
                response_payload = {
                    "task_id": task_id,
                    "channel_id": task.get("channel_id"),
                    "type": task_type,
                    "status": "error",
                    "data": str(e)
                }

            self.rds.rpush(RESULT_QUEUE, json.dumps(response_payload))


if __name__ == "__main__":
    worker = GPUWorker()
    worker.run()