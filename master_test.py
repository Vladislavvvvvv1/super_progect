import sys
import base64
import json
import time
import uuid
import redis

# --- НАСТРОЙКИ ---
# Мастер работает на той же машине, где крутится Redis, поэтому подключаемся к 127.0.0.1
REDIS_HOST = "127.0.0.1"
REDIS_PORT = 6379

# Имена очередей строго такие же, как в worker.py
TASK_QUEUE = "tasks_queue"
RESULT_QUEUE = "results_queue"

# Подключение к Redis
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


def send_task(task_type: str, prompt: str):
    task_id = str(uuid.uuid4())[:8]

    # Собираем payload точно под формат, который ждет worker.py
    payload = {
        "task_id": task_id,
        "channel_id": "test_cli",
        "type": task_type,     # "text" или "image"
        "prompt": prompt
    }

    print(f"\n[MASTER] Отправляем задачу [{task_id}] (тип: {task_type}) в '{TASK_QUEUE}'...")
    print(f"[MASTER] Промпт: \"{prompt}\"")

    # Кладем задачу в очередь (LPUSH)
    r.lpush(TASK_QUEUE, json.dumps(payload, ensure_ascii=False))

    print("[MASTER] Ожидаем ответ от Worker (блокирующий BLPOP)...")
    start_time = time.time()

    # Ждем ответ из очереди результатов до 180 секунд
    response = r.blpop(RESULT_QUEUE, timeout=180)

    elapsed = round(time.time() - start_time, 2)

    if not response:
        print(f"[MASTER ERROR] Таймаут! Воркер не ответил за 180 секунд.")
        return

    # response — это кортеж ('results_queue', '{"task_id": ...}')
    _, raw_data = response
    res = json.loads(raw_data)

    print(f"\n=== [ОТВЕТ ПОЛУЧЕН за {elapsed} сек] ===")
    print(f"Task ID: {res.get('task_id')}")
    print(f"Статус:  {res.get('status')}")

    if res.get("status") == "ok":
        if task_type == "text":
            print(f"\nОтвет модели Qwen 27B:\n{res.get('data')}")
        elif task_type == "image":
            # Если это картинка, она приходит в base64
            img_b64 = res.get("data")
            output_file = f"output_{task_id}.png"
            with open(output_file, "wb") as f:
                f.write(base64.b64decode(img_b64))
            print(f"Картинка успешно сохранена на диск: {output_file}")
    else:
        print(f"Ошибка от воркера: {res.get('data')}")


def main():
    try:
        r.ping()
        print(f"[OK] Успешное подключение к Redis на {REDIS_HOST}:{REDIS_PORT}")
    except Exception as e:
        print(f"[ERROR] Не удалось подключиться к Redis: {e}")
        return

    print("=" * 50)
    print("Выберите действие для теста:")
    print("1 — Отправить текстовый промпт в Qwen 27B")
    print("2 — Отправить генерацию картинки в ComfyUI")
    print("3 — Свой текстовый промпт")
    print("=" * 50)

    choice = input("Введите цифру (1/2/3): ").strip()

    if choice == "1":
        send_task("text", "Привет! Ответь коротко: ты работаешь на GPU AMD MI50?")
    elif choice == "2":
        send_task("image","""2girls,(They are looking at each other:1.3),(face focus:1.7),
            (columbina_(genshin_impact),  purple eyes,  white pupils, gradient eyes,  head wings, x hair ribbon ,  blunt bangs, black hair, two-tone hair ,purple hair, streaked hair ,   wrist ribbon, detached sleeves,  white choker，off-shoulder dress, bare shoulders,   leg ribbon, criss-cross lacing, thigh strap, purple eyes, white pupils, gradient eyes,, translucent mask ,eye mask, closed eyes:1.3)
            BREAK
            (S4ndr0ne, blue eyes, light brown hair,  hair bun, short hair, blunt bangs, long sidelocks, long twin sidelocks, maid headdress, white frilled headdress, gold trim, off shoulder dress, long detached sleeves, black detached sleeves, high neck collar, black frilled collar, red gemstone pendant, white corset, corset bodice, gold filigree, black waist cincher, white apron, ornate apron, layered skirt, asymmetrical skirt, overskirt, split skirt, red inner skirt, black outer skirt, back bow, large back bow, waist sash, red sash, white thighhighs, lace thighhighs, black and white outfit, red accents:1.3)
            BREAK
            (open mouth, adult kiss, tongues touching, saliva dripping from the mouth,nsfw,2girls:1.3), columbina_(genshin_impact), S4ndr0ne, close-up, heart hand sign, finger heart, blushing, smiling, head-to-head, eyes closed, half-closed eyes, anime style, digital art.
            This detailed close-up illustration depicts columbina_(genshin_impact) and S4ndr0ne from  \(genshin impact\),leaning head-to-head, their foreheads gently touching. Together, they are forming a heart shape with their hands right in front of their faces,
            
            On the left is columbina_(genshin_impact), with her long black hair featuring pink-purple streaks. She has a light, playful smile and one eye is mischievously narrowed, conveying a fun-loving mood, She wears a white off-the-shoulder dress with intricate lacing details across the chest.
            
            On the right is S4ndr0ne, with her ashen-brown hair styled in complex buns on the sides. She looks visibly embarrassed and shy, with a heavy blush covering her cheeks and her eyes squinted shut.  She wears an elaborate ruffled mob cap adorned with gold trim and a high-collared dress with frills and a bow. She is wearing a white glove on her hand.
            at the room,
            (masterpiece, best quality, ultra-detailed, absurdres, highres, 8k, very aesthetic, highly detailed, cinematic, depth of field, raytracing, volumetric light, ambient occlusion, dynamic lighting, glowing particles, bokeh, realistic skin texture, glossy highlights, shiny skin, detailed skin pores, HDR, DSLR, rim lighting,solo,  oily skin highlights,  blush,  detailed fingers,sweet facial expression, cinematic lighting, dynamic lighting, depth of field, volumetric lighting, bokeh, HDR, glowing particles, DSLR effect,  cabstl)" )""")
    elif choice == "3":
        print("Введите ваш промпт: ", end="", flush=True)
        user_prompt = sys.stdin.buffer.readline().decode('utf-8', errors='replace').strip()
        send_task("text", user_prompt)
    else:
        print("Неверный выбор.")

if __name__ == "__main__":
    main()
