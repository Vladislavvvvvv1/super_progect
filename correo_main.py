import os
import io
import re
import json
import base64
import asyncio
import datetime
import random

import discord
from discord import app_commands
import redis.asyncio as redis
from dotenv import load_dotenv

# ============================================ БЛОК 2: Загрузка из env ============================================
load_dotenv('token_by_correo.env')
load_dotenv('vip_passwords.env')

TOKEN = os.getenv('TOKEN')

REDIS_HOST = 'localhost'
REDIS_PORT = 6379
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD')
TASK_QUEUE = 'tasks_queue'
RESULT_QUEUE = 'results_queue'

# Считываем строку из env, режем по запятой и превращаем в цундерку
raw_passwords = os.getenv('VALID_PASSWORDS', '')
VALID_PASSWORDS = {p.strip() for p in raw_passwords.split(',') if p.strip()}

FREE_LIMIT = 5                  # Лимит запросов для простых смертных
FREE_LIMIT_TTL = 24 * 3600      # Сброс лимита через 24 часа
VIP_TTL = 31 * 24 * 3600        # Срок действия пароля --------------------------------------------------------- Я ЗДЕСЬ Я ВИПКА
TIMEOUT_TTL = 3600              # Время мута за перебор пароля
MAX_PASS_FAILS = 5              # Максимум ошибок при вводе пароля

# ============================================ БЛОК 3: Инициализация клиента и древа команд ============================================
intents = discord.Intents.default()
client = discord.Client(intents=intents)
tree = app_commands.CommandTree(client)

# ============================================= БЛОК 4: Асинхронный клиент Redis ============================================
redis_client = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    password=REDIS_PASSWORD,  # если в redis.conf выключен "requirepass", то закомментируй
    decode_responses=True,
    socket_timeout=15,
    socket_connect_timeout=5,
    health_check_interval=30
)


def get_private_channel_name(user: discord.abc.User) -> str:
    """Формирует валидное имя приватного канала без точек и спецсимволов (как это делает Discord)."""
    clean_name = re.sub(r"[^a-z0-9_-]", "", user.name.lower())
    return f"ai-{clean_name or user.id}"


# ============================================= БЛОК 4.1: Проверка доступа, канала, лимитов ============================================
async def check_ai_access(interaction: discord.Interaction) -> bool:
    """Проверяет, есть ли у пользователя доступ к ИИ в текущем канале и не исчерпан ли суточный лимит."""
    user_id = interaction.user.id

    # Запрещаем писать боту в ЛС в обход сервера                                                                                                                                     большой брат следит за тобой
    if interaction.guild is None:
        await interaction.response.send_message("❌ Бот работает только на сервере!", ephemeral=True)
        return False

    # Смотрим не в тайм-ауте ли пользователь за форс
    if await redis_client.exists(f"bot_ban:{user_id}"):
        ttl = await redis_client.ttl(f"bot_ban:{user_id}")
        await interaction.response.send_message(
            f"⏳ Ты в тайм-ауте за перебор пароля! Жди еще {max(1, ttl // 60)} мин.",
            ephemeral=True
        )
        return False

    # Глядим введен ли у пользователя пароль
    is_vip = await redis_client.exists(f"vip:{user_id}")
    channel_name = getattr(interaction.channel, "name", "")
    is_private_channel = channel_name.startswith("ai-")

    # Если пароль активен — нет лимитов, нет запретов
    if is_vip:
        return True

    # 4. Если пароля нет или истек, то забираем благо
    if is_private_channel:
        await interaction.response.send_message(
            "🔒 Безлимит истек! Введи `/password`, чтобы продлить доступ к приватному чату.",
            ephemeral=True
        )
        return False

    # Проверяем суточный лимит в публичном канале
    used_raw = await redis_client.get(f"free_used:{user_id}")
    used = int(used_raw) if used_raw else 0

    if used >= FREE_LIMIT:
        ttl = await redis_client.ttl(f"free_used:{user_id}")
        hours = max(0, ttl) // 3600
        minutes = (max(0, ttl) % 3600) // 60
        await interaction.response.send_message(
            f"🛑 Твой суточный лимит ({FREE_LIMIT}) исчерпан!\n"
            f"⏳ Лимит сбросится через: **{hours} ч. {minutes} мин.**\n"
            f"💢Чтобы снять все лимиты и получить личный чат, введи ключ через `/password`.",
            ephemeral=True
        )
        return False

    new_used = await redis_client.incr(f"free_used:{user_id}")

    # Если это первый запрос за сутки — таймер на 24ч
    if new_used == 1 or await redis_client.ttl(f"free_used:{user_id}") == -1:
        await redis_client.expire(f"free_used:{user_id}", FREE_LIMIT_TTL)

    return True


# ============================================= БЛОК 5.1: Фоновый слушатель готовых ответов от Воркера ============================================
async def listen_results():
    """Фоновая задача: забирает готовые ответы из Redis и редактирует сообщения в Discord."""
    await client.wait_until_ready()
    print("Бот💢: 🎧 Слушатель очереди результатов запущен!")

    while not client.is_closed():
        try:
            item = await redis_client.blpop(RESULT_QUEUE, timeout=5)
            if not item:
                continue

            _, raw_result = item
            result = json.loads(raw_result)

            raw_task_id = result.get("task_id")
            raw_channel_id = result.get("channel_id")

            # Защита от битых ID (например, "unknown" или None при ошибке парсинга на воркере)
            if not str(raw_task_id).isdigit() or not str(raw_channel_id).isdigit():
                print(f"Бот💢: Пропущен ответ с некорректными ID: {result}")
                continue

            task_id = int(raw_task_id)
            channel_id = int(raw_channel_id)
            task_type = result.get("type")
            status = result.get("status")
            data = result.get("data", "")

            channel = client.get_channel(channel_id) or await client.fetch_channel(channel_id)
            message = await channel.fetch_message(task_id)

            if status != "ok":
                await message.edit(content=f"❌ **Ошибка генерации:**\n`{data}`")
                continue

            # --- ГОТОВА КАРТИНКА ---
            if task_type == "image":
                image_bytes = base64.b64decode(data)
                file = discord.File(io.BytesIO(image_bytes), filename="generated.png")
                await message.edit(content="🎨 **Изображение готово!**", attachments=[file])

            # --- ГОТОВ ТЕКСТ ---
            elif task_type == "text":
                if not data:
                    data = "⚠️ Модель вернула пустой ответ."

                if len(data) <= 2000:
                    await message.edit(content=data)
                else:
                    chunks = [data[i:i + 1990] for i in range(0, len(data), 1990)]
                    await message.edit(content=chunks[0])
                    for chunk in chunks[1:]:
                        await channel.send(content=chunk, reference=message)

        except (redis.TimeoutError, asyncio.TimeoutError):
            continue

        except (discord.NotFound, discord.Forbidden) as e:
            print(f"Бот💢: Не удалось обновить сообщение в Discord: {e}")

        except Exception as e:
            print(f"Бот💢: Ошибка в цикле чтения результатов: {e}")
            await asyncio.sleep(2)


# ============================================= БЛОК 5: Логи и синхронизация слеш-команд ==============================
@client.event
async def on_ready():
    print(f'Бот💢: Бот {client.user} успешно запущен!')
    try:
        await redis_client.ping()
        print("Бот💢: Успешное подключение к редиске!")
    except Exception as e:
        print(f'Бот💢: Ошибка подключения к редиске: {e} (,,>﹏<,,💢)')

    if not hasattr(client, 'synced_once'):
        try:
            synced = await tree.sync()
            client.synced_once = True
            print(f'Бот💢: Синхронизировано команд: {len(synced)}')
        except Exception as e:
            print(f'Бот💢: Ошибка синхронизации: {e} я ломаюсь не потому-что так хочу а потому-что рукожоп на разрабе 💢')

    if not hasattr(client, 'result_listener_task'):
        client.result_listener_task = client.loop.create_task(listen_results())


# ========================================= БЛОК 6: Обработчик команды /start ===========================================
@tree.command(name="start", description="Привет")
async def start(interaction: discord.Interaction):
    await interaction.response.send_message(f'Привет, {interaction.user.mention}! Чем займемся?😁')


# ========================================= БЛОК 7: Обработчик команды /stop ================================================
@tree.command(name="stop", description="Пока - пока")
async def stop(interaction: discord.Interaction):
    gifs = random.choice([
        'https://klipy.com/gifs/sviaz-3',
        'https://klipy.com/gifs/anime-tsundere-6',
        'https://tenor.com/view/dog-ball-dog-gets-ball-meme-bottles-gif-25461285',
        'https://cdn.discordapp.com/attachments/1277529653879373876/1534820706120892437/ezgif-749128c5d625158b.gif',
        'https://media.giphy.com/media/v1.Y2lkPTc5MGI3NjExZDBxdTJudnI1bTE2cGlvOWZoeXY5cXFraDZwMHFmbzB0dmlteTZlcyZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/HhAswFzculTnQ0abml/giphy.gif',
        'https://tenor.com/view/%D0%B3%D0%BE%D0%BB%D0%BE%D0%B2%D0%BE%D0%BB%D0%BE%D0%BC%D0%BA%D0%B0-%D1%8D%D0%BC%D0%BE%D1%86%D0%B8%D1%8F-%D0%B8%D0%B2%D0%B0%D0%BD-%D0%B7%D0%BE%D0%BB%D0%BE-gif-16215278410832285228',
    ])
    await interaction.response.send_message(gifs)


# ============================================= БЛОК 8: СБОРЩИК ПЕЙЛОАДА (PAYLOAD BUILDER) ============================================
def build_task_payload(
    message_id: int,
    channel_id: int,
    user_id: int,
    task_type: str,
    prompt: str,
    user_name: str | None = None,
    extra_context: str | None = None
) -> str:


    clean_prompt = prompt.strip()
    if not clean_prompt:
        raise ValueError("Промпт не может быть пустым.")

    if task_type == "text":
        parts = []
        if user_name:
            parts.append(f"[Пользователь: {user_name}]")
        if extra_context:
            parts.append(f"[Контекст:\n{extra_context.strip()}]")
        parts.append(clean_prompt)
        final_prompt = "\n\n".join(parts)
    else:
        final_prompt = clean_prompt

    task_payload = {
        "task_id": str(message_id),   # Строка в воркере, в listen_results() кастуется обратно в int
        "channel_id": channel_id,     # Нужен воркеру для возврата в results_queue
        "user_id": user_id,
        "type": task_type,            # "text" или "image"
        "prompt": final_prompt
    }

    # ensure_ascii=False сохраняет читаемую кириллицу в Redis
    return json.dumps(task_payload, ensure_ascii=False)


# ======================================= БЛОК 9: Обработчик команды /ask и Redis ==============================
@tree.command(name="ask", description="Текстовый агент")
async def ask(interaction: discord.Interaction, prompt: str):
    if not prompt.strip():
        await interaction.response.send_message("❌ Промпт не может быть пустым!", ephemeral=True)
        return

    if not await check_ai_access(interaction):
        return

    await interaction.response.defer(thinking=True)

    msg = await interaction.followup.send(
        "📌 Задача в очереди, ждем ответ от Агента...",
        wait=True
    )

    raw_payload = build_task_payload(
        message_id=msg.id,
        channel_id=interaction.channel_id,
        user_id=interaction.user.id,
        task_type="text",
        prompt=prompt,
        user_name=interaction.user.display_name
    )
    await redis_client.rpush(TASK_QUEUE, raw_payload)


# ============================== БЛОК 10: Обработчик команды /gen ==============================
@tree.command(name="gen", description="Сгенерировать изображение")
async def gen(interaction: discord.Interaction, prompt: str):
    if not prompt.strip():
        await interaction.response.send_message("❌ Промпт не может быть пустым!", ephemeral=True)
        return

    if not await check_ai_access(interaction):
        return

    await interaction.response.defer(thinking=True)

    msg = await interaction.followup.send(
        "📌 Запрос на генерацию изображения в очереди...",
        wait=True
    )

    raw_payload = build_task_payload(
        message_id=msg.id,
        channel_id=interaction.channel_id,
        user_id=interaction.user.id,
        task_type="image",
        prompt=prompt
    )
    await redis_client.rpush(TASK_QUEUE, raw_payload)


# ============================== БЛОК 10.1: Авторизация по паролю ==============================
@tree.command(name="password", description="Свой?")
@app_commands.describe(key="Секретный пароль💢:")
async def password_cmd(interaction: discord.Interaction, key: str):
    if interaction.guild is None:
        await interaction.response.send_message("❌ Вводить пароль можно только на сервере!", ephemeral=True)
        return

    user = interaction.user
    user_id = user.id

    # Проверяем не в бане ли пользователь за перебор
    if await redis_client.exists(f"bot_ban:{user_id}"):
        ttl = await redis_client.ttl(f"bot_ban:{user_id}")
        await interaction.response.send_message(
            f"⏳ Ты в тайм-ауте за перебор пароля! Жди еще {max(1, ttl // 60)} мин.",
            ephemeral=True
        )
        return

    # Если пароль неверный - КАЗНЬ                                                                       ГОООООООООООЙДА
    if key.strip() not in VALID_PASSWORDS:
        fails = await redis_client.incr(f"pass_fails:{user_id}")
        await redis_client.expire(f"pass_fails:{user_id}", TIMEOUT_TTL)

        if fails >= MAX_PASS_FAILS:
            await redis_client.delete(f"pass_fails:{user_id}")
            await redis_client.set(f"bot_ban:{user_id}", "1", ex=TIMEOUT_TTL)

            timeout_until = discord.utils.utcnow() + datetime.timedelta(hours=1)
            try:
                if isinstance(user, discord.Member):
                    await user.timeout(timeout_until, reason="5 неверных попыток ввода пароля")
                await interaction.response.send_message(
                    "🚫 5 неверных попыток 🚫! Ты отправлен в тайм-аут на 1 час.",
                    ephemeral=True
                )
            except discord.Forbidden:
                await interaction.response.send_message(
                    " Я не доступна... не потому что так хочу. А потому что... 🚫 5 ошибок! 🚫",
                    ephemeral=True
                )
            return

        left = MAX_PASS_FAILS - fails
        await interaction.response.send_message(
            f"💢 Неверный пароль! Осталось попыток: **{left}**",
            ephemeral=True
        )
        return

    # Если пароль ВЕРНЫЙ — даем VIP на n дней и сбрасываем ошибки
    await redis_client.set(f"vip:{user_id}", "1", ex=VIP_TTL)
    await redis_client.delete(f"pass_fails:{user_id}")

    await interaction.response.send_message(
        "Принято",
        ephemeral=True
    )


# ============================== БЛОК 10.2: Создание и удаление приватного чата ==============================
@tree.command(name="gen_chat", description="Создать личный приватный канал для общения с ИИ")
async def gen_chat(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)

    guild = interaction.guild
    user = interaction.user
    user_id = user.id

    if guild is None:
        await interaction.followup.send("❌ Команда работает только на сервере!", ephemeral=True)
        return

    is_vip = await redis_client.exists(f"vip:{user_id}")
    if not is_vip:
        await interaction.followup.send(
            "🔒 Создавать личные комнаты могут только... Vip`ки 💢:!\n"
            "Сначала введи пароль `/password key: твой_пароль`.",
            ephemeral=True
        )
        return

    channel_name = get_private_channel_name(user)

    existing_channel = discord.utils.get(guild.text_channels, name=channel_name)
    if existing_channel:
        await interaction.followup.send(
            f"💢 У тебя уже есть личная комната: {existing_channel.mention}!",
            ephemeral=True
        )
        return

    overwrites = {
        guild.default_role: discord.PermissionOverwrite(view_channel=False),
        user: discord.PermissionOverwrite(
            view_channel=True,
            send_messages=True,
            read_message_history=True
        ),
        guild.me: discord.PermissionOverwrite(
            view_channel=True,
            send_messages=True,
            read_message_history=True,
            manage_channels=True
        )
    }

    try:
        new_channel = await guild.create_text_channel(
            name=channel_name,
            overwrites=overwrites,
            reason=f"Приватный ИИ-чат для {user}"
        )
        await interaction.followup.send(
            f"✅ Личная комната создана: {new_channel.mention}",
            ephemeral=True
        )
        await new_channel.send(
            f"Привет, {user.mention}! Это твой личный чат. Пиши запросы через `/ask` или `/gen` 💢"
        )
    except discord.Forbidden:
        await interaction.followup.send(
            "❌ У меня нет прав на создание каналов (`Manage Channels`)! Проверь роль бота в настройках сервера.",
            ephemeral=True
        )
    except Exception as e:
        await interaction.followup.send(f"❌ Ошибка при создании канала: `{e}`", ephemeral=True)


@tree.command(name="del_chat", description="Удалить свой личный приватный канал")
async def del_chat(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)

    if interaction.guild is None:
        await interaction.followup.send("❌ Команда работает только на сервере!", ephemeral=True)
        return

    channel_name = get_private_channel_name(interaction.user)
    existing_channel = discord.utils.get(interaction.guild.text_channels, name=channel_name)

    if not existing_channel:
        await interaction.followup.send("💢 У тебя нет активной личной комнаты.", ephemeral=True)
        return

    try:
        await existing_channel.delete(reason=f"Удалено владельцем {interaction.user}")
        if interaction.channel != existing_channel:
            await interaction.followup.send("🗑️ Твоя личная комната удалена.", ephemeral=True)
    except discord.Forbidden:
        await interaction.followup.send("❌ Не хватает прав для удаления канала!", ephemeral=True)


# ======================================== БЛОК 11: Поехали ============================================
client.run(TOKEN)