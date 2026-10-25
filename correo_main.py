import os
import discord
from discord import app_commands
import redis
import json
from dotenv import load_dotenv

# ============================================ БЛОК 2: Загрузка конфигурации и токена из env файла ============================================
load_dotenv('token_by_correo.env')
TOKEN = os.getenv('TOKEN')

load_dotenv("token_by_correo.env")
TOKEN = os.getenv("TOKEN")


# ============================================ БЛОК 3: Инициализация Discord-клиента, настроек intents и дерева команд ============================================
intents = discord.Intents.default()
client = discord.Client(intents=intents)
tree = app_commands.CommandTree(client)

# ============================================= БЛОК 4: Подключение к локальной базе данных Redis для работы с очередями задач ============================================
try:
    redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)
    redis_client.ping()
    print("Успешх!")
except Exception as e:
    print(f' Ошибка подключения к редиске: {e}')



# ============================================= БЛОК 5: Событие успешного запуска бота и синхронизация слеш-команд с Discord ==============================
@client.event
async def on_ready():
    print(f'Бот {client.user} успешно запущен!')
    try:
        synced = await tree.sync()
        print(f'Синхронизировано команд: {len(synced)}')
    except Exception as e:
        print(f'Ошибка синхронизации: {e}')

# ========================================= БЛОК 6: Обработчик команды /start (отправляет приветствие пользователю) ===========================================
@tree.command(name="start", description="Получить приветствие от бота")
async def start(interaction: discord.Interaction):
    await interaction.response.send_message(f'Привет, {interaction.user.mention}! Чем займемся?😁')


# ========================================= БЛОК 7: Обработчик команды /stop (отправляет ссылку на гифку) ================================================

@tree.command(name="stop", description="Пока - пока")
async def stop(interaction: discord.Interaction):
    await interaction.response.send_message(f'https://klipy.com/gifs/sviaz-3')



# ======================================= БЛОК 8: Обработчик команды /ask (добавление запроса к текстовой нейросети в очередь Redis) ==============================
@tree.command(name="ask", description="Задать вопрос текстовой нейросети")
async def ask(interaction: discord.Interaction, prompt: str):


    # ---Откладываем ответ, так как обработка воркером может занять время---
    await interaction.response.defer(thinking=True)

    # ---Формируем словарь с данными задачи---
    task = {
        "type": "ask",
        "user_id": interaction.user.id,
        "interaction_id": interaction.id,
        "prompt": prompt
    }
    # ---Пушим задачу в список очередей Redis--
    redis_client.rpush("task_queue", json.dumps(task))

    await interaction.followup.send("🥳Задача добавлена в очередь, ждем ответ ...")


# ============================== БЛОК 9: Обработчик команды /gen (добавление запроса на генерацию изображения в очередь Redis) ==============================
@tree.command(name="gen", description="Сгенерировать изображение")
async def gen(interaction: discord.Interaction, prompt: str):

    # ---Откладываем ответ на время обработки---
    await interaction.response.defer(thinking=True)

    # ---Формируем структуру задачи для генератора---
    task = {
        "type": "gen",
        "user_id": interaction.user.id,
        "interaction_id": interaction.id,
        "prompt": prompt
    }
    # ---Отправляем задачу в очередь Redis---
    redis_client.rpush("task_queue", json.dumps(task))

    await interaction.followup.send("😉 Запрос на генерацию изображения отправлен в очередь...")

@tree.command(name="promt", description="Промт окажется в чате")
async def promt(interaction: discord.Interaction, prompt: str):
    await interaction.response.send_message(f'Промт: {prompt}')

# ======================================== БЛОК 10: Запуск ============================================
client.run(TOKEN)
