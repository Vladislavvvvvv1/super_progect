import os
import discord
from discord import app_commands
from dotenv import load_dotenv


load_dotenv("token_by_correo.env")
TOKEN = os.getenv("TOKEN")


intents = discord.Intents.default()
client = discord.Client(intents=intents)
tree = app_commands.CommandTree(client)

@client.event
async def on_ready():
    print(f'Бот {client.user} успешно запущен!')
    try:
        synced = await tree.sync()
        print(f'Синхронизировано слэш-команд: {len(synced)}')
    except Exception as e:
        print(f'Ошибка синхронизации: {e}')

@tree.command(name="start", description="Запустить бота")
async def start(interaction: discord.Interaction):
    await interaction.response.send_message(f'Привет, {interaction.user.mention}! Чем займемся?.')

@tree.command(name="stop", description="Пока - пока")
async def stop(interaction: discord.Interaction):
    await interaction.response.send_message(f'Пока, {interaction.user.mention}!')

@tree.command(name="promt", description="Промт окажется в чате")
async def promt(interaction: discord.Interaction, prompt: str):
    await interaction.response.send_message(f'Промт: {prompt}')

client.run(TOKEN)