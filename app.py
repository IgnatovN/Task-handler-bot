"""Task handler bot
Functions:
1. Add task to list
2. Change task status to 'completed' and delete it
3. Show task list"""

import logging
import os
from tabulate import tabulate

from aiogram import Bot, Dispatcher, executor, types
from clickhouse_driver import Client
import pandas as pd

logging.basicConfig(
    format="%(levelname)s; %(asctime)s - %(message)s",
    datefmt="%d-%b-%y %H-%M-%S",
    level=logging.INFO
)

APP_TOKEN = os.environ.get('TOKEN')

PATH_TO_TABLE = 'todo_list.csv'

bot = Bot(token=APP_TOKEN)
dp = Dispatcher(bot)

connection = Client(
    host='localhost',
    user='user',
    password='password',
    port=9000,
    database='todo'
)


@dp.message_handler(commands='all')
async def all_tasks(payload: types.Message):
    """Show all tasks"""
    all_data = connection.execute("SELECT * FROM todo.todo")

    await payload.reply(
        f"```{pd.DataFrame(all_data, columns=['text', 'status']).to_markdown()}```",
        parse_mode='Markdown'
    )


@dp.message_handler(commands='add')
async def add_task(payload: types.Message):
    """Add task to list"""
    text = payload.get_args().strip()

    connection.execute(
        "INSERT INTO todo.todo (text, status) VALUES (%(text)s, %(status)s)",
        {"text": text, "status": "active"}
    )

    logging.info('Добавлена задача - %s', text)
    await payload.reply(f'Добавлена задача - "{text}"', parse_mode='Markdown')


@dp.message_handler(commands='done')
async def complete_task(payload: types.Message):
    """Mark completed tasks handler"""
    text = payload.get_args().strip()

    connection.execute(
        "ALTER TABLE todo.todo UPDATE status = 'complete' WHERE text = %(text)s",
        {"text": text}
    )

    logging.info('Выполнена задача - %s', text)
    await payload.reply(f'Выполнена задача - "{text}"', parse_mode='Markdown')


if __name__ == '__main__':
    executor.start_polling(dp)
