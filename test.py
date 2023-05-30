import logging
import os
from tabulate import tabulate

import pandas as pd
from aiogram import Bot, Dispatcher, executor, types

logging.basicConfig(
    format="%(levelname)s; %(asctime)s - %(message)s",
    datefmt="%d-%b-%y %H-%M-%S",
    level=logging.INFO
)

APP_TOKEN = os.environ.get('TOKEN')


PATH_TO_TABLE = 'todo_list.csv'

bot = Bot(token=APP_TOKEN)
dp = Dispatcher(bot)


def get_todo_data():
    return pd.read_csv(PATH_TO_TABLE)


@dp.message_handler(commands='all')
async def all_tasks(payload: types.Message):
    await payload.reply(f"```{tabulate(get_todo_data(), showindex=False, headers='keys', tablefmt='pipe', stralign='center')}```", parse_mode='Markdown'
    )


@dp.message_handler(commands='add')
async def add_task(payload: types.Message):
    text = payload.get_args().strip()
    new_task = pd.DataFrame({'task': [text], 'status': ['active']})
    updated_task = pd.concat([get_todo_data(), new_task], ignore_index=True, axis=0)

    updated_task.to_csv(PATH_TO_TABLE, index=False)

    logging.info(f'Добавлена задача - {text}')
    await payload.reply(f'Добавлена задача - "{text}"', parse_mode='Markdown')


@dp.message_handler(commands='done')
async def complete_task(payload: types.Message):
    text = payload.get_args().strip()
    df = get_todo_data()
    df.loc[df.task == text, 'status'] = 'complete'

    df.to_csv(PATH_TO_TABLE, index=False)

    logging.info(f'Выполнена задача - {text}')
    await payload.reply(f'Выполнена задача - "{text}"', parse_mode='Markdown')

if __name__ == '__main__':
    executor.start_polling(dp)