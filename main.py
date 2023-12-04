import asyncio
import io
import zipfile
import datetime
import logging
import json
import os
import math
import sys
from aiogram import Bot, Dispatcher, executor, types
from aiogram.contrib.fsm_storage.files import JSONStorage
from aiogram.dispatcher.middlewares import BaseMiddleware
from aiogram.dispatcher.handler import CancelHandler, current_handler
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.dispatcher import FSMContext
from enum import Enum,auto

BOT_TOKEN="TOKEN"

ALLOWED_USERS=[ID] # IDS of allowed tasks

DATE_FORMATE='%A %Y/%m/%d %X'

bot = Bot(token=BOT_TOKEN,protect_content=True,parse_mode='html')
dp = Dispatcher(bot, storage=JSONStorage('.fsm_storage.json'))

logging.basicConfig(level=logging.INFO)


class JsonDatabase:
    class FileLock:
        def __init__(self):
            self.lock = asyncio.Lock()

        async def __aenter__(self):
            await self.lock.acquire()

        async def __aexit__(self, exc_type, exc_val, exc_tb):
            self.lock.release()

    def __init__(self, filename):
        self.filename = filename

    async def update(self, value):
        async with self.FileLock():
            data = await self._load()
            data['tasks'] = value
            await self._save(data)

    async def get(self):
        data = await self._load()
        return data.get('tasks')

    async def delete(self, id):
        async with self.FileLock():
            data = await self._load()
            if str(id) in data['tasks']:
                del data['tasks'][str(id)]
            await self._save(data)

    async def _load(self):
        loop = asyncio.get_event_loop()
        try:
            with open(self.filename, "r") as f:
                return await loop.run_in_executor(None, json.load, f)
        except FileNotFoundError:
            return await self._save({
                'tasks':{}
            })

    async def _save(self, data):
        loop = asyncio.get_event_loop()
        with open(self.filename, "w+") as f:
            await loop.run_in_executor(None, json.dump, data, f)
        return data

class Compressor():

    MAX_SIZE = 49 * 1024 * 1024  # 49 MB

    def __init__(self,compress_level=9):
        self.compress_level=compress_level
    
    def compress(self,path: str):
        path=os.path.normpath(path)
        if (os.path.isfile(path)):
            compressed_file=self._compress_file(path)
            size=sys.getsizeof(compressed_file)
            if (size>self.MAX_SIZE):
                
                return self._cut_large_file(compressed_file,os.path.basename(path)+'.zip')
            else:
                return [{'name':os.path.basename(path)+'.zip','data':compressed_file}]
        else:
            compressed_folder=io.BytesIO()
            with zipfile.ZipFile(compressed_folder, 'w', zipfile.ZIP_DEFLATED,compresslevel=self.compress_level) as archive:
                for root, dirs, files in os.walk(path):
                    for file in files:
                        file_path = os.path.join(root, file)
                        try:
                            archive.write(file_path)
                        except FileNotFoundError:
                            logging.warning(f'Не удалось прочитать файл для бэкапа,файл {file_path} перестал существовать!')
            compressed_folder.seek(0)
            size=sys.getsizeof(compressed_folder)
            if (size>self.MAX_SIZE):
                return self._cut_large_file(compressed_folder,os.path.basename(path)+'.zip')
            else:
                return [{'name':os.path.basename(path)+'.zip','data':compressed_folder}]
    
    def _compress_file(self,path: str):
        compressed_file=io.BytesIO()
        with zipfile.ZipFile(compressed_file, 'w', zipfile.ZIP_DEFLATED,compresslevel=self.compress_level) as archive:
            archive.write(path)
        compressed_file.seek(0)
        return compressed_file

    def _cut_large_file(self,file: io.BytesIO,file_name: str):
        
        file_size=sys.getsizeof(file)
        num_chunks = math.ceil(file_size / self.MAX_SIZE)

        compressed_data_list = []
        for chunk_num in range(num_chunks):
            offset = chunk_num * self.MAX_SIZE
            file.seek(offset)
            compressed_data_list.append({'name':f'{file_name}.part{chunk_num}','data':io.BytesIO(file.read(self.MAX_SIZE))})

        return compressed_data_list


db=JsonDatabase('db.json')

class UserStates(StatesGroup):
    MENU=State()
    CREATING_TASK=State()


class KeyboardCallbackData(Enum):
    CREATE_TASK='Создать задание'

class InlineCallbackData(Enum):
    BACKUP_LIST ='backup_list'
    DELETE_TASK='delete_task_'
    EDIT_TASK='edit_task_'
    

class AllowedUsersMiddleware(BaseMiddleware):
    def __init__(self):
        super(AllowedUsersMiddleware, self).__init__()
    async def on_process_message(self, message: types.Message, data: dict):
        if not (message.from_user.id in ALLOWED_USERS):
            logging.warning(f'Неизвестный пользователь {"@"+message.from_user.username+"#"+str(message.from_user.id) if message.from_user.username else "#"+str(message.from_user.id)} попробовал использовать меня')
            await message.reply('<b>Я тебя не знаю...</b>')
            raise CancelHandler()

@dp.message_handler(commands=['start'],state='*')
async def start_message(message: types.Message,state:FSMContext=None):
    if (state!=None):
        await state.reset_state(with_data=True)
    await UserStates.MENU.set()

    backup_list_button=types.InlineKeyboardButton('Список бекап-целей', callback_data=InlineCallbackData.BACKUP_LIST.value)
    await message.reply('Привет,я ваш личный бекапер',reply_markup=types.ReplyKeyboardRemove())
    await bot.send_message(message.from_id,'<i>Меню</i>',reply_markup=types.InlineKeyboardMarkup().add(backup_list_button))
    

@dp.callback_query_handler(lambda c: c.data == str(InlineCallbackData.BACKUP_LIST.value),state=UserStates.MENU)
async def process_callback_backup_list(callback_query: types.CallbackQuery):
    
    tasks=(await db.get())
    if (len(tasks)==0):
        keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True)
        button = types.KeyboardButton(KeyboardCallbackData.CREATE_TASK.value)
        keyboard.add(button)
        await bot.send_message(callback_query.from_user.id,"У вас нет заданий!\nСоздайте их с помощью кнопки снизу",reply_markup=keyboard)
        return
    
    keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True)
    button = types.KeyboardButton(KeyboardCallbackData.CREATE_TASK.value)
    keyboard.add(button)
    await bot.send_message(callback_query.from_user.id,'<b><i>Задачи</i></b>',reply_markup=keyboard)
    for id in tasks.keys():
        """
        task=id:{path: string,sheduledTo: string date(time),delay:number}
        """
        text=f'\n Задача #{int(id)+1}\n Путь:<code>{os.path.normpath(tasks[id]["path"])}</code>\n\n Время следущего бэкапа: <i>{tasks[id]["sheduledTo"]}</i>'
        markup=types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton(f'Удалить задание #{int(id)+1}',callback_data=InlineCallbackData.DELETE_TASK.value+id))
        markup.add(types.InlineKeyboardButton(f'Изменить задание #{int(id)+1}',callback_data=InlineCallbackData.EDIT_TASK.value+id))
        await bot.send_message(callback_query.from_user.id,text,reply_markup=markup)
    
@dp.callback_query_handler(lambda c: c.data.startswith(InlineCallbackData.DELETE_TASK.value),state=UserStates.MENU)
async def handle_delete_task_callback(callback_query: types.CallbackQuery):
    tasks=(await db.get())
    try:
        await db.delete(callback_query.data.replace(InlineCallbackData.DELETE_TASK.value,''))
        await bot.delete_message(callback_query.from_user.id,callback_query.message.message_id)
    except Exception as error:
        logging.warning('Неизвестная ошибка при удалении',exc_info=error)
    
@dp.message_handler(text=KeyboardCallbackData.CREATE_TASK.value,state=UserStates.MENU)
async def create_task(message: types.Message, state: FSMContext):
    logging.info(f'Пользователь {"@"+message.from_user.username+"#"+str(message.from_user.id) if message.from_user.username else "#"+str(message.from_user.id)} начал создавать задание')
    await state.reset_state(True)
    await UserStates.CREATING_TASK.set()
    await bot.send_message(message.from_id,'Отправьте путь до файла/папки',reply_markup=types.ReplyKeyboardRemove())

@dp.message_handler(state=UserStates.CREATING_TASK)
async def task_handler(message: types.Message, state: FSMContext):
    async with state.proxy() as data:
        if (data.get('path')==None):
            try:
                path=os.path.normpath(message.text)
                if not os.path.exists(path):
                    await message.reply('Такой папки не существует!')
                    return await start_message(message)
                if os.path.isdir(path):
                    for dirpath, dirnames, filenames in os.walk(path):
                        for dirname in dirnames:
                            full_path = os.path.join(dirpath, dirname)
                            if not os.access(full_path, os.R_OK):
                                await message.reply('У меня нет доступа к этой папке!')
                                return await start_message(message)
                        for filename in filenames:
                            full_path = os.path.join(dirpath, filename)
                            if not os.access(full_path, os.R_OK):
                                await message.reply('У меня нет доступа к файлам этой папки!')
                                return await start_message(message)
                else:
                    if not os.access(path, os.R_OK):
                        await message.reply('У меня нет доступа к этому файлу!')
                        return await start_message(message)
                data['path'] = path
                await bot.send_message(message.from_id,'Теперь введите частоту создания бэкапа в часах')
            except Exception as error:
                logging.error("Неизвестная ошибка произошла!",exc_info=error)
                await message.reply('Неизвестная ошибка при оброботке вашего сообщения')
                return await start_message(message)
        elif (data.get('sheduledTo')==None):
            if (message.text.isdigit() and int(message.text)>0):
                tasks=(await db.get())
                id= int(list(tasks.keys())[-1])+1 if len(list(tasks.keys()))>0 else 0
                now=datetime.datetime.now()
                tasks[str(id)]={
                    'path':data['path'],
                    'sheduledTo':(now+datetime.timedelta(hours=int(message.text))).strftime(DATE_FORMATE),
                    'delay':int(message.text)
                }

                await bot.send_message(message.from_id,'Отправляю тестовый бэкап...')
                try:
                    files=Compressor().compress(data['path'])
                    
                    caption=f'<code>Тестовый бэкап {os.path.basename(data["path"])}</code>'
                    file_message=await bot.send_document(message.from_id,document=types.InputFile(files[0]['data'], filename=files[0]['name']),caption=caption)
                    for file in files[1:]:
                        file_message=await bot.send_document(message.from_id,document=types.InputFile(file['data'], filename=file['name']),caption=caption,reply_to_message_id=file_message.message_id)
                    
                    # try:
                    # await bot.send_media_group(message.from_id,media=media_group)
                    
                except Exception as error:
                    logging.error('Возникла неизвестная ошибка при попытке тестовой отправки бэкапа',exc_info=error)
                    await bot.send_message(message.from_id,'Я не могу архивировать и отправить файл/папку')
                    return await start_message(message)
                await db.update(tasks)
                logging.info(f'Пользователь {"@"+message.from_user.username+"#"+str(message.from_user.id) if message.from_user.username else "#"+str(message.from_user.id)} создал задание')
                return await start_message(message)
            else:
                await message.reply('Вы ввели не правильное значение!')
            
async def send_backups():
    while True:
        tasks=await db.get()
        for task_id in tasks.keys():
                if (datetime.datetime.now()>datetime.datetime.strptime(tasks[task_id]['sheduledTo'],DATE_FORMATE)):
                    path=os.path.normpath(tasks[task_id]['path'])
                    if os.path.exists(path):
                        compressor=Compressor()
                        try:
                            files=compressor.compress(path)
                            now=datetime.datetime.now()
                            caption=f'<code>Бэкап файла/папки {os.path.basename(path)}\n Сделан в: {now.strftime(DATE_FORMATE)}</code>'
                            for user in ALLOWED_USERS:
                                file_message=await bot.send_document(user,document=types.InputFile(io.BytesIO(files[0]['data'].getvalue()), filename=files[0]['name']),caption=caption)
                                for file in files[1:]:
                                    file_message=await bot.send_document(user,document=types.InputFile(io.BytesIO(file['data'].getvalue()), filename=file['name']),caption=caption,reply_to_message_id=file_message.message_id)
                                logging.info(f'Отправляю бэкап {path} пользователю {user}')
                            for file in files: file['data'].close()
                            tasks[task_id]['sheduledTo']=(now+datetime.timedelta(hours=tasks[task_id]['delay'])).strftime(DATE_FORMATE)
                            await db.update(tasks)
                        except Exception as error:
                            logging.error('Возникла неизвестная ошибка при попытке отправки бэкапа',exc_info=error)
                    else:
                        pass
        await asyncio.sleep(60*57)

async def on_startup(dp: Dispatcher):
    await bot.set_my_commands([types.BotCommand('start','Перезапускает/запускает бота')])
    asyncio.create_task(send_backups())

if __name__ == '__main__':
    dp.middleware.setup(AllowedUsersMiddleware())
    executor.start_polling(dp,allowed_updates=types.AllowedUpdates.MESSAGE+types.AllowedUpdates.INLINE_QUERY+types.AllowedUpdates.CALLBACK_QUERY,on_startup=on_startup)
