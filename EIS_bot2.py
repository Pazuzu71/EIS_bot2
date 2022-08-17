import requests
import re
from bs4 import BeautifulSoup
import telebot
from telebot import types
from config import token
import os
from ftplib import FTP
import datetime
import zipfile
import time
import threading
import sqlite3
from sqlite3 import Error
import schedule


def sql_connection():
    try:
        conn = sqlite3.connect('base.db')
        # print('Соединение открыто')
        return conn
    except Error:
        print(Error)


def conn_close(conn):
    # print('Соединение закрыто')
    return conn.close()


def create_table(conn):
    cur = conn.cursor()
    cur.execute('''create table if not exists contractProcedure
    (
        id         INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
        zipname    TEXT,
        xmlname    TEXT,
        createdate TEXT
    );''')
    conn.commit()


def insert(conn, entities):
    cur = conn.cursor()
    cur.execute('''insert into contractProcedure(zipname, xmlname, createdate) values (?, ?, ?)''', entities)
    conn.commit()
    # print('Записано!')


def selectz_distinct(conn):
    cur = conn.cursor()
    cur.execute('''select distinct(zipname) from contractProcedure''')
    records = cur.fetchall()
    # print('select_records', records)
    return records


def select_like(conn, column1, column2):
    cur = conn.cursor()
    cur.execute('''select zipname from contractProcedure where zipname like ? and xmlname like ?''', ('%' + column1 + '%', '%' + column2 + '%'))
    records = cur.fetchall()
    # print('select_like_records', records)
    return records


def journal_update():
    conn = sql_connection()
    zips_in_base = selectz_distinct(conn)
    conn_close(conn)
    ftp = FTP('ftp.zakupki.gov.ru')
    ftp.login('free', 'free')
    ftp.set_pasv(True)
    files = []
    ftp.cwd(f'fcs_regions//Tulskaja_obl//contracts')
    ftp.dir(files.append)
    for file in files:
        tokens = file.split()
        file_name = tokens[8]
        if file_name not in zips_in_base and file_name not in ('currMonth', 'prevMonth'):

            with open(f'Temp//{file_name}', 'wb') as f:
                ftp.retrbinary('RETR ' + file_name, f.write)
            z = zipfile.ZipFile(f'Temp//{file_name}', 'r')
            for item in z.namelist():
                if item.endswith('.xml') and 'contractProcedure' in item:
                    conn = sql_connection()
                    entities = (file_name, item, datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S'))
                    insert(conn, entities)
                    conn_close(conn)
            z.close()
            os.unlink(f'Temp//{file_name}')
    ftp.close()


def journal_update_start():
    schedule.every().day.at("12:20").do(journal_update)
    while True:
        try:
            schedule.run_pending()
            time.sleep(1)
        except Exception as ex:
            print('Ошибка обновления журнала',ex)


def create_dir(directory_name):
    if not os.path.exists(directory_name):
        os.mkdir(directory_name)


def clean_dir(directory_name):
    for path, dirs, files in os.walk(directory_name):
        if files:
            for file in files:
                os.unlink(os.path.join(path, file))
    for path, dirs, files in os.walk(directory_name):
        if dirs:
            for dir in dirs:
                os.unlink(os.path.join(path, dir))


def dir_choice(month_date):

    date_now = datetime.datetime.now()

    if date_now.year - month_date.year == 0:
        if date_now.month - month_date.month == 0:
            directory = 'currMonth'
        elif date_now.month - month_date.month == 1:
            directory = 'prevMonth'
        else:
            directory = ''
    elif date_now.year - month_date.year == 1:
        if date_now.month == '01' and month_date.month == '12':
            directory = 'prevMonth'
        else:
            directory = ''
    else:
        directory = ''

    return directory


def get_from_ftp(eisdocno, month, month_date, chat_id, message_id, directory):

    directory_name = f'Temp//{eisdocno}_{month}_{month_date}'
    create_dir(directory_name)

    ftp_files = []
    ftp = FTP('ftp.zakupki.gov.ru')
    ftp.login('free', 'free')
    ftp.set_pasv(True)
    ftp.cwd(f'fcs_regions//Tulskaja_obl//contracts//{directory}')
    ftp.dir(ftp_files.append)
    month_date_format = month_date[6:] + month_date[3:5] + month_date[:2]

    for file in ftp_files:
        tokens = file.split()
        file_name = tokens[8]
        file_name_date = file_name.split('_')[3][:8]

        if month_date_format == file_name_date:
            with open(f'{directory_name}//{file_name}', 'wb') as f:
                ftp.retrbinary('RETR ' + file_name, f.write)
            z = zipfile.ZipFile(f'{directory_name}//{file_name}', 'r')
            for item in z.namelist():
                if item.endswith('.xml') and eisdocno in item and 'contractProcedure' in item:
                    z.extract(item, f'{directory_name}')
            z.close()
            time.sleep(1)
            for path, dirs, files in os.walk(f'{directory_name}'):
                if files:
                    for file in files:
                        if file.endswith('.zip'):
                            os.unlink(os.path.join(path, file))
    ftp.close()
    queue.append([chat_id, message_id, directory_name])


def get_from_ftp2(eisdocno, month, month_date, chat_id, message_id):

    month_date_format = month_date[6:] + month_date[3:5] + '01'
    conn = sql_connection()
    journal_files = select_like(conn,
                                f'contract_Tulskaja_obl_{month_date_format}',
                                eisdocno)
    journal_files = list(set([x[0] for x in journal_files]))
    # print(journal_files)
    conn_close(conn)

    directory_name = f'Temp//{eisdocno}_{month}_{month_date}'
    create_dir(directory_name)

    ftp = FTP('ftp.zakupki.gov.ru')
    ftp.login('free', 'free')
    ftp.set_pasv(True)
    ftp.cwd(f'fcs_regions//Tulskaja_obl//contracts')
    month_date_format = f'{month_date[6:]}-{month_date[3:5]}-{month_date[:2]}'
    for journal_file in journal_files:
        with open(f'{directory_name}//{journal_file}', 'wb') as f:
            ftp.retrbinary('RETR ' + journal_file, f.write)
        z = zipfile.ZipFile(f'{directory_name}//{journal_file}', 'r')
        for item in z.namelist():
            if item.endswith('.xml') and eisdocno in item and 'contractProcedure' in item:
                z.extract(item, f'{directory_name}')
        z.close()
        time.sleep(1)
        for path, dirs, files in os.walk(f'{directory_name}'):
            if files:
                for file in files:
                    if file.endswith('.zip'):
                        os.unlink(os.path.join(path, file))
                    else:
                        with open(os.path.join(path, file), 'r', encoding='UTF-8') as f:
                            pattern = r'<publishDate>.*?</publishDate>'
                            publishDate = re.search(pattern, f.read())
                        if month_date_format not in publishDate[0]:
                            os.unlink(os.path.join(path, file))
    ftp.close()
    queue.append([chat_id, message_id, directory_name])


def spider(eisdocno):
    dates = []
    url = f'https://zakupki.gov.ru/epz/contract/contractCard/event-journal.html?reestrNumber={eisdocno}'

    headers = {
        'accept': '*/*',
        'user-agent': 'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36'
    }

    r = requests.get(url=url, headers=headers)

    with open('index.html', 'w', encoding='UTF-8') as f:
        f.write(r.text)
    with open('index.html', encoding='UTF-8') as f:
        src = f.read()

    if re.search('sid:.*?,', src, re.DOTALL):
        sid = re.search('sid:.*?,', src, re.DOTALL)[0].replace("sid: '", "").replace("',", "")

        url = f'https://zakupki.gov.ru/epz/contract/card/event/journal/list.html?sid={sid}&page=1&pageSize=100'

        r = requests.get(url=url, headers=headers)

        with open('index_j.html', 'w', encoding='UTF-8') as f:
            f.write(r.text)
        with open('index_j.html', encoding='UTF-8') as f:
            src = f.read()

        soup = BeautifulSoup(src, 'lxml')

        table_rows = soup.find_all('tr', class_ ="table__row")
        for table_row in table_rows:
            table_cells = table_row.find_all('td', class_="table__cell table__cell-body")
            journal_date = table_cells[0].text.strip()[0:-6]
            journal_event = table_cells[1].text.strip()
            # print(f'{journal_date} - {journal_event}')
            if 'Размещена информация об исполнении (о расторжении) контракта' in journal_event:
                dates.append(journal_date)
    return dates


def gen_kb1(text, dates):

    kb1 = types.InlineKeyboardMarkup(row_width=3)
    months = set(x[3:10] for x in dates)
    kb1_buttons = []
    for month in months:
        kb1_buttons.append(types.InlineKeyboardButton(month, callback_data=f'{text}_{month}'))
    kb1_buttons.append(types.InlineKeyboardButton('Новый поиск', callback_data=f'{text}'))
    kb1.add(*kb1_buttons)
    return kb1


def gen_kb2(month, eisdocno):
    kb2 = types.InlineKeyboardMarkup(row_width=3)
    dates = spider(eisdocno)
    month_dates = set([date[:10] for date in dates if month in date])
    kb2_buttons = []
    for month_date in month_dates:
        kb2_buttons.append(types.InlineKeyboardButton(month_date, callback_data=f'{eisdocno}_{month}_{month_date}'))
    # kb2_buttons.append(types.InlineKeyboardButton(f'Всё за {month}', callback_data=f'{eisdocno}_{month}_all'))
    kb2_buttons.append(types.InlineKeyboardButton('Новый поиск', callback_data=f'{eisdocno}'))
    kb2_buttons.append(types.InlineKeyboardButton('Вернуться к месяцам', callback_data=f'{eisdocno}_back'))
    kb2.add(*kb2_buttons)

    return kb2


if __name__ == '__main__':

    queue = []
    end_search = []
    create_dir('Temp')

    conn = sql_connection()
    create_table(conn)
    conn_close(conn)

    bot = telebot.TeleBot(token=token)


    def send_file():
        while True:
            if queue:
                queue_instance = queue[0]
                chat_id, message_id, directory_name = queue_instance[0], queue_instance[1], queue_instance[2]
                for path, dirs, files in os.walk(f'{directory_name}'):
                    if files:
                        for file in files:
                            file_to_send = open(os.path.join(path, file), 'rb')
                            path_list = path.split('_')
                            eisdocno, month, month_date = path_list[0][6:], path_list[1], path_list[2]
                            bot.send_document(chat_id=chat_id, document=file_to_send, reply_to_message_id=message_id, caption=f'{eisdocno} за {month_date}')
                            file_to_send.close()
                        # clean_dir(directory_name)
                    else:
                        bot.edit_message_text(text=f'Поиск завершен',
                                              chat_id=chat_id, message_id=message_id)
                        bot.send_message(chat_id=chat_id,
                                         text='Что-то пошло не так. Возможно указанный реестровый номер некорректен. '
                                               'Пожалуйста попробуйте еще раз.', reply_to_message_id=message_id-1)
                        bot.send_message(chat_id=chat_id,
                                         text='👇 Введите реестровый номер сведений о контракте 👇 и нажите "Ввод"', reply_to_message_id=message_id-1)
                    del queue[0]
            time.sleep(5)


    @bot.message_handler(func=lambda msg: True)
    def start(msg):

        pattern = '\d{19}'

        if msg.text == '/start':
            bot.send_message(chat_id=msg.chat.id,
                             text='👇 Введите реестровый номер сведений о контракте 👇 и нажите "Ввод"')
        elif msg.text != '/start' and not re.fullmatch(pattern, msg.text):
            bot.reply_to(msg, text='Что-то пошло не так. Возможно указанный реестровый номер некорректен. '
                                   'Пожалуйста попробуйте еще раз.')
            bot.send_message(chat_id=msg.chat.id,
                             text='👇 Введите реестровый номер сведений о контракте 👇 и нажите "Ввод"')
        elif msg.text != '/start' and re.fullmatch(pattern, msg.text):
            dates = spider(eisdocno=msg.text)
            if not dates:
                bot.reply_to(msg, text='Что-то пошло не так. Возможно публикаций сведений об исполнении еще не было. '
                                       'Пожалуйста попробуйте еще раз.')
                bot.send_message(chat_id=msg.chat.id,
                                 text='👇 Введите реестровый номер сведений о контракте 👇 и нажите "Ввод"')
            else:
                kb1 = gen_kb1(msg.text, dates)
                bot.send_message(chat_id=msg.chat.id, text='Сведения об исполнении найдены в следующих периодах (месяцах):',
                                 reply_to_message_id=msg.id, reply_markup=kb1)

                for search in end_search:
                    search_chat_id, search_msg_id = search[0], search[1]
                    if msg.chat.id == search_chat_id:  # and call.message.id != msg_id
                        bot.edit_message_text(text=f'Поиск завершен',
                                              chat_id=search_chat_id, message_id=search_msg_id)
                        search[0], search[1] = 0, 0

                chat_id, msg_id = msg.chat.id, msg.id+1
                end_search.append([chat_id, msg_id])


    @bot.callback_query_handler(func=lambda call: True)
    def callbacks(call):

        if call.data.count('_') == 0:
            for search in end_search:
                search_chat_id, search_msg_id = search[0], search[1]
                if call.message.chat.id == search_chat_id: #and call.message.id != msg_id
                    bot.edit_message_text(text=f'Поиск завершен',
                                  chat_id=search_chat_id, message_id=search_msg_id)
                    search[0], search[1] = 0, 0

            bot.send_message(chat_id=call.message.chat.id,
                             text='👇 Введите реестровый номер сведений о контракте 👇 и нажите "Ввод"')

        if call.data.count('_') == 1:
            eisdocno = call.data[:19]
            month = call.data[20:]
            if month == 'back':
                dates = spider(eisdocno)
                kb1 = gen_kb1(eisdocno, dates)
                bot.edit_message_text(text=f'Сведения об исполнении найдены в следующих периодах (месяцах):',
                                      chat_id=call.message.chat.id, message_id=call.message.id, reply_markup=kb1)
            else:
                kb2 = gen_kb2(month, eisdocno)
                bot.edit_message_text(text=f'Укажите дату из выбранного периода {month}. '
                                           f'Чтобы выбрать период заново, нажмите "Посмотреть периоды (месяцы) публикации"',
                                      chat_id=call.message.chat.id, message_id=call.message.id, reply_markup=kb2)

        if call.data.count('_') == 2:
            chat_id, message_id = call.message.chat.id, call.message.id
            call_list = call.data.split('_')
            eisdocno, month, month_date = call_list[0], call_list[1], call_list[2]
            # Следующий блок определяет рабочую папку на фтп
            if month_date != 'all':
                directory = dir_choice(datetime.datetime.strptime(month_date, '%d.%m.%Y'))
            else:
                directory = dir_choice(datetime.datetime.strptime(month, '%m.%Y'))

            if directory in ('currMonth', 'prevMonth'):
                get_from_ftp(eisdocno, month, month_date, chat_id, message_id, directory)
                # print(queue)
            else:
                get_from_ftp2(eisdocno, month, month_date, chat_id, message_id)

    thr1 = threading.Thread(target=send_file)
    thr1.start()
    thr2 = threading.Thread(target=journal_update_start)
    thr2.start()


    while True:
        try:
            bot.polling(none_stop=True, interval=0, timeout=20)
        except Exception as ex:
            print('Exception')
            print('Exception', ex)