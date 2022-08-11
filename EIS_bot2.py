import requests
import re
from bs4 import BeautifulSoup
import telebot
from telebot import types
from config import token
import os
from ftplib import FTP
import datetime


def create_dir(directory_name):
    if not os.path.exists(directory_name):
        os.mkdir(directory_name)


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


def get_from_ftp(eisdocno, month, month_date, directory):

    create_dir(directory_name=f'Temp//{eisdocno}_{month}_{month_date}')

    ftp_files = []
    ftp = FTP('ftp.zakupki.gov.ru')
    ftp.login('free', 'free')
    ftp.set_pasv(True)
    ftp.cwd(f'fcs_regions//Tulskaja_obl//contracts//{directory}')
    ftp.dir(ftp_files.append)
    ftp.close()
    print(ftp_files)


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
    kb1.add(*kb1_buttons)
    return kb1


def gen_kb2(month, eisdocno):
    kb2 = types.InlineKeyboardMarkup(row_width=3)
    dates = spider(eisdocno)
    month_dates = set([date[:10] for date in dates if month in date])
    kb2_buttons = []
    for month_date in month_dates:
        kb2_buttons.append(types.InlineKeyboardButton(month_date, callback_data=f'{eisdocno}_{month}_{month_date}'))
    kb2_buttons.append(types.InlineKeyboardButton(f'Всё за {month}', callback_data=f'{eisdocno}_{month}_all'))
    kb2_buttons.append(types.InlineKeyboardButton('Новый поиск', callback_data=f'{eisdocno}'))
    kb2_buttons.append(types.InlineKeyboardButton('Посмотреть периоды (месяцы) публикации', callback_data=f'{eisdocno}_back'))
    kb2.add(*kb2_buttons)

    return kb2


if __name__ == '__main__':

    create_dir('Temp')

    bot = telebot.TeleBot(token=token)

    @bot.message_handler(func=lambda msg: True)
    def start(msg):

        pattern = '\d{19}?'

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

    @bot.callback_query_handler(func=lambda call: True)
    def callbacks(call):

        if call.data.count('_') == 0:
            bot.edit_message_text(text=f'Поиск завершен',
                                  chat_id=call.message.chat.id, message_id=call.message.id)
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
            call_list = call.data.split('_')
            eisdocno, month, month_date = call_list[0], call_list[1], call_list[2]
            # Следующий блок определяет рабочую папку на фтп
            if month_date != 'all':
                directory = dir_choice(datetime.datetime.strptime(month_date, '%d.%m.%Y'))
            else:
                directory = dir_choice(datetime.datetime.strptime(month, '%m.%Y'))

            if directory in ('currMonth', 'prevMonth'):
                get_from_ftp(eisdocno, month, month_date, directory)
            else:
                pass


    while True:
        try:
            bot.polling(none_stop=True, interval=0, timeout=20)
        except Exception as ex:
            print('Exception')
            print('Exception', ex)