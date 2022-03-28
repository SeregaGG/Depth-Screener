import os
import time
from queue import Queue
from binance import Client
import telebot
import pandas as pd
import datetime
import threading
import psycopg2
from db_settings import settings
from loguru import logger

class ReportBot:
    __UTC_diff = 5
    __pair = 'ALGOUSDT'
    __kline_interval = '5m'
    __avg_volume_interval = 1
    __stop_word = False
    __stop_q = Queue()

    def __init__(self, binance_keys: dict, bot_api: str, is_daemon=False):
        self.__stop_q.put(False)
        self.__stop_q.put(True)
        logger.add('logs/logs.log', format='{time} {level} {message}')

        self.client = Client(binance_keys["api"], binance_keys["secret"])
        self.__bot = telebot.TeleBot(bot_api)
        self.__task = threading.Thread(target=self.send_info, args=(self.__stop_q,), daemon=True)
        self.__task.start()
        self.bot_commands_init()
        if is_daemon:
            self.__task_polling = threading.Thread(target=self.start_daemon_polling, daemon=True)
            self.__task_polling.start()
        else:
            self.start_daemon_polling()

    def start_daemon_polling(self):
        self.__bot.polling()

    @property
    def pair(self):
        return self.__pair

    @pair.setter
    def pair(self, pair: str):
        self.__pair = pair

    @property
    def interval(self):
        return self.__kline_interval

    @interval.setter
    def interval(self, kline_interval):
        self.__kline_interval = kline_interval

    @logger.catch
    def check_db_connection(self):
        is_up = False
        while not is_up:
            try:
                connection = psycopg2.connect(user=settings['user'],
                                              password=settings['password'],
                                              port=settings['port'],
                                              host=settings['host'],
                                              database=settings['database'])
                connection.close()

            except Exception as err:
                logger.error(f"Connection error {err}")
            else:
                is_up = True

        logger.info('DataBase is OK!')

    def send_info(self, in_q: Queue):
        self.check_db_connection()
        while not self.__stop_q.queue[0]:
            connection = psycopg2.connect(user=settings['user'],
                                          password=settings['password'],
                                          host=settings['host'],
                                          port=settings['port'],
                                          database=settings['database'])
            cursor = connection.cursor()
            select_q = f"SELECT * FROM bot_user"
            cursor.execute(select_q)

            for bot_user in cursor.fetchall():
                if bot_user[2]:
                    self.sub_info(bot_user[1])
            cursor.close()
            connection.close()
            time.sleep(60 * 5)  # 5 min

    def sub_info(self, sub_chat_id):
        order_book = self.client.get_order_book(symbol=self.__pair)
        last_day = datetime.datetime.now() - datetime.timedelta(days=1, hours=self.__UTC_diff)
        avg_last_day = self.client.get_historical_klines(self.__pair, self.__kline_interval,
                                                         start_str=last_day.strftime("%d-%b-%Y (%H:%M:%S.%f)"))
        df = pd.DataFrame(avg_last_day,
                          columns=['dateTime', 'open', 'high', 'low', 'close', 'volume', 'closeTime',
                                   'quoteAssetVolume', 'numberOfTrades', 'takerBuyBaseVol',
                                   'takerBuyQuoteVol', 'ignore'],
                          dtype="float64")
        current_avg_vol = self.__avg_volume(df)
        found_bids, found_asks = self.__get_big_orders(order_book, current_avg_vol)

        self.__bot.send_message(sub_chat_id, 'Pair is ' + self.__pair)
        self.__bot.send_message(sub_chat_id, 'Avg volume is ' + current_avg_vol.__str__())

        if not len(found_bids):
            self.__bot.send_message(sub_chat_id, 'Bids is empty')

        if not len(found_asks):
            self.__bot.send_message(sub_chat_id, 'Asks is empty')

        for i in range(len(found_bids)):
            self.__bot.send_message(sub_chat_id, 'Bid price ' + found_bids[i][0]
                                    + '\nVolume ' + found_bids[i][1])

        for i in range(len(found_asks)):
            self.__bot.send_message(sub_chat_id, 'Ask price ' + found_asks[i][0]
                                    + '\nVolume ' + found_asks[i][1])

        print("Information: ", found_bids)

    def __avg_volume(self, dataframe: pd.DataFrame):  # pd.Series
        return dataframe['volume'].sum() / dataframe['volume'].count()

    def __get_big_orders(self, order_book: dict, current_avg_vol):
        bids_orders = []
        asks_orders = []
        volume_pos = 1

        for order in order_book['bids']:
            if float(order[volume_pos]) > current_avg_vol:
                bids_orders.append(order)

        for order in order_book['asks']:
            if float(order[volume_pos]) > current_avg_vol:
                asks_orders.append(order)

        return bids_orders, asks_orders

    def bot_commands_init(self):
        @self.__bot.message_handler(commands=['start'])
        def put_user(message: telebot.types.Message):
            # self.__users[message.chat.id] = True
            connection = psycopg2.connect(user=settings['user'],
                                          password=settings['password'],
                                          host=settings['host'],
                                          port=settings['port'],
                                          database=settings['database'])
            cursor = connection.cursor()
            select_q = f"SELECT * FROM bot_user WHERE chat_id = {message.chat.id}"
            cursor.execute(select_q)
            if cursor.fetchone() is None:
                insert_q = """ INSERT INTO bot_user (chat_id, is_sub) VALUES (%s, %s)"""
                values_tuple = (message.chat.id, True)
                cursor.execute(insert_q, values_tuple)
                connection.commit()
                print("New user added")

            cursor.close()
            connection.close()

            markup = telebot.types.ReplyKeyboardMarkup(resize_keyboard=True)
            bot_info_btn = telebot.types.KeyboardButton("About bot.")
            commands_info_btn = telebot.types.KeyboardButton("About commands.")
            markup.add(bot_info_btn, commands_info_btn)
            self.__bot.send_message(message.chat.id, "Hello!", reply_markup=markup)

        @self.__bot.message_handler(commands=['info'])
        def current_info(message):
            self.sub_info(message.chat.id)

        @self.__bot.message_handler(commands=['sub'])
        def subscribe(message):
            connection = psycopg2.connect(user=settings['user'],
                                          password=settings['password'],
                                          host=settings['host'],
                                          port=settings['port'],
                                          database=settings['database'])
            cursor = connection.cursor()
            select_q = f"SELECT * FROM bot_user WHERE chat_id = {message.chat.id}"
            cursor.execute(select_q)
            if cursor.fetchone() is not None:
                update_q = f""" UPDATE bot_user set is_sub = True where chat_id = {message.chat.id}"""
                cursor.execute(update_q)
                connection.commit()
                print(f"New subscriber added - {message.chat.id}")

            cursor.close()
            connection.close()

        @self.__bot.message_handler(commands=['stop'])
        def stop_info(message):
            # self.__users[message.chat.id] = False
            connection = psycopg2.connect(user=settings['user'],
                                          password=settings['password'],
                                          host=settings['host'],
                                          port=settings['port'],
                                          database=settings['database'])
            cursor = connection.cursor()
            select_q = f"SELECT * FROM bot_user WHERE chat_id = {message.chat.id}"
            cursor.execute(select_q)
            if cursor.fetchone() is not None:
                update_q = f""" UPDATE bot_user set is_sub = False where chat_id = {message.chat.id}"""
                cursor.execute(update_q)
                connection.commit()
                print(f"Sub is stopped by {message.chat.id}")

            cursor.close()
            connection.close()

        @self.__bot.message_handler(content_types=['text'])
        def general_info(message):
            if message.text == "About bot.":
                self.__bot.send_message(message.chat.id, text="Данный бот сканирует стакан ордеров "
                                                              + "на наличие крупных заявок."
                                                              + "\nВ данном случае, заявка считается крупной если она "
                                                                "больше среднего объема (за день) в пятиминутной свече."
                                                              + f"\nВалютная пара - {self.__pair}."
                                                              + "\nИнформация берется с биржи Binance")
            if message.text == "About commands.":
                self.__bot.send_message(message.chat.id, text="/start - подписывает вас на рассылку с интервалом в 5м. "
                                                              + "/info - сообщает текущее состояние стакана. "
                                                              + "/stop - отменяет рассылку."
                                                              + "/sub - подписаться самостоятельно."
                                                              + "\nИнформация берется с биржи Binance")

    def __del__(self):
        self.__stop_q.get()
        self.__task.join()
        self.__bot.stop_bot()
        self.__task_polling.join()
        self.client.close_connection()
