import time
from queue import Queue
from binance import Client
import telebot
import pandas as pd
import datetime
import threading


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

        self.client = Client(binance_keys["api"], binance_keys["secret"])
        self.__bot = telebot.TeleBot(bot_api)
        self.__users = dict()
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

    def send_info(self, in_q: Queue):
        while not self.__stop_q.queue[0]:
            for chat_id in self.__users:
                if self.__users[chat_id]:
                    self.sub_info(chat_id)
            print(self.__users)
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

        print("bids: ", found_bids)

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
        def put_user(message):
            self.__users[message.chat.id] = True
            markup = telebot.types.ReplyKeyboardMarkup(resize_keyboard=True)
            bot_info_btn = telebot.types.KeyboardButton("About bot.")
            commands_info_btn = telebot.types.KeyboardButton("About commands.")
            markup.add(bot_info_btn, commands_info_btn)
            self.__bot.send_message(message.chat.id, "Hello!", reply_markup=markup)
            print("add user")

        @self.__bot.message_handler(commands=['info'])
        def current_info(message):
            self.sub_info(message.chat.id)

        @self.__bot.message_handler(commands=['stop'])
        def stop_info(message):
            self.__users[message.chat.id] = False

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
                self.__bot.send_message(message.chat.id, text="/start - подписывает вас на рассылку с интервалом в 5м."
                                                              + "/info - сообщает текущее состояние стакана."
                                                              + "/stop - отменяет рассылку."
                                                              + "\nИнформация берется с биржи Binance")

    def __del__(self):
        self.__stop_q.get()
        self.__task.join()
        self.__bot.stop_bot()
        self.__task_polling.join()
        self.client.close_connection()
