import pandas as pd
import warnings
from datetime import datetime
import time
import requests
from bs4 import BeautifulSoup

warnings.simplefilter('ignore')


class India_get_data:
    """
    Класс реализующий парсинг данных импорта и экспорта Индии.
    :param type_flow - тип операции, импорт или экспорт
    :param url_flow - url на страницу с данными
    :param year - год, по которому идет сбор данных
    :param month_number - словарь с порядковыми номерами месяцев
    :param key_unit - параметр отвечающий за выбор денежного или весового эквивалента
    """

    def __init__(self, type_flow, url_flow, year, month_number, key_unit=None):
        self.type_flow = type_flow
        self.url_flow = url_flow
        self.year = year
        self.key_unit = key_unit
        self.month_number = month_number

    def get_country_option(self):
        """
        :return - возвращает словарь {'Значение опции страны на сайте': 'значение страны'}
        """
        r_country = requests.get(self.url_flow)
        res_dct_country = {}
        soup = BeautifulSoup(r_country.text, "lxml")
        for op in soup.findAll("select", {"name": "cntcode"})[0].findAll("option"):
            res_dct_country[op['value']] = op.text.strip()
        return res_dct_country

    def get_month_option(self):
        """
        :return - возвращает словарь {'Значение опции месяца на сайте': 'значение месяца'}
        """
        r_month = requests.get(self.url_flow)
        res_dct_month = {}
        soup = BeautifulSoup(r_month.text, "lxml")
        for op in soup.findAll("select", {"name": "Mm1"})[0].findAll("option"):
            res_dct_month[op['value']] = op.text.replace('\r\n', '')
        return res_dct_month

    def get_response(self, month_option, country_option, flag_write=True):
        """
        :param month_option - опциональное значение месяца(для заполнения и отправки формы)
        :param country_option - опциональное значение страны(для заполнения и отправки формы)
        :param flag_write - флаг проверки возвращаемого кода запроса response_unit.status_code
        :return - датафрейм
        """

        # Ссылка для выполнения POST запроса
        url = f'https://tradestat.commerce.gov.in/meidb/cntcom.asp?ie={self.type_flow[0]}'

        # Словарь параметров для заполнения формы запроса
        dct_data_cfg = {
            'radioCY': '1',
            'Mm1': month_option,
            'yy1': self.year,
            'cntcode': str(country_option),
            'hslevel': '8',
            'sort': '0',
            'radioDAll': '1',
            self.key_unit: 1
        }

        # Блок проверки статуса запроса
        while flag_write:
            try:
                # время задержки
                time.sleep(1)
                response_unit = requests.post(url, data=dct_data_cfg)
                if response_unit.status_code == 200:
                    flag_write = False
                else:
                    print(f'Статус запроса {response_unit.status_code}')
            except:
                print(f'Получили ошибку')
                time.sleep(37)


        # Обрабатываем полученный ответ
        soup_df = BeautifulSoup(response_unit.text, "lxml")
        all_h2 = soup_df.findAll('h2')

        # Если присутствует искомое предложение, возвращаем пустой датафрейм
        for h in all_h2:
            if h.text == 'No record found!':
                return pd.DataFrame()
        temp_soup = soup_df.findAll('table')[0]
        response_df = pd.read_html(temp_soup.prettify())[0]
        return response_df




    def build_df_value(self, month_option, month_value, country_option, country_value):
        """
        Функция скачивания и преобразовния данных в формате стоимостного выражения
        :param month_option - опциональное значение месяца(для заполнения и отправки формы)
        :param month_value - название месяца
        :param country_option - опциональное значение страны(для заполнения и отправки формы)
        :param country_value - название страны
        :return - датафрейм
        """

        # Функция переименовывания столбцов
        def rename_columns(x):
            return x.split()[1] if '20' in x else x

        # Получаем датафрейм
        flag_index_error = True
        while flag_index_error:
            try:
                temp_df = self.get_response(month_option, country_option)
                flag_index_error = False
            except IndexError:
                print('Нет таблицы!!!!')
                time.sleep(1080)
                flag_index_error = True

        # Проверка, не пустой ли он
        if temp_df.empty:
            return pd.DataFrame()

        # Преобразуем данные
        temp_df = temp_df.iloc[:, 1:5]
        temp_df.rename(columns=rename_columns, inplace=True)
        temp_df = temp_df.melt(id_vars=['HSCode', 'Commodity'])
        temp_df.rename(columns={'HSCode': 'commodity_code', 'value': 'trade_value', 'variable': 'year'}, inplace=True)
        temp_df.dropna(subset=['commodity_code'], inplace=True)
        # Дописываем код, если он меньше 8 знаков
        temp_df['commodity_code'] = temp_df['commodity_code'].apply(
            lambda x: '0' + str(int(x)) if len(str(int(x))) < 8 else str(int(x)))
        temp_df['classification'] = 'HS'
        temp_df['period'] = temp_df.year.apply(lambda x:
                                               datetime.strptime(
                                                   f'01-{self.month_number[month_value] if len(self.month_number[month_value]) > 1 else "0" + self.month_number[month_value]}-'
                                                   + x, '%d-%m-%Y'))
        temp_df['trade_value'] = temp_df['trade_value'].apply(lambda x: x * 1000000)
        temp_df['reporter_code'] = 699
        temp_df['name_country_source'] = country_value.strip()
        temp_df['aggregate_level'] = 8
        temp_df['trade_flow_code'] = 1 if self.type_flow == 'import' else 2
        temp_df['customs_proc_code'] = 'C00'
        temp_df['flag'] = 0
        temp_df['plus'] = 0
        temp_df['load_mark'] = 1
        temp_df['update_date'] = datetime.now().strftime('%Y-%m-%d')
        return temp_df

    def build_df_qty(self, month_option, country_option, country_value):
        """
        Функция скачивания и преобразовния данных в формате веса(единицы измерения)
        :param month_option - опциональное значение месяца(для заполнения и отправки формы)
        :param country_option - опциональное значение страны(для заполнения и отправки формы)
        :param country_value - название страны
        :return - датафрейм
        """

        # Функция переименовывания столбцов
        def rename_columns(x):
            return x.split()[1] if '20' in x else x

        # Получаем датафрейм
        flag_index_error = True
        while flag_index_error:
            try:
                temp_df = self.get_response(month_option, country_option)
                flag_index_error = False
            except IndexError:
                print('Нет таблицы!!!!')
                time.sleep(1080)
                flag_index_error = True

        # Проверка, не пустой ли он
        if temp_df.empty:
            return pd.DataFrame()

        # Преобразуем данные
        temp_df = temp_df.iloc[:, 1:6]
        temp_df.drop(columns=['Commodity'], inplace=True)
        temp_df.rename(columns=rename_columns, inplace=True)
        temp_df = temp_df.melt(id_vars=['HSCode', 'Unit'])
        temp_df.rename(columns={'HSCode': 'commodity_code', 'value': 'qty', 'variable': 'year'}, inplace=True)
        temp_df.dropna(subset=['commodity_code'], inplace=True)
        # Дописываем код, если он меньше 8 знаков
        temp_df['commodity_code'] = temp_df['commodity_code'].apply(
            lambda x: '0' + str(int(x)) if len(str(int(x))) < 8 else str(int(x)))
        temp_df['name_country_source'] = country_value.strip()

        return temp_df
