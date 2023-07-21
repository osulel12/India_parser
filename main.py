from datetime import datetime
from parser_india import India_get_data
import json
import pandas as pd

# Ссылки на данные импорта и экспорта
URL_IMPORT = 'https://tradestat.commerce.gov.in/meidb/cntcomq.asp?ie=i'
URL_EXPORT = 'https://tradestat.commerce.gov.in/meidb/cntcomq.asp?ie=e'

# Словарь с порядковым номером месяца
with open('month_number.json', 'r') as fl:
    d = json.load(fl)

# Словарь с конфигурацией запроса
# 'export', 'import'  'radioqty' 'radiousd'
dict_config = {
    'type_flow': 'export',
    'url_flow': URL_EXPORT,
    'year': 2023,
    'month_number': d,
    'key_unit_value': 'radiousd',
    'key_unit_qty': 'radioqty'
}

# Внести только те названия, данные по которым нужно получить
# Пример со страной: 'ALGERIA'
# Пример с месяцем: 'FEB'
needed_country_list = []
need_month_list = []


# Валидация пропущенных значений
def bol(x, y):
    if x == 0 and y == 0:
        return 'full_none'
    else:
        return 'not_none'


# Создаем экземпляр класса
need_entries = India_get_data(dict_config['type_flow'], dict_config['url_flow'], dict_config['year'], d)

# Получаем нужные страны и месяцы
lst_country = {k: v for k, v in need_entries.get_country_option().items() if v in needed_country_list} \
    if needed_country_list else need_entries.get_country_option()
lst_month = {k: v for k, v in need_entries.get_month_option().items() if v in need_month_list} \
    if need_month_list else need_entries.get_month_option()

# Заготовка под итоговый датасет
df_void = pd.DataFrame()

# Обходим все страны и месяцы
for country_option, country_value in lst_country.items():
    start = datetime.now()
    print(f'Загружаем {country_value}')
    for month_option, month_value in lst_month.items():
        print(f'Месяц {month_value}')
        # Формируем датафрейм для trade_value
        df_value = India_get_data(dict_config['type_flow'], dict_config['url_flow'], dict_config['year'],
                                  dict_config['month_number'], key_unit=dict_config['key_unit_value']).build_df_value(
                                  month_option, month_value, country_option, country_value)
        # Формируем датафрейм для qty
        df_qty = India_get_data(dict_config['type_flow'], dict_config['url_flow'], dict_config['year'],
                                dict_config['month_number'], key_unit=dict_config['key_unit_qty']).build_df_qty(
                                month_option, country_option, country_value)

        # Собираем полученные датафреймы в один
        try:
            merge_ff = df_value.merge(df_qty, on=['commodity_code', 'year', 'name_country_source'])
            merge_ff.fillna(0, inplace=True)
            merge_ff['bool_border'] = merge_ff.apply(lambda x: bol(x.trade_value, x.qty), axis=1)
            merge_ff = merge_ff.query('bool_border != "full_none"')

            df_void = pd.concat((df_void, merge_ff))
        except KeyError:
            print(f'Нет данный в {country_value} месяца {month_value}')
    print(f'Выгрузка данных по {country_value} составила {round((datetime.now() - start).total_seconds(), 2)} секунд')

# Сохраняем данные в файл
df_void.to_csv(f'india_{dict_config["type_flow"]}.csv', index=False)
