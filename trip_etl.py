#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests
import pandas as pd

from pathlib import Path
import os
import zipfile

import time
import re

from clickhouse_driver import Client

from loguru import logger

#Перманентно поменял настройки вывода графиков
from matplotlib import pylab
from pylab import *
pylab.rcParams['figure.figsize'] = (18.0, 6.0)
plt.rcParams.update({'font.size': 13})

#Скрыл вывод предупреждений.
import warnings
warnings.filterwarnings('ignore') #чтобы вернуть: (action='once')

# Глобально снял ограничение на кол-во отображаемых результатов для каждой ячейки ввода кода.
from IPython.core.interactiveshell import InteractiveShell
InteractiveShell.ast_node_interactivity = "all"

# Включил возможность форматировать стили текста с помощью метода printmd()
from IPython.display import Markdown, display, HTML
def printmd(string):
    display(Markdown(string))
    
# Снял ограничение на вывод кол-ва столбцов и ширины колонки.
pd.set_option('display.max_columns', 0)
pd.set_option('display.max_colwidth', -1)

# Установил формат вывода в таблице на 2 знака после запятой.
pd.options.display.float_format = '{:,.3f}'.format
    
# Добавил функцию вывода таблиц в одну строку, для экономии пространства и улучшения восприятия информации.
def display_side_by_side(dfs:list, captions:list):
    output = ""
    combined = dict(zip(captions, dfs))
    for caption, df in combined.items():
        output += df.style.set_table_attributes("style='display:inline'").set_caption(caption)._repr_html_()
        output += "\xa0\xa0\xa0"
    display(HTML(output))


# In[2]:


logger.add('logs/logs.log', format="{time} {level} {message}", filter="my_module", level="INFO")


# ### Extract

# In[3]:


test_req = requests.get('https://s3.amazonaws.com/tripdata')
tt = pd.read_xml(test_req.text)


# In[4]:


table_source = tt[tt['Key'] == tt['Key']].iloc[:, 3:-1].reset_index(drop=True)[:-1]
table_source.head(3)


# In[5]:


table_source['url'] =  table_source['Key'].apply(lambda x: 'https://s3.amazonaws.com/tripdata/' + str(x))
table_source.head(3)


# In[6]:


# sample2 = table_source.iloc[:, :-1]
# sample1 = tt.iloc[:, 3:-1]


# In[21]:


#Сравнение таблицы исходника с актуальной версией
logger.info('Сравнение таблицы исходника с актуальной версией')
if os.path.exists('etl_log.csv'):
    logger.info('Таблица загрузчика существует.')
    
    table_actual = pd.read_csv('etl_log.csv',sep=";")
    del table_actual['update']
    
    table_final=table_source.merge(table_actual,indicator=True,how='left')
    table_final._merge=table_final._merge.eq('both')
    table_final = table_final.rename(columns={'_merge':'update'})
    table_final['update'] = ~table_final['update']
    changes = table_final[table_final['update'] != False]
    
    if len(changes) == 0:
        logger.info('Изменений нет.')
    else:
        logger.info(str(len(changes)) + 'изменений найдено.')
else:
    table_source['downloaded'] = False
    table_source['extracted'] = False
    table_source['ch_uploaded'] = False
    table_source['csv'] = 'No'
    
    table_source.to_csv('etl_log.csv',sep=";",index=False)
    
    logger.info('Файл таблицы создан.')


# In[22]:


# Методы сохранения файлов
def get_file_file(url): #делаем запрос на сам файл
    try:
        r = requests.get(url, stream=True)
        return r
    except:
        print('Нет коннекта при соединении с файлом по указанной ссылке.')
        logger.info('Нет коннекта при соединении с файлом по указанной ссылке.')
        pass


def get_file_name(url): #получаем имя файла
    try:
        name = url.split('/')[-1]  #потрошим ссылку через / и берём оттуда последние данные
        return name
    except:
        return url

def save_file(name, file_object): #сохраняет файл в корневой папке
    try:
        path = './files/'+name
        with open(path, 'bw') as f:
            for chunk in file_object.iter_content(None):
                f.write(chunk)
            print(str(name)+' загружен')
            logger.info(str(name)+' загружен')
    except:
        pass
    time.sleep(5)


# In[23]:


# Директория сохранения файлов
dir_path = r'./files/'

# Список файлов в директории
res = os.listdir(dir_path)


# In[24]:


# for file in res:
#         print(file+" : "+ str(Path(dir_path+file).stat().st_size))


# In[25]:


def zip_extractor(target_file, index):
    endswith = '.csv'
    try:
        path = Path(target_file)
        target_file_size = path.stat().st_size
        
        with zipfile.ZipFile(target_file) as z:
            for file in z.namelist():
                if file.endswith(endswith):
                    if (target_file_size) > 0:
                        logger.info("Извлекаемый из архива файл существует: "+file)
                    else:
                        z.extract(file,'./files/csv/')
                logger.info(file+" файл разархивирован")
                if file[1] != "_":
                    table_final.at[index, 'csv'] = file
        table_final.at[index, 'extracted'] = True
    except ValueError as ve:
        logger.info(ve)


# ### Transform Load

# In[37]:


def load_trip(filename,index):
    try:
        df = pd.read_csv('./files/csv/'+filename)
#         df.info()
        df.columns = df.columns.str.replace(' ', '')
        df.columns = df.columns.str.lower()
        try:
            df['stop_date'] = pd.to_datetime(df['stoptime'])
        except:
            df = df.rename(columns={'ended_at':'stop_date','ride_id':'bikeid','':'tripduration','member_casual':'gender'})
            df['stop_date'] = pd.to_datetime(df['stop_date'])
            df['started_at'] = pd.to_datetime(df['started_at'])
            df['tripduration'] = (df['stop_date'] - df['started_at']) / np.timedelta64(1, 's')
            df = df.replace('member', 1).replace('casual', 2)
            pass
            
        df['stop_date'] = df['stop_date'].astype('datetime64[D]')
    #     df.head(5)

        trips_count_daily = df.groupby('stop_date')['bikeid'].count().to_frame().reset_index().rename(columns={'bikeid':'trips_count','stop_date':'date'})
    #     trips_count_daily.head(10)
        avg_duration_daily = df.groupby('stop_date')['tripduration'].mean().to_frame().reset_index().rename(columns={'tripduration':'avg_trip_dur','stop_date':'date'})
    #     avg_duration_daily.head(10)


        gender_df = df.groupby('stop_date')['bikeid'].count().to_frame()
        for sex in df['gender'].unique():
            gender_df['gender_'+str(sex)] = df[df['gender'] == sex].groupby('stop_date')['gender'].count().to_frame()
        gender_daily = gender_df.reset_index().rename(columns={'stop_date':'date'})
        del gender_daily['bikeid']
        for sex in df['gender'].unique():
            gender_daily['gender_'+str(sex)] = gender_daily['gender_'+str(sex)].fillna(0).astype(int)
        if 'gender_0' not in gender_daily.columns:
            gender_daily['gender_0'] = 0
    #     gender_daily.head(5)

        client = Client(host='localhost', settings={'use_numpy': True}).from_url('clickhouse://default:@localhost:19000/default') #логин пароль порт по умолчанию, дефолтная БД тоже по умолчанию
    #     result = client.execute("SHOW TABLES")
    #     print(result)

        client.execute('INSERT INTO `default`.gender_daily VALUES', gender_daily.to_dict('records'),types_check=True)
        client.execute('OPTIMIZE TABLE `default`.gender_daily')
    #     client.query_dataframe('SELECT * FROM gender_daily')

        client.execute('INSERT INTO `default`.trips_count_daily VALUES', trips_count_daily.to_dict('records'),types_check=True)
        client.execute('OPTIMIZE TABLE `default`.trips_count_daily')
    #     client.query_dataframe('SELECT * FROM trips_count_daily')

        client.execute('INSERT INTO `default`.avg_duration_daily VALUES', avg_duration_daily.to_dict('records'),types_check=True)
        client.execute('OPTIMIZE TABLE `default`.avg_duration_daily')
    # client.query_dataframe('SELECT * FROM avg_duration_daily')
        table_final.at[index, 'ch_uploaded'] = True
        logger.info('Файл успешно загружен в БД: ' + filename)
    except ValueError as ve:
        logger.info(ve)


# In[27]:


df = pd.read_csv('./files/csv/202102-citibike-tripdata.csv')
df.info()


# In[33]:


for index, row in table_final.iterrows():
    file = row['url']
    filename = file.split('/')[-1]
    try:
        path = Path(dir_path+filename)
        target_file_size = path.stat().st_size
    except:
        logger.info("Файл скачивается: "+file)
        save_file(get_file_name(file), get_file_file(file))
    else:
        if (target_file_size) > 0:
            logger.info("Файл существует: "+filename)
        else:
            logger.info("Файл скачан не полностью, продолжается скачивание: "+file)
            save_file(get_file_name(file), get_file_file(file))
            
    table_final.at[index, 'downloaded'] = True
    zip_extractor('.\\'+str(path),index)


# In[34]:


table_final.to_csv('etl_log.csv',sep=";",index=False)


# In[38]:


for index, row in table_final.iterrows():
    uploaded = row['ch_uploaded']
    if uploaded != True:
        load_trip(row['csv'],index)
    else:
        logger.info('Датасет уже существует в БД: ' + row['csv'])


# In[39]:


table_final.to_csv('etl_log.csv',sep=";",index=False)

