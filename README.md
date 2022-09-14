# NetologyDiplom
 
1. Развернул БД clickhouse и шедулер airflow в Docker
2. Построил архитектуру, которую описал в файле "Диаграмма-схема архитектуры.jpg"
3. Написал ETL - trip_etl.py. ETL скачивает файлы и грузит в БД. Подробное описание в Netology Diplom [Final].ipynb
4. Создал ДАГ, который запускается через airflow 1 раз в сутки. Используется SSHOperator, устанавливается ssh соединение к windows из контейнера docker airflow, который запускает bin.bat 
5. Данные хранятся в таблицах:
	- trips_count_daily (количество поездок в день)
	- avg_duration_daily (средняя продолжительность поездок в день)
	- gender_daily (распределение поездок пользователей, разбитых по категории «gender»)
6. Таблицы создавал через DDL, использовались движки ReplacingMergeTree и AggregatingMergeTree. в ETL прописывается OPTIMIZE TABLE для исключения дубликатов.
