# Структура DAG:

### DAG:
1) запускается ежедневно в 3 часа schedule_interval="0 3 * * *", start_date=datetime(2025,1,1), catchup=True, max_active_runs=1 
2) дефолтные аргументы  
default_args = {  
    'owner': 'airflow',  
    'retries': 3,                    
    'retry_delay': timedelta(minutes=5),  # с интервалом 5 минут  
    'email_on_failure': False,       #  используем собственное оповещение в ТГ  
}
3) fetch_flight - PythonOperator, который выполняет скрипт (из задания 4), и через xcom передаём путь к csv файлу 
4) valuidate_data - GreatExpectationsOperator или PythonOperator с использованием Great Expectations для валидации данных. Проверка схемы и типов, объём данных более 0 записей или N, бизнес правила где значения в допустимых пределах, дата рейса соответствующая запросу и тд 
5) load_to_clickhouse - используем ClickHouseOperator предварительно его установив apache-airflow-providers-clickhouse
6) notify_telegram - PythonOperator который отправляет уведомление в тг 
7) зависимости - fetch_flight >> valuidate_data >> load_to_clickhouse >> notify_telegram

### Допы:  
1) настроить оповещение если таска окончательно провалилась email_on_failure=True или настроить Python оператор с оповещением в тг с trigger_rule на one_failed
2) идемпотентность реализовать можно с ReplicatedReplacingMergeTree/ReplacingMergeTree или проверять максимальную дату существующих данных в ClickHouse и сверять с датой загружаемых данных, если дата загружаемых данных больше, то грузим
3) по валидации использовать Great Expectations как выше описал
4) перед fetch_flight можно использовать HttpSensor для проверки появились ли данные, когда источник данных не готов по расписанию 



