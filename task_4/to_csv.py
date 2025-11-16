import csv
import time
from datetime import datetime
import requests
import logging
import json

logging.basicConfig(level=logging.DEBUG)


def get_data(date: str, retries: int, delay_second: int):
    for attempt in range(retries):
        try:
            logging.info(f'Попытка {attempt + 1} из {retries}')
            r = requests.get(f'http://127.0.0.1:8000/flights?date={date}')
            logging.info(f'Успешно получили {len(r.json())} записей')
            return r.json()
        except ConnectionError as e:
            logging.error(f'Ошибка сети {e}')
            if attempt < retries - 1:
                logging.info(f'Повтор через {delay_second}')
                time.sleep(delay_second)
        except json.decoder.JSONDecodeError as e:
            logging.error(f'Ошибка сети {e}')
            if attempt < retries - 1:
                logging.info(f'Повтор через {delay_second}')
                time.sleep(delay_second)
    logging.critical('Превышено число попыток')
    raise


def save_to_csv(data: list[dict], date: str):
    if len(data) == 0:
        logging.error('Данных нет')
        raise IndexError('Данных нет')
    for row in data:
        row['load_timestamp'] = datetime.now().isoformat()
    logging.info('Успешно добавлен load_timestamp')
    with open(f'flight_{date}.csv', 'w') as file:
        writer = csv.DictWriter(file, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)
    logging.info(f'Данные за {date} успешно сохранены')


if __name__ == '__main__':
    date = '2025-01-02'
    data = get_data(date=date, retries=3, delay_second=3)
    save_to_csv(data, date)
