import pandas as pd
import time
from collections import defaultdict
import threading

batch_size = 100
interval = 1

csv_file = 'portugal_listinigs.csv'
df = pd.read_csv(csv_file, low_memory=False)

grouped_data = defaultdict(list)

# Структура для хранения агрегированных данных
aggregated_data = {
    'total_price': 0,  # Сумма цен
    'total_area': 0,  # Сумма площадей
    'total_count': 0,  # Общее количество объявлений
}


# Функция для обновления агрегированных данных
def update_aggregated_data(record):
    price = float(record['Price'])
    area = float(record['TotalArea'])

    aggregated_data['total_price'] += price
    aggregated_data['total_area'] += area
    aggregated_data['total_count'] += 1


# Функция для вычисления и вывода агрегированных данных
def calculate_aggregated_data():
    if aggregated_data['total_count'] > 0:
        avg_price = aggregated_data['total_price'] / aggregated_data['total_count']
        avg_area = aggregated_data['total_area'] / aggregated_data['total_count']
        avg_price_per_sqm = aggregated_data['total_price'] / aggregated_data['total_area']
    else:
        avg_price = avg_area = avg_price_per_sqm = 0

    print(f"\nСредняя цена недвижимости: {avg_price:.2f}")
    print(f"Средняя площадь: {avg_area:.2f} м²")
    print(f"Общее количество объявлений: {aggregated_data['total_count']}")
    print(f"Средняя цена за квадратный метр: {avg_price_per_sqm:.2f}")


# Функция для отправки данных и обновления агрегированных данных
def send_data(record):

    update_aggregated_data(record)

    key = (record['Type'], record['District'])
    grouped_data[key].append(record.to_dict())

    # Для демонстрации: вывод записи
    print(f"Отправлено: {record.to_dict()}")


# Функция для обработки данных с интервалом и батчами
def stream_data(df):
    for i in range(0, len(df), batch_size):
        batch = df.iloc[i:i + batch_size]
        for _, record in batch.iterrows():
            # Фильтрация данных
            if pd.notna(record['Price']) and pd.notna(record['TotalArea']):
                price = float(record['Price'])
                total_area = float(record['TotalArea'])

                if price >= 50000 and total_area >= 20:
                    send_data(record)
        time.sleep(interval)


# Функция для периодического обновления агрегированных данных
def periodic_update():
    while True:
        time.sleep(30)
        calculate_aggregated_data()


# Запуск фонового потока для обновления данных каждые 30 секунд

update_thread = threading.Thread(target=periodic_update, daemon=True)
update_thread.start()


stream_data(df)

# Итоговая обработка сгруппированных данных
for key, records in grouped_data.items():
    property_type, district = key
    print(f"\nТип недвижимости: {property_type}, Район: {district}")
    print(f"Количество объектов: {len(records)}")