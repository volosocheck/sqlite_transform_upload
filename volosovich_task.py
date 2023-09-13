import sqlite3
import pandas as pd
from datetime import datetime
import psutil


def create_sqlite_table(table_name: str) -> None:
    """
    Создать таблицу SQLite, если она не существует.

    Args:
        table_name (str): Название таблицы, которую нужно создать.

    Returns:
        None
    """
    conn = sqlite3.connect(f"{table_name}.db")
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS {table_name} (
            timestamp DATETIME,
            player_id INTEGER,
            event_id INTEGER,
            error_id INTEGER,
            json_server TEXT,
            json_client TEXT
        )
    '''.format(table_name=table_name))

    conn.commit()
    conn.close()
    print(f"Таблица {table_name} создана!")


def process_data(date: str, server_path: str = 'server', client_path: str = 'client',
                 cheaters_table_name: str = 'cheaters', players_table_name: str = 'full_players_info') -> None:
    """
    Обработать данные и сохранить их в таблицу SQLite.

    Args:
        date (str): Желаемая дата в формате 'YYYY-MM-DD'.
        server_path (str): Путь к файлу CSV сервера.
        client_path (str): Путь к файлу CSV клиента.
        cheaters_table_name (str): Название таблицы читеров SQLite.
        players_table_name (str): Название таблицы игроков SQLite.

    Returns:
        None
    """
    desired_date = datetime.strptime(date, '%Y-%m-%d').date()

    # Загрузка данных сервера и клиента
    server_df = pd.read_csv(f"{server_path}.csv")
    server_df['timestamp_data'] = pd.to_datetime(server_df['timestamp'], unit='s')
    server_df = server_df[server_df['timestamp_data'].dt.date == desired_date]
    server_df = server_df.rename(columns={'description': 'json_server'})

    client_df = pd.read_csv(f"{client_path}.csv")
    client_df['timestamp_data'] = pd.to_datetime(client_df['timestamp'], unit='s')
    client_df = client_df[client_df['timestamp_data'].dt.date == desired_date]
    client_df = client_df.rename(columns={'description': 'json_client'})

    # Объединение данных
    merged_data = pd.merge(server_df, client_df[['error_id', 'player_id', 'json_client']], on='error_id')

    # Загрузка данных о читерах
    conn = sqlite3.connect(f"{cheaters_table_name}.db")
    cheaters_data = pd.read_sql_query(f"SELECT * FROM {cheaters_table_name}", conn)
    cheaters_data['ban_time'] = pd.to_datetime(cheaters_data['ban_time'])
    cheaters_data = cheaters_data[cheaters_data['ban_time'].dt.date < desired_date]

    # Фильтрация данных, исключая читеров
    filtered_data = merged_data[~merged_data['player_id'].isin(cheaters_data['player_id'])]

    # Удаление столбца 'timestamp_data'
    filtered_data.drop(columns=['timestamp_data'], inplace=True)

    # Создание таблицы игроков
    create_sqlite_table(players_table_name)

    # Сохранение данных в таблицу SQLite
    conn = sqlite3.connect(f"{players_table_name}.db")
    filtered_data.to_sql(name=players_table_name, con=conn, if_exists='append', index=False)
    conn.close()

    print(f"Выгрузка данных в таблицу {players_table_name} завершена!")


if __name__ == "__main__":
    process = psutil.Process()
    start_memory = process.memory_info().rss / 1024 #Перевод в КБ
    process_data('2023-09-13')
    end_memory = process.memory_info().rss / 1024

    print(f"Потребление памяти: {end_memory - start_memory} КБ")