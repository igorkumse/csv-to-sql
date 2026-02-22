import csv
import os
import time
from pathlib import Path


INPUT_DIR = Path("/data/INPUT")
OUTPUT_DIR = Path("/data/OUTPUT")
PROCESSED_DIR = Path("/data/PROCESSED")


def escape_sql_value(value):
    if value is None:
        return "NULL"
    return "'" + str(value).replace("'", "''") + "'"


def process_file(file_path):
    table_name = file_path.stem
    output_file = OUTPUT_DIR / f"{table_name}.sql"

    with open(file_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        columns = reader.fieldnames

        with open(output_file, 'w', encoding='utf-8') as outfile:
            for row in reader:
                values = [escape_sql_value(row[col]) for col in columns]
                sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(values)});"
                outfile.write(sql + "\n")

    print(f"Processed: {file_path.name}")

    file_path.rename(PROCESSED_DIR / file_path.name)


def watch():
    print("Watching INPUT folder...")
    while True:
        for file in INPUT_DIR.glob("*.csv"):
            process_file(file)
        time.sleep(5)