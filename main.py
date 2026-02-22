import csv
import os


def escape_sql_value(value):
    """Escapes single quotes for SQL."""
    if value is None:
        return "NULL"
    value = str(value)
    return "'" + value.replace("'", "''") + "'"


def csv_to_sql(input_file, output_file, table_name):
    if not os.path.exists(input_file):
        print(f"Napaka: Datoteka {input_file} ne obstaja.")
        return

    with open(input_file, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        columns = reader.fieldnames

        if not columns:
            print("CSV nima glave (header).")
            return

        with open(output_file, 'w', encoding='utf-8') as outfile:
            for row in reader:
                values = [escape_sql_value(row[col]) for col in columns]
                sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(values)});"
                outfile.write(sql + "\n")

    print(f"SQL stavki zapisani v {output_file}")


if __name__ == "__main__":
    INPUT_FILE = "input.csv"
    OUTPUT_FILE = "output.sql"
    TABLE_NAME = "moja_tabela"

    csv_to_sql(INPUT_FILE, OUTPUT_FILE, TABLE_NAME)