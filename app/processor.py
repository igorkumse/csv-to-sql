import csv
from pathlib import Path
import shutil
import logging
from logging.handlers import TimedRotatingFileHandler
import time

# --- Direktories ---
INPUT_DIR = Path("/data/INPUT")
OUTPUT_DIR = Path("/data/OUTPUT")
PROCESSED_DIR = Path("/data/PROCESSED")
ERROR_DIR = Path("/data/ERROR")
LOG_DIR = Path("/data/logs")

# --- Ustvari mape, če ne obstajajo ---
for d in [INPUT_DIR, OUTPUT_DIR, PROCESSED_DIR, ERROR_DIR, LOG_DIR]:
    d.mkdir(parents=True, exist_ok=True)

# --- Nastavi dnevno rotacijo logov ---
logger = logging.getLogger("csv_watcher")
logger.setLevel(logging.INFO)

log_handler = TimedRotatingFileHandler(
    LOG_DIR / "csv_watcher.log",
    when="midnight",
    interval=1,
    backupCount=30,  # hrani zadnjih 30 dni
    encoding="utf-8"
)
formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
log_handler.setFormatter(formatter)
logger.addHandler(log_handler)

# --- Pomožna funkcija za SQL escaping ---
def escape_sql_value(value):
    if value is None or value == "":
        return "NULL"
    return f"'{str(value).replace('\'', '\'\'')}'"

# --- Standard CSV obdelava ---
def process_standard_csv(file_path):
    output_file = OUTPUT_DIR / f"{file_path.stem}.sql"

    if output_file.exists():
        logger.error(f"{output_file.name} že obstaja! Premik {file_path.name} v ERROR mapo.")
        shutil.move(str(file_path), str(ERROR_DIR / file_path.name))
        return

    try:
        with open(file_path, newline='', encoding='utf-8') as csvfile, \
             open(output_file, 'w', encoding='utf-8') as outfile:

            reader = csv.DictReader(csvfile)
            columns = reader.fieldnames

            if not columns:
                logger.warning(f"{file_path.name} nima glave, premik v ERROR mapo")
                shutil.move(str(file_path), str(ERROR_DIR / file_path.name))
                return

            for row in reader:
                values = [escape_sql_value(row[col]) for col in columns]
                sql = f"INSERT INTO {file_path.stem} ({', '.join(columns)}) VALUES ({', '.join(values)});"
                outfile.write(sql + "\n")

        logger.info(f"Processed CSV: {file_path.name} → {output_file.name}")
        shutil.move(str(file_path), str(PROCESSED_DIR / file_path.name))

    except Exception as e:
        logger.error(f"Napaka pri obdelavi {file_path.name}: {e}")
        shutil.move(str(file_path), str(ERROR_DIR / file_path.name))

# --- Fixed-width CSV obdelava z numerično kontrolo ---
def process_fixed_width_file(file_path):
    insert_file = OUTPUT_DIR / f"{file_path.stem}-INSERT.sql"
    update_file = OUTPUT_DIR / f"{file_path.stem}-UPDATE.sql"

    try:
        with open(file_path, 'r', encoding='utf-8') as rf, \
             open(insert_file, 'w', encoding='utf-8') as wf_insert, \
             open(update_file, 'w', encoding='utf-8') as wf_update:

            for line in rf:
                if len(line) < 54:
                    logger.error(f"Vrstica prekratka ({len(line)} znakov): premik v ERROR")
                    shutil.move(str(file_path), str(ERROR_DIR / file_path.name))
                    return

                # --- slice polja ---
                fin_inst_in = line[0:2].strip()
                org_enota_in = '0' if line[2:5] == '   ' else line[2:5].strip()
                aplikacija_in = line[5:9].strip()
                kom_stevilka_in = line[9:25].strip()
                apl_domicil_in = line[25:26].strip()
                status_in = line[26:27].strip()

                fin_inst_out = line[27:29].strip()
                org_enota_out = line[29:32].strip()
                aplikacija_out = line[32:36].strip()
                kom_stevilka_out = line[36:52].strip()
                apl_domicil_out = line[52:53].strip()
                status_out = line[53:54].strip()

                # --- preverjanje numeričnosti ---
                numeric_fields = [
                    fin_inst_in, org_enota_in, aplikacija_in, kom_stevilka_in,
                    apl_domicil_in, status_in, fin_inst_out, org_enota_out,
                    aplikacija_out, kom_stevilka_out, apl_domicil_out, status_out
                ]

                if not all(f.isdigit() for f in numeric_fields):
                    logger.error(f"Nenumerična vrstica v {file_path.name}: {line.strip()}")
                    shutil.move(str(file_path), str(ERROR_DIR / file_path.name))
                    return

                # --- formatiramo SQL polja ---
                kom_stevilka_in_sql = f"'{kom_stevilka_in}'"
                kom_stevilka_out_sql = f"'{kom_stevilka_out}'"

                # --- INSERT SQL ---
                line_insert = (
                    f"INSERT INTO DBU01.T01PREV_NGB VALUES ("
                    f"{fin_inst_in}, {org_enota_in}, {aplikacija_in}, {kom_stevilka_in_sql}, "
                    f"{apl_domicil_in}, {status_in}, {fin_inst_out}, {org_enota_out}, "
                    f"{aplikacija_out}, {kom_stevilka_out_sql}, {apl_domicil_out}, {status_out}, current timestamp);"
                )
                wf_insert.write(line_insert + "\n")

                # --- UPDATE SQL ---
                line_update = (
                    f"UPDATE DBU01.T01PREV_NGB SET "
                    f"STATUS_IN = {status_in}, FIN_INST_OUT = {fin_inst_out}, "
                    f"ORG_ENOTA_OUT = {org_enota_out}, APLIKACIJA_OUT = {aplikacija_out}, "
                    f"KOM_STEVILKA_OUT = {kom_stevilka_out_sql}, APL_DOMICIL_OUT = {apl_domicil_out}, "
                    f"STATUS_OUT = {status_out} "
                    f"WHERE FIN_INST_IN = {fin_inst_in} AND ORG_ENOTA_IN = {org_enota_in} "
                    f"AND APLIKACIJA_IN = {aplikacija_in} AND KOM_STEVILKA_IN = {kom_stevilka_in_sql};"
                )
                wf_update.write(line_update + "\n")

        logger.info(f"Fixed-width file processed: {file_path.name}")
        shutil.move(str(file_path), str(PROCESSED_DIR / file_path.name))

    except Exception as e:
        logger.error(f"Napaka pri obdelavi {file_path.name}: {e}")
        shutil.move(str(file_path), str(ERROR_DIR / file_path.name))

# --- Glavna funkcija za obdelavo ---
def process_file(file_path):
    if file_path.suffix.lower() == ".csv":
        if "FIXED" in file_path.name.upper():
            process_fixed_width_file(file_path)
        else:
            process_standard_csv(file_path)

# --- Polling loop ---
def watch_polling():
    logger.info("CSV watcher zagnan, polling INPUT mape vsakih 30 sekund...")
    while True:
        try:
            for file in INPUT_DIR.glob("*.csv"):
                process_file(file)
            time.sleep(30)
        except Exception as e:
            logger.error(f"Napaka v polling loop: {e}")

if __name__ == "__main__":
    watch_polling()