FROM python:3.12.10

WORKDIR /app

# Kopiramo aplikacijo
COPY app/ app/
COPY requirements.txt .

# Namestimo Python pakete
# Ker watchdog ni več potreben, requirements.txt je lahko prazen ali vsebuje samo druge pakete
RUN pip install --no-cache-dir -r requirements.txt

# Privzeti ukaz
CMD ["python", "app/processor.py"]