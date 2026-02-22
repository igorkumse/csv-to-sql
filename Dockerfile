# Uporabimo uradno Python sliko
FROM python:3.11-slim

# Nastavimo delovni direktorij v containerju
WORKDIR /app

# Kopiramo vse datoteke v container
COPY . .

# (ni odvisnosti, ampak pustimo za prihodnost)
RUN pip install --no-cache-dir -r requirements.txt

# Privzeti ukaz ob zagonu containerja
CMD ["python", "main.py"]
