# Imagem leve Python 3.11 (total compatibilidade de wheels pre-compilados com cassandra-driver)
FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Instala dependencias primeiro para maximizar aproveitamento do cache de camadas Docker
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Copia o codigo da aplicacao e do simulador IoT
COPY database.py main.py simulator.py ./

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
