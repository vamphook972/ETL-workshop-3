import json
import joblib
import numpy as np
import pandas as pd
from kafka import KafkaConsumer
from sqlalchemy import create_engine, text

TOPIC = "happiness"
BOOTSTRAP_SERVERS = "localhost:9092"
MODEL_PATH = "../models/happiness_model.pkl"
DB_PATH = "sqlite:///../database/predictions.db"

# Cargar modelo
model = joblib.load(MODEL_PATH)
print("Modelo cargado")

# Conectar a base de datos
engine = create_engine(DB_PATH)

# Crear tabla si no existe
with engine.connect() as conn:
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS predictions (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            country     TEXT,
            year        INTEGER,
            actual      REAL,
            predicted   REAL,
            error       REAL
        )
    """))
    conn.commit()
print("Base de datos lista")

# Iniciar consumer
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset="earliest",
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

print(f"Escuchando topic '{TOPIC}'...\n")

FEATURES = ["gdp_per_capita", "social_support", "life_expectancy",
            "freedom", "generosity", "corruption"]

for message in consumer:
    data = message.value

    # Predecir
    X = pd.DataFrame([[data[f] for f in FEATURES]], columns=FEATURES)
    predicted = round(float(model.predict(X)[0]), 4)
    actual = round(data["happiness_score"], 4)
    error = round(abs(actual - predicted), 4)

    # Guardar en BD
    with engine.connect() as conn:
        conn.execute(text("""
            INSERT INTO predictions (country, year, actual, predicted, error)
            VALUES (:country, :year, :actual, :predicted, :error)
        """), {
            "country": data["country"],
            "year": data["year"],
            "actual": actual,
            "predicted": predicted,
            "error": error
        })
        conn.commit()

    print(f"{data['country']} ({data['year']}) | Real: {actual} | Pred: {predicted} | Error: {error}")