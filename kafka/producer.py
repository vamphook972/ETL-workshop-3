import pandas as pd
import json
import time
from kafka import KafkaProducer

TOPIC = "happiness"
BOOTSTRAP_SERVERS = "localhost:9092"

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

df = pd.read_csv("../data/processed/happiness_dataset.csv")

print(f"Enviando {len(df)} registros al topic '{TOPIC}'...")

for _, row in df.iterrows():
    message = {
        "country": row["country"],
        "year": int(row["year"]),
        "gdp_per_capita": row["gdp_per_capita"],
        "social_support": row["social_support"],
        "life_expectancy": row["life_expectancy"],
        "freedom": row["freedom"],
        "generosity": row["generosity"],
        "corruption": row["corruption"],
        "happiness_score": row["happiness_score"]
    }
    producer.send(TOPIC, value=message)
    print(f"  Enviado: {message['country']} ({message['year']})")
    time.sleep(0.05)  # 50ms entre mensajes para simular streaming

producer.flush()
print("Todos los registros enviados")