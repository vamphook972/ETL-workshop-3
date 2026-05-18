# 🌍 ETL Workshop 003 — Predicción del Happiness Score con Apache Kafka

Pipeline completo de ingeniería de datos que combina Machine Learning con streaming en tiempo real usando Apache Kafka. El sistema entrena un modelo de regresión lineal sobre datos del World Happiness Report (2015–2019), y luego transmite predicciones registro por registro a través de Kafka, almacenando los resultados en una base de datos SQLite.

---

## 📁 Estructura del proyecto

```
ETL-workshop-3/
│
├── data/
│   ├── raw/                        # CSVs originales (2015–2019)
│   └── processed/
│       └── happiness_dataset.csv   # Dataset limpio y normalizado
│
├── notebooks/
│   ├── eda.ipynb                   # Exploración y limpieza de datos
│   └── model_training.ipynb        # Entrenamiento y evaluación del modelo
│
├── models/
│   └── happiness_model.pkl         # Modelo entrenado (generado por el notebook)
│
├── kafka/
│   ├── producer.py                 # Transmite los datos al topic de Kafka
│   └── consumer.py                 # Recibe datos, predice y guarda en BD
│
├── database/
│   └── predictions.db              # SQLite con las predicciones generadas
│
├── docker-compose.yml              # Configuración de Kafka en KRaft mode
├── requirements.txt                # Dependencias del proyecto
└── README.md
```

---

## 🛠️ Tecnologías utilizadas

- **Python 3.14**
- **Pandas / NumPy** — manipulación de datos
- **Scikit-learn** — entrenamiento del modelo de regresión lineal
- **Matplotlib / Seaborn** — visualizaciones en el EDA
- **Apache Kafka** (Confluent 7.6.0, KRaft mode) — streaming de datos
- **kafka-python** — cliente Kafka para Python
- **SQLAlchemy + SQLite** — almacenamiento de predicciones
- **Docker** — orquestación de Kafka

---

## ⚙️ Instalación y configuración

### 1. Clonar el repositorio

```bash
git clone https://github.com/tu-usuario/ETL-workshop-3.git
cd ETL-workshop-3
```

### 2. Crear entorno virtual e instalar dependencias

```bash
python3 -m venv .venv
source .venv/bin/activate        # En Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 3. Levantar Kafka con Docker

```bash
docker-compose up -d
docker ps   # Verificar que el contenedor kafka esté Up
```

---

## 🚀 Ejecución del pipeline

### Paso 1 — EDA y preparación de datos
Abrir y ejecutar todas las celdas de `notebooks/eda.ipynb`.
Genera el archivo `data/processed/happiness_dataset.csv`.

### Paso 2 — Entrenar el modelo
Abrir y ejecutar todas las celdas de `notebooks/model_training.ipynb`.
Genera el archivo `models/happiness_model.pkl`.

### Paso 3 — Correr el Consumer (Terminal 1)
```bash
python3 kafka/consumer.py
```

### Paso 4 — Correr el Producer (Terminal 2)
```bash
python3 kafka/producer.py
```

El consumer queda escuchando el topic `happiness`, recibe cada registro, genera la predicción y la guarda en `database/predictions.db`.

---

## 📊 Resultados del modelo

| Métrica | Valor |
|---|---|
| Algoritmo | Regresión Lineal |
| Split | 70% entrenamiento / 30% prueba |
| MAE | 0.4486 |
| R² | 0.7266 |

El modelo explica el **72.7% de la varianza** del Happiness Score usando 6 features: GDP per cápita, soporte social, esperanza de vida, libertad, generosidad y percepción de corrupción.

### Importancia de features (por coeficiente)

| Feature | Coeficiente |
|---|---|
| Freedom | 1.6213 |
| GDP per capita | 1.0878 |
| Corruption | 1.0299 |
| Life expectancy | 0.9739 |
| Social support | 0.6292 |
| Generosity | 0.4048 |

---

## 🗄️ Esquema de la base de datos

Tabla `predictions` en `database/predictions.db`:

| Columna | Tipo | Descripción |
|---|---|---|
| id | INTEGER | Clave primaria autoincremental |
| country | TEXT | Nombre del país |
| year | INTEGER | Año del registro |
| actual | REAL | Happiness Score real |
| predicted | REAL | Happiness Score predicho |
| error | REAL | Error absoluto (|actual - predicted|) |

---

## 📦 Dependencias principales

```
pandas==2.2.2
numpy==1.26.4
scikit-learn==1.4.2
matplotlib==3.8.4
seaborn==0.13.2
kafka-python==2.0.2
sqlalchemy==2.0.30
joblib==1.4.2
```

---

## 👤 Autor

**vamphook**  
Programa: Ingeniería de Datos e IA  
Curso: ETL — Workshop 003