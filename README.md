# 🧪 ETL Pipeline: CSV to PostgreSQL

This project demonstrates a simple yet effective **ETL (Extract, Transform, Load)** pipeline using Python. The pipeline reads data from a `.csv` file, processes it using `pandas`, and loads it into a PostgreSQL database using SQLAlchemy and psycopg2.

---

## 📁 Project Structure

```
ETL-CSV-to-PostgreSQL/
│
├── etl_pipeline.ipynb      # Main Jupyter notebook to run the ETL
├── .env                    # Environment variables (ignored in version control)
├── .gitignore              # Git ignore rules
├── requirements.txt        # Python dependencies
└── README.md               # Project documentation
```

---

## 🚀 Features

- Read CSV using `pandas`
- Securely connect to PostgreSQL using `.env` file
- Insert data via `SQLAlchemy` ORM
- Clean and modular code via Jupyter Notebook

---

## 🛠️ Technologies Used

- Python
- pandas
- SQLAlchemy
- psycopg2
- PostgreSQL

---

### Create and Configure `.env`

Create a `.env` file in the root directory:

```ini
DB_USERNAME='your_postgres_user'
DB_PASSWORD='your_password'
HOST='localhost'
PORT='5432'
DB_DATABASE='your_database_name'
```

### Access the values form .env



> ✅ **Make sure your PostgreSQL server is running and the database exists.**

from dotenv import load_dotenv
import os

load_dotenv()

USERNAME = os.getenv('DB_USERNAME')
PASSWORD = os.getenv('DB_PASSWORD')
HOST = os.getenv('HOST')
PORT = os.getenv('PORT')
DATABASE = os.getenv('DB_DATABASE')

---

## ▶️ How to Run

Open the notebook and run the cells step-by-step:

```bash
jupyter notebook etl_pipeline.ipynb
```

It will:
1. Load the `.csv` file.
2. Connect to PostgreSQL using environment variables.
3. Create table and insert data.

---

## 🔒 Security Notes

- `.env` is added to `.gitignore` to avoid leaking sensitive credentials.
- Never commit your real `.env` file to GitHub.

---

## ✅ Recommended Improvements

- Add input CSVs to a `/data` directory (with `.gitignore` rule like `data/*.csv`).
- Convert the notebook into a script for automation.
- Add logging and exception handling.

