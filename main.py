import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from faker import Faker
import random
from datetime import datetime, timedelta
import getpass

fake = Faker('pt_BR')

print("Conectando ao PostgreSQL local (sem senha)...")
DB_CONFIG = {
    'host': 'localhost',
    'port': '5432',
    'database': 'postgres',
    'user': getpass.getuser()
}

try:
    conn = psycopg2.connect(**DB_CONFIG)
    conn.close()
    print("Conexão OK!")
except Exception as e:
    print(f"Erro: {e}")
    exit()

def gerar_logs(n=100_000):
    print(f"\nGerando {n:,} logs...")
    data = []
    start = datetime(2024, 1, 1)
    for _ in range(n):
        ts = start + timedelta(seconds=random.randint(0, 365*24*60*60))
        data.append({
            'timestamp': ts,
            'user_id': fake.uuid4(),
            'ip': fake.ipv4(),
            'pagina': random.choice(['home', 'produto', 'login', 'carrinho', 'checkout']),
            'status': random.choice([200, 404, 500, 301]),
            'tempo_resposta': round(random.uniform(0.1, 5.0), 3)
        })
    df = pd.DataFrame(data)
    df['timestamp'] = pd.to_datetime(df['timestamp'])  # Garante datetime
    df['data'] = df['timestamp'].dt.date  # Extrai a data
    df['data'] = pd.to_datetime(df['data'])  # ← AQUI ESTAVA O ERRO! Converte para datetime
    print(f"Logs gerados: {len(df):,} linhas")
    return df

def load_particionado(df):
    print("\nCriando Data Lake particionado...")
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    cursor.execute("DROP TABLE IF EXISTS logs_acesso;")
    cursor.execute("""
    CREATE TABLE logs_acesso (
        timestamp TIMESTAMPTZ,
        user_id VARCHAR(36),
        ip VARCHAR(15),
        pagina VARCHAR(20),
        status INTEGER,
        tempo_resposta FLOAT,
        data DATE
    ) PARTITION BY RANGE (data);
    """)

    meses = df['data'].dt.to_period('M').unique()
    for mes in meses:
        inicio = mes.start_time.date()
        fim = (mes + 1).start_time.date()
        nome = f"logs_{inicio.strftime('%Y_%m')}"
        cursor.execute(f"CREATE TABLE {nome} PARTITION OF logs_acesso FOR VALUES FROM ('{inicio}') TO ('{fim}');")

    print("Inserindo dados...")
    for mes in meses:
        df_part = df[df['data'].dt.to_period('M') == mes]
        if len(df_part) == 0: continue
        nome = f"logs_{mes.start_time.strftime('%Y_%m')}"
        tuples = [tuple(x) for x in df_part[['timestamp','user_id','ip','pagina','status','tempo_resposta','data']].to_numpy()]
        execute_values(cursor, f"INSERT INTO {nome} VALUES %s;", tuples)

    conn.commit()
    cursor.close()
    conn.close()
    print("Data Lake criado com sucesso!")

def analisar():
    print("\n" + "="*60)
    print("ANÁLISE DO DATA LAKE")
    print("="*60)
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    print("\nAcessos por página:")
    cursor.execute("SELECT pagina, COUNT(*) FROM logs_acesso GROUP BY pagina ORDER BY 2 DESC;")
    for row in cursor.fetchall():
        print(f"   {row[0]} → {row[1]:,} acessos")

    print("\nErros 500 (top 3):")
    cursor.execute("SELECT data, COUNT(*) FROM logs_acesso WHERE status = 500 GROUP BY data ORDER BY 2 DESC LIMIT 3;")
    for row in cursor.fetchall():
        print(f"   {row[0]} → {row[1]:,} erros")

    print("\nPartições criadas:")
    cursor.execute("SELECT tablename FROM pg_tables WHERE tablename LIKE 'logs_%' ORDER BY tablename;")
    for row in cursor.fetchall():
        print(f"   → {row[0]}")

    conn.close()

if __name__ == "__main__":
    print("INICIANDO DATA LAKE...")
    df = gerar_logs(100_000)
    load_particionado(df)
    analisar()
    print("\Pipeline concluído: Data Lake particionado e pronto para análise")
