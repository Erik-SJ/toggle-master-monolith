import os
import click
from flask import Flask, request, jsonify
import psycopg2
from psycopg2.extras import RealDictCursor
import boto3
import json

app = Flask(__name__)

# 🔐 Nome do secret
SECRET_NAME = "db_connection"

# 🔁 Cache do secret (evita chamar AWS toda hora)
_secret_cache = None


def get_secret():
    global _secret_cache

    if _secret_cache is None:
        client = boto3.client("secretsmanager")
        response = client.get_secret_value(SecretId=SECRET_NAME)
        _secret_cache = json.loads(response["SecretString"])

    return _secret_cache


def get_db_connection():
    secret = get_secret()

    conn = psycopg2.connect(
        host=secret["DB_HOST"],
        database=secret["DB_NAME"],
        user=secret["DB_USER"],
        password=secret["DB_PASSWORD"]
    )
    return conn


# ✅ INIT DB
def init_db():
    print("Tentando inicializar a tabela 'flags'...")
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS flags (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) UNIQUE NOT NULL,
                is_enabled BOOLEAN NOT NULL DEFAULT false,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
        """)

        conn.commit()
        cur.close()
        conn.close()

        print("Tabela 'flags' inicializada com sucesso.")

    except psycopg2.OperationalError as e:
        print(f"Erro de conexão: {e}")
    except Exception as e:
        print(f"Erro inesperado: {e}")


@app.cli.command("init-db")
def init_db_command():
    init_db()


# ✅ HEALTH
@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({"status": "ok"}), 200


# ✅ CREATE FLAG
@app.route('/flags', methods=['POST'])
def create_flag():
    data = request.get_json()

    if not data or 'name' not in data:
        return jsonify({"error": "O campo 'name' é obrigatório"}), 400

    name = data['name']
    is_enabled = data.get('is_enabled', False)

    try:
        conn = get_db_connection()
        cur = conn.cursor()

        cur.execute(
            "INSERT INTO flags (name, is_enabled) VALUES (%s, %s)",
            (name, is_enabled)
        )

        conn.commit()

    except psycopg2.IntegrityError:
        return jsonify({"error": f"A flag '{name}' já existe"}), 409
    except Exception as e:
        return jsonify({"error": "Erro interno", "details": str(e)}), 500
    finally:
        if 'cur' in locals() and not cur.closed:
            cur.close()
        if 'conn' in locals() and not conn.closed:
            conn.close()

    return jsonify({"message": f"Flag '{name}' criada com sucesso"}), 201


# ✅ LIST FLAGS
@app.route('/flags', methods=['GET'])
def get_flags():
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        cur.execute("SELECT name, is_enabled FROM flags ORDER BY name")
        flags = cur.fetchall()

    except Exception as e:
        return jsonify({"error": "Erro interno", "details": str(e)}), 500
    finally:
        if 'cur' in locals() and not cur.closed:
            cur.close()
        if 'conn' in locals() and not conn.closed:
            conn.close()

    return jsonify(flags), 200


# ✅ GET FLAG
@app.route('/flags/<string:name>', methods=['GET'])
def get_flag_status(name):
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        cur.execute(
            "SELECT name, is_enabled FROM flags WHERE name = %s",
            (name,)
        )

        flag = cur.fetchone()

    except Exception as e:
        return jsonify({"error": "Erro interno", "details": str(e)}), 500
    finally:
        if 'cur' in locals() and not cur.closed:
            cur.close()
        if 'conn' in locals() and not conn.closed:
            conn.close()

    if flag:
        return jsonify(flag), 200

    return jsonify({"error": "Flag não encontrada"}), 404


# ✅ UPDATE FLAG
@app.route('/flags/<string:name>', methods=['PUT'])
def update_flag(name):
    data = request.get_json()

    if data is None or 'is_enabled' not in data or not isinstance(data['is_enabled'], bool):
        return jsonify({"error": "O campo 'is_enabled' (booleano) é obrigatório"}), 400

    is_enabled = data['is_enabled']

    try:
        conn = get_db_connection()
        cur = conn.cursor()

        cur.execute(
            "UPDATE flags SET is_enabled = %s WHERE name = %s",
            (is_enabled, name)
        )

        if cur.rowcount == 0:
            return jsonify({"error": "Flag não encontrada"}), 404

        conn.commit()

    except Exception as e:
        return jsonify({"error": "Erro interno", "details": str(e)}), 500
    finally:
        if 'cur' in locals() and not cur.closed:
            cur.close()
        if 'conn' in locals() and not conn.closed:
            conn.close()

    return jsonify({"message": f"Flag '{name}' atualizada"}), 200


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)