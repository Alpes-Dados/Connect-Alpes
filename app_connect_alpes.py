import calendar
import os
from datetime import date, datetime, timedelta

from flask import Flask, render_template, request, url_for
import mysql.connector

app = Flask(__name__, template_folder="templates", static_folder="static")

DB_CONFIG = {
    "host": os.getenv("ALPES_DB_HOST", "localhost"),
    "database": os.getenv("ALPES_DB_NAME", "alpes"),
    "user": os.getenv("ALPES_DB_USER", "alpesdados"),
    "password": os.getenv("ALPES_DB_PASSWORD", ""),
}


def get_db_connection():
    return mysql.connector.connect(**DB_CONFIG)


def load_filter_options():
    connection = get_db_connection()
    cursor = connection.cursor()
    cursor.execute(
        "SELECT DISTINCT cliente FROM gradeOPEC WHERE cliente IS NOT NULL AND cliente <> '' ORDER BY cliente"
    )
    clientes = [row[0] for row in cursor.fetchall()]
    cursor.execute(
        "SELECT DISTINCT emissora FROM gradeOPEC WHERE emissora IS NOT NULL AND emissora <> '' ORDER BY emissora"
    )
    emissoras = [row[0] for row in cursor.fetchall()]
    cursor.close()
    connection.close()
    return clientes, emissoras


def load_grade_pairs(selected_cliente=None, selected_emissora=None):
    connection = get_db_connection()
    cursor = connection.cursor()

    query = "SELECT cliente, emissora, COUNT(*) AS programado FROM gradeOPEC WHERE cliente IS NOT NULL AND cliente <> ''"
    params = []
    if selected_cliente:
        query += " AND cliente = %s"
        params.append(selected_cliente)
    if selected_emissora:
        query += " AND emissora = %s"
        params.append(selected_emissora)

    query += " GROUP BY cliente, emissora ORDER BY cliente, emissora"
    cursor.execute(query, tuple(params))
    grade_rows = cursor.fetchall()
    cursor.close()
    connection.close()

    return [
        {"cliente": row[0], "emissora": row[1], "programado": row[2]}
        for row in grade_rows
    ]


def load_historico(start_date, end_date, selected_cliente=None, selected_emissora=None):
    connection = get_db_connection()
    cursor = connection.cursor()

    query = """
        SELECT emiCliente, emissora, DATE(dtAcao) AS dia, COUNT(*) AS total
        FROM historico_emissoras
        WHERE dtAcao >= %s AND dtAcao < %s
    """
    params = [start_date, end_date]

    if selected_cliente:
        query += " AND emiCliente = %s"
        params.append(selected_cliente)
    if selected_emissora:
        query += " AND emissora = %s"
        params.append(selected_emissora)

    query += " GROUP BY emiCliente, emissora, DATE(dtAcao)"
    cursor.execute(query, tuple(params))
    historico_rows = cursor.fetchall()
    cursor.close()
    connection.close()

    return [
        {"cliente": row[0], "emissora": row[1], "dia": row[2].day, "total": row[3]}
        for row in historico_rows
    ]


def weekday_initial(weekday_number):
    mapping = {
        0: "S",  # Segunda
        1: "T",  # Terça
        2: "Q",  # Quarta
        3: "Q",  # Quinta
        4: "S",  # Sexta
        5: "S",  # Sábado
        6: "D",  # Domingo
    }
    return mapping.get(weekday_number, "?")


@app.route("/")
def mapa_conferencia():
    today = date.today()
    selected_year = request.args.get("year", str(today.year))
    selected_month = request.args.get("month", str(today.month))
    selected_cliente = request.args.get("cliente", "")
    selected_emissora = request.args.get("emissora", "")

    try:
        year = int(selected_year)
        month = int(selected_month)
        if month < 1 or month > 12:
            raise ValueError()
    except ValueError:
        year = today.year
        month = today.month
        selected_year = str(year)
        selected_month = str(month)

    month_days = calendar.monthrange(year, month)[1]
    starting_date = date(year, month, 1)
    ending_date = starting_date + timedelta(days=month_days)

    clientes, emissoras = load_filter_options()
    grade_pairs = load_grade_pairs(selected_cliente or None, selected_emissora or None)
    historico = load_historico(starting_date, ending_date, selected_cliente or None, selected_emissora or None)

    historico_map = {}
    historico_total = {}
    for row in historico:
        key = (row["cliente"], row["emissora"])
        item = historico_map.setdefault(key, {})
        item[row["dia"]] = row["total"]
        historico_total[key] = historico_total.get(key, 0) + row["total"]

    table_rows = []
    for pair in grade_pairs:
        key = (pair["cliente"], pair["emissora"])
        day_map = historico_map.get(key, {})
        table_rows.append(
            {
                "cliente": pair["cliente"],
                "emissora": pair["emissora"],
                "status": {
                    "programado": pair["programado"],
                    "realizado": historico_total.get(key, 0),
                },
                "days": {day: bool(day_map.get(day, 0)) for day in range(1, month_days + 1)},
            }
        )

    month_options = list(range(1, 13))
    year_options = [today.year - 1, today.year, today.year + 1]

    days = [
        {
            "day": day,
            "weekday": weekday_initial(datetime(year, month, day).weekday()),
        }
        for day in range(1, month_days + 1)
    ]

    return render_template(
        "mapa_conferencia.html",
        clientes=clientes,
        emissoras=emissoras,
        selected_cliente=selected_cliente,
        selected_emissora=selected_emissora,
        selected_year=selected_year,
        selected_month=selected_month,
        month_options=month_options,
        year_options=year_options,
        rows=table_rows,
        days=days,
    )


if __name__ == "__main__":
    app.run(debug=True, port=5001)
