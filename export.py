import sqlite3
import csv
import math

DB_FILE = "data/reservas.db"
CSV_FILE = "reservas_espectaculo.csv"
ASIENTOS_POR_FILA = 20  # Sincronizado con el frontend


def exportar_csv():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT session_time, owner_name, seat_number 
        FROM seats 
        WHERE status = 'reserved'
        ORDER BY session_time, owner_name
    """)

    reservas = cursor.fetchall()
    conn.close()

    mitad = ASIENTOS_POR_FILA // 2

    with open(CSV_FILE, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            ["Sesión", "Nombre y Apellidos", "Fila", "Butaca", "ID_Interno_BD"]
        )

        for session, owner, seat_id in reservas:
            fila = math.ceil(seat_id / ASIENTOS_POR_FILA)
            pos_in_row = (seat_id - 1) % ASIENTOS_POR_FILA

            if seat_id <= 220:
                fila = math.ceil(seat_id / ASIENTOS_POR_FILA)
                pos_in_row = (seat_id - 1) % ASIENTOS_POR_FILA
                if pos_in_row < mitad:
                    butaca = (mitad - pos_in_row) * 2
                else:
                    butaca = ((pos_in_row - mitad) * 2) + 1
            else:
                fila = 12
                fila12_nums = [22, 20, 18, 16, 14, 12, 10, 8, 6, 4, 2, 1, 3, 5, 7, 9, 11, 13, 15, 17, 19, 21, 23]
                butaca = fila12_nums[seat_id - 221]

            writer.writerow([session, owner, f"Fila {fila}", butaca, seat_id])

    print(f"✅ Exportadas {len(reservas)} butacas al archivo {CSV_FILE}")


if __name__ == "__main__":
    exportar_csv()
