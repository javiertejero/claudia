import sqlite3
import csv
import math

DB_FILE = "data/reservas.db"
CSV_FILE = "reservas_espectaculo.csv"

def exportar_csv():
    # Usamos la librería estándar para no necesitar asyncio en un script síncrono
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    # Obtenemos las reservas ordenadas por sesión y nombre
    cursor.execute('''
        SELECT session_time, owner_name, seat_number 
        FROM seats 
        WHERE status = 'reserved'
        ORDER BY session_time, owner_name
    ''')
    
    reservas = cursor.fetchall()
    conn.close()

    with open(CSV_FILE, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Sesión', 'Nombre y Apellidos', 'Fila', 'Butaca', 'ID_Interno_BD'])
        
        for session, owner, seat_id in reservas:
            # La misma lógica del frontend en Python
            fila = math.ceil(seat_id / 10)
            pos_in_row = (seat_id - 1) % 10
            
            if pos_in_row < 5:
                butaca = (pos_in_row * 2) + 1
            else:
                butaca = ((pos_in_row - 5) * 2) + 2
                
            writer.writerow([session, owner, f"Fila {fila}", butaca, seat_id])

    print(f"✅ Exportadas {len(reservas)} butacas al archivo {CSV_FILE}")

if __name__ == "__main__":
    exportar_csv()