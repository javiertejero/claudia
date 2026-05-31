import sqlite3
import csv
import math

DB_FILE = "data/reservas.db"
CSV_FILE = "reservas_espectaculo.csv"
ASIENTOS_POR_FILA = 20 # Sincronizado con el frontend

def exportar_csv():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT session_time, owner_name, seat_number 
        FROM seats 
        WHERE status = 'reserved'
        ORDER BY session_time, owner_name
    ''')
    
    reservas = cursor.fetchall()
    conn.close()

    mitad = ASIENTOS_POR_FILA // 2

    with open(CSV_FILE, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Sesión', 'Nombre y Apellidos', 'Fila', 'Butaca', 'ID_Interno_BD'])
        
        for session, owner, seat_id in reservas:
            fila = math.ceil(seat_id / ASIENTOS_POR_FILA)
            pos_in_row = (seat_id - 1) % ASIENTOS_POR_FILA
            
            if pos_in_row < mitad:
                butaca = (pos_in_row * 2) + 1
            else:
                butaca = ((pos_in_row - mitad) * 2) + 2
                
            writer.writerow([session, owner, f"Fila {fila}", butaca, seat_id])

    print(f"✅ Exportadas {len(reservas)} butacas al archivo {CSV_FILE}")

if __name__ == "__main__":
    exportar_csv()