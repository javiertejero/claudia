import ws from 'k6/ws';
import { check } from 'k6';

// Configuración del escenario de carga
export const options = {
    stages: [
        { duration: '10s', target: 150 }, // Rampa de subida rápida a 150 usuarios simultáneos (worst case parece ser 243 x 3 sesiones / 6 butacas por familias = 121.5)
        { duration: '10m', target: 150 },  // Mantenemos la presión durante 10 minutos
        { duration: '30s', target: 0 },   // Rampa de bajada para cerrar limpiamente
    ],
};

// Función auxiliar para generar números aleatorios
function getRandomInt(min, max) {
    return Math.floor(Math.random() * (max - min + 1)) + min;
}

export default function () {
    // Generamos un ID único por cada Virtual User (VU) e iteración
    const clientId = `bot_${__VU}_${__ITER}`;
    const nombre = `Usuario${__VU}`;
    const apellido = `Test${__ITER}`;
    
    // IMPORTANTE: Cambia 127.0.0.1 por la IP/Dominio de tu VPS si pruebas en remoto
    // Si pruebas con Caddy (HTTPS), cambia 'ws://' por 'wss://'
    const url = `ws://127.0.0.1:8000/ws/${clientId}?nombre=${nombre}&apellido=${apellido}`;

    const res = ws.connect(url, {}, function (socket) {
        let isActive = false;
        let clicsRealizados = 0;
        
        socket.on('open', function open() {
            // Conexión establecida, esperando instrucciones del servidor
        });

        socket.on('message', function (msg) {
            const data = JSON.parse(msg);

            // 1. Control de estado (Cola vs Activo)
            if (data.type === 'status') {
                if (data.status === 'active') {
                    isActive = true;
                }
            }

            // 2. Interacción cuando somos activos y recibimos la cuadrícula
            if (data.type === 'seats_update' && isActive) {
                if (clicsRealizados < 3) {
                    // Simulamos el tiempo de reacción humana (500ms) antes de hacer clic
                    socket.setTimeout(function () {
                        const sesiones = ['11h', '12:45h', '18h'];
                        const sesionAleatoria = sesiones[getRandomInt(0, 2)];
                        // Asumimos que tienes hasta 50-60 butacas inicializadas
                        const asientoAleatorio = getRandomInt(1, 50);

                        socket.send(JSON.stringify({
                            action: "toggle",
                            seat_number: asientoAleatorio,
                            session_time: sesionAleatoria
                        }));
                        
                        clicsRealizados++;
                    }, 500);
                } else {
                    // Ya hemos elegido 3 butacas, simulamos pulsar "Finalizar Reserva"
                    socket.setTimeout(function() {
                        socket.send(JSON.stringify({ action: "finalizar" }));
                        socket.close();
                    }, 1000);
                }
            }

            // 3. Control de errores y desconexiones forzadas del servidor
            if (data.type === 'timeout' || data.type === 'duplicate' || data.type === 'error') {
                socket.close();
            }
        });

        // 4. Timeout de seguridad del script (3.5 minutos máximo en cola)
        socket.setTimeout(function () {
            socket.close();
        }, 210000); 
    });

    // Validamos que el protocolo hizo el 'Upgrade' a WebSocket correctamente (HTTP 101)
    check(res, { 'WebSocket conectado': (r) => r && r.status === 101 });
}
