# Manual de Operación: Panel de Administración 🛡️
**Sistema de Reserva de Butacas - Torrelles en Dansa**

Este manual detalla el funcionamiento del panel de control de aforo para los administradores y taquilleros del espectáculo.

---

### 1. Acceso al Panel 🔒
* Accede a la URL `/admin`.
* Introduce el **Token de seguridad** (UUID) asignado y pulsa **Entrar al Panel**.
* *Nota: Si accedes mediante un enlace completo con el token integrado, la aplicación lo guardará y limpiará la URL automáticamente por seguridad.*

### 2. Panel de Control Superior (Acciones Rápidas) ⚡
En la cabecera del panel dispones de las siguientes herramientas:

* **🎫 Taquilla (Saltar Cola)**: Abre una pestaña de la app de reservas en modo taquilla. Te permite reservar butacas sin pasar por la cola de espera, independientemente del aforo de edición activo.
* **📋 Combinaciones**: Abre o cierra el panel de gestión de usuarios y cuotas (ver sección 4).
* **📥 Exportar CSV**: Descarga un archivo Excel/CSV con el listado detallado de todas las reservas confirmadas (con datos de sesión, usuario, fila y asiento).
* **📊 Descargar Cuotas / 📤 Cargar Cuotas**: Permite descargar la lista de cuotas actuales de todos los usuarios en un CSV, o subir un nuevo CSV para cambiar las cuotas de forma masiva.
* **🔒 Cerrar sesión**: Sale del panel administrativo y elimina las credenciales locales de este navegador.
* **⚠️ Borrar Base de Datos**: Acción crítica que elimina **todas** las reservas del sistema y expulsa a los usuarios activos. Requiere doble confirmación de seguridad.
* **Pases virtuales**: Lleva la cuenta de pases virtuales validados y permite reiniciarla (`🔄 Reset`).
* **👥 Aforo máx.**: Controla el número de personas que pueden seleccionar butacas en tiempo real simultáneamente (de 0 a 10). Los que excedan este número irán a la cola de espera.
* **📅 Día del Espectáculo (QR)**: Activa el modo especial del día del evento. Al activarlo, cambia el flujo de la aplicación de cara a la lectura de códigos QR.

### 3. Estadísticas en Tiempo Real 📈
* Muestra tarjetas con las estadísticas de ocupación de las tres sesiones (`11h`, `12:45h` y `18h`).
* Debajo, una lista detallada muestra en tiempo real las reservas confirmadas, indicando el usuario, la fila y el número exacto de butaca.
* **Monitoreo de conexiones en vivo**:
  * **Editando ahora**: Usuarios que se encuentran dentro del mapa seleccionando butacas.
  * **En cola**: Usuarios en espera de que se libere un hueco en el aforo de edición.

### 4. Gestión de Combinaciones y Cuotas 📋
Al activar el botón **Combinaciones**, se despliega la base de datos de usuarios autorizados:

* **Buscador**: Filtra por el nombre del animal o adjetivo.
* **Ajuste de Cuotas**: Cada usuario tiene asignada una cuota de butacas reservables (ej. 6). Puedes aumentarla o disminuirla en caliente haciendo clic en los botones `+` o `−`.
* **Compartir Credenciales (WhatsApp)**: Cada fila incluye un botón verde con el logotipo de WhatsApp. Al pulsarlo, abre un mensaje con el enlace de auto-login pre-rellenado para que puedas enviárselo directamente al usuario por chat:
  `http://<dominio>/?auth=<token_encriptado>`
* **Entrar como Usuario (Taquilla)**: Al pulsar sobre el nombre del usuario en el panel, se abrirá la aplicación principal de reservas logueada automáticamente con esa dupla en modo taquilla (saltándose la cola).

### 5. Advertencias de Overbooking ⚠️
* El panel cuenta con un sistema de detección de riesgos.
* Si la suma de las cuotas individuales de todos los usuarios supera el número total de butacas físicas disponibles en el teatro (729 asientos), se mostrará un banner rojo de advertencia: **"Riesgo de Overbooking"**. 
* En este caso, se recomienda ajustar a la baja las cuotas de las combinaciones que no vayan a usar todas sus butacas para eliminar el aviso.
