Backend de chat básico (SSE)

Este backend implementa un chat muy simple usando Node.js con Server-Sent Events (SSE), sin dependencias externas. Incluye una pequeña página de prueba.

Requisitos
- Node.js 18 o superior

Ejecución
1) Inicia el servidor

   - Windows PowerShell:
     `node server/index.js`

2) Abre el cliente de prueba

   - Navega a `http://localhost:3000/` en tu navegador.

   Alternativamente, puedes abrir el archivo `public/index.html`, pero es preferible servirlo desde el backend para evitar problemas de CORS.

Endpoints
- `GET /healthz` → estado del servicio
- `GET /events` → stream SSE de mensajes
- `POST /message` → enviar mensaje `{ "user": string, "text": string }`
 - `POST /clear` → limpiar chat en todos los clientes

Notas
- Este ejemplo no persiste mensajes; todo es en memoria.
- SSE mantiene una conexión unidireccional del servidor al cliente; el envío se hace con `POST /message`.
- Si luego deseas websockets, podemos migrar fácilmente a `socket.io` o `ws`.
Frontend (GitHub Pages)
- Se añadió un frontend estático en `docs/index.html` que permite configurar la URL del backend.
- Para publicarlo con GitHub Pages:
  1) En GitHub → Settings → Pages.
  2) Source: selecciona `Deploy from a branch`.
  3) Branch: `main` y carpeta `/docs`.
  4) Guarda. La página quedará disponible en `https://<usuario>.github.io/<repo>/`.
- En la página, ajusta el campo “Backend URL” (por defecto `http://localhost:3000`) y pulsa “Conectar”.
