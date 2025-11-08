Colección de peticiones HTTP (REST Client)

Uso rápido
- Editor VS Code: instala la extensión "REST Client" (humao.rest-client).
- JetBrains (IDEA/WebStorm/PyCharm): soporta .http de forma nativa.
- Abre cualquier archivo en `request/*.http` y pulsa "Send Request" sobre el bloque deseado.

Endpoints y servicios
- Backend Node (chat, Gmail, páginas): `http://localhost:3000`
- API Flask (SQLite utilidades): `http://localhost:5000`

Consejos
- Los archivos usan URLs absolutas; su ubicación en `request/` no afecta el funcionamiento.
- Puedes definir variables al inicio de un archivo `.http`, por ejemplo:
  @base = http://localhost:3000
  GET {{base}}/healthz
- Si cambias puertos/host, ajusta las variables o las URLs en cada archivo.

Archivos incluidos
- `request/gmail.http`: flujo Gmail OAuth y envío/listado de correos.
- `request/db_sqlite3.http`: utilidades de la API Flask para SQLite.
