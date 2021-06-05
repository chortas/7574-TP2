# 7574-TP2

## Instrucciones generales 

Para que el flujo del trabajo práctico tenga sentido primero es necesario levantar los contenedores que crean las colas. Para esto hay dos opciones:

- Si los containers no fueron cerrados correctamente en una ejecución previa -> `make restart`

- Caso contrario -> `make docker-compose-up`

Luego, para correr  el cliente que lee los archivos correr el siguiente comando:

- `./run.sh`