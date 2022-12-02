# Ejemplo Pipeline de Ingesta de Datos

Para ejecutar este proyecto es necesario tener instalado al menos Python 3.9 y Poetry

- Instalar Poetry: https://python-poetry.org/docs/#installation
- Instalar dependencias: poetry install

## Estructura del proyecto

- `data`: Contiene los datos de entrada y salida del proyecto
- `exploration`: Contiene los notebooks de exploración de datos
- `apps`: Contiene el código necesario para ejecutar la aplicación

La forma sencilla de ejecutar la aplicación es mediante el comando `poetry run python apps/<application>`

## Aplicaciones

- `landing_phase`: Encargada de transformar los datos de entrada a un formato apto para manipularlos en el siguiente paso
- `transformation_phase`: Esta etapa consolida los datos de entrada y los transforma en un formato apto para ser analizados
- `mean_trips`: Servicio que permite obtener el promedio de viajes por día de la semana dado un bounding box

## Ejecución de la aplicación

- `landing_phase`: `poetry run python apps/landing_phase`
- `transformation_phase`: `poetry run python apps/transformation_phase`
- `mean_trips`: `poetry run python apps/mean_trips/main.py`

Las aplicaciones deben ser ejecutadas en el orden indicado para que el resultado sea el esperado (landing_phase -> transformation_phase -> mean_trips) esto es dado que la responsabilidad de crear las tablas necesarias para la aplicación `mean_trips` es de la aplicación `transformation_phase` y no de la aplicación `landing_phase`.
