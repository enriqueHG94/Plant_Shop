# Plant_Shop Data Warehouse Project

## Descripción
El proyecto Plant_Shop es un almacén de datos que modela y analiza las operaciones de una tienda de plantas virtual. Utiliza ELT para procesar los datos, permitiendo análisis complejos y toma de decisiones basada en datos.

## Objetivo del proyecto
el objetivo de este proyecto es imitar a mi proyecto final de el curso de ingeniería de datos que hice con Cívica Software pero con diferentes tecnologías, para ver las diferencias entre las tecnologias modern data stack que ellos nos enseñaron y las que he usado yo en este proyecto que pueden ser mas legacy o actuales.
proyecto final del curso de ingenieria de datos con tecnologias como Fivetran, Snowflake y dbt: https://github.com/enriqueHG94/curso_data_engineering

## Estructura del Proyecto
El proyecto está estructurado en la arquitectura de medallón de Lakehouse:
- **Bronze (Raw Data Zone)**: Almacena datos brutos sin procesar.
- **Silver (Cleansed Data Zone)**: Contiene datos limpios y transformados listos para ser modelados.
- **Gold (Dimensional Model Zone)**: Incluye el modelado dimensional con esquemas en estrella, optimizados para análisis.

## Casos de Uso Implementados
1. Análisis de compras de usuarios.
2. Resumen de sesiones de usuario.
3. Análisis de ventas por producto por mes.

## Tecnologías usadas
- Python 3.10.0
- PostgreSQL 16.1
- Librerías de Python: pandas 2.1.4, sqlalchemy 2.0.23

## Contacto
Email: enriquehervasguerrero@gmail.com LinkedIn: https://www.linkedin.com/in/enrique-hervas-guerrero/

