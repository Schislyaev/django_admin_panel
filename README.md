# Реализация задания:

Главный модуль: postgres_to_es/etl_service.py 

Вспомогательные модули: 
*     postgres_to_es/postgresextractor.py: работа с PG
*     postgres_to_es/es_loader.py: работа с elastic
*     postgres_to_es/redis_storage.py: работа с redis

Логи: postgres_to_es/logs/

.env.example: app/config/

Версия elastic servier в докере 7.9.1
