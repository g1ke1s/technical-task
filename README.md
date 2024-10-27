## Чтобы клонировать репозиторий на ваш локальный компьютер, выполните команду:
git clone https://github.com/g1ke1s/technical-task.git

cd tech-task.git

## Запуск сервера

После клонирования, запустите контейнер в Docker

docker run -d --name clickhouse-server -p 8123:8123 -p 9000:9000 yandex/clickhouse-server

Теперь запустите Clickhouse в Docker

docker exec -it clickhouse-server clickhouse-client

## Установка пакетов

pip install requirements.txt
