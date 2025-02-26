#!/bin/bash

set -e

# Загрузка переменных из переменного окржуения для инициализации
if [ -f .env ]; then
    export $(cat -v '^#' .env | xargs)
else
    echo "Ошибка: не найден .env файл"
    exit 1
fi

#PostgreSQL
if ! command -v psql &> /dev/null
then
    echo "Ошибка: необходимо установить PostgreSQL"
    exit 1
fi

echo "Создание таблицы $DB_TABLE_NAMEE в базе данных $DB_NAME..."

PGPASSWORD=$DB_PASSWORD psql -U $DB_USER_NAME -h $DB_HOST -p $DB_PORT -d $DB_NAME -c "
CREATE TABLE IF NOT EXISTS $DB_TABLE_NAME (
    id SERIAL PRIMARY KEY,           -- Автоматически увеличиваемый идентификатор
    created_at DATE NOT NULL,        -- Дата создания продукта
    name VARCHAR(255) NOT NULL,      -- Название продукта
    category VARCHAR(255) NOT NULL,  -- Категория продукта
    price DECIMAL(10, 2) NOT NULL    -- Цена продукта с точностью до 2 знаков после запятой
);"

echo "База данных инициалзирована успешно"

#Go
echo "Устанавка Go зависимостей..."
go mod tidy
echo "Go Зависимости установлены"