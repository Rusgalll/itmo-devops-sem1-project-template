#!/bin/bash

set -e

if lsof -i:8080 -t >/dev/null 2>&1; then  
  PID=$(lsof -i:8080 -t)
  echo "Обнаружен процесс на порту 8080 с PID: $PID"  
  kill -9 "$PID"
  echo "Процесс успешно завершён"
fi

echo "Сборка приложения..."

if ! command -v go >/dev/null 2>&1; then  
  echo "Ошибка: Go не установлен"
  exit 1
fi

if go build -o app ./main.go; then
  echo "Сборка завершена успешно"
else
  echo "Ошибка: не удалась сборка приложения" 
  exit 1
fi