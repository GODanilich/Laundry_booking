# Laundry Booking System

## Состав MVP

Сервисы:

- `gateway-service` - публичный REST API и Swagger.
- `identity-service` - регистрация, логин, JWT, профиль.
- `machine-service` - стиральные машины.
- `schedule-service` - слоты и Redis locks.
- `booking-service` - бронирования и обработка payment events.
- `event-worker-service` - mock payment, mock notification, audit, analytics.

Инфраструктура:

- PostgreSQL
- Redis
- Kafka
- Jaeger

Все шесть приложений запускаются в 2 репликах. Инфраструктурные компоненты остаются в 1 реплике,так как для настоящей репликации PostgreSQL, Redis и Kafka нужна отдельная кластерная настройка, а не простая замена `replicas: 1` на `replicas: 2`.

## Быстрый запуск

Требования:

- Docker Desktop с поддержкой Swarm.
- PowerShell или bash.

Собрать образы:

```powershell
.\scripts\build-images.ps1
```

Linux/macOS:

```bash
./scripts/build-images.sh
```

Развернуть stack:

```powershell
.\scripts\deploy-stack.ps1
```

Linux/macOS:

```bash
./scripts/deploy-stack.sh
```

Проверить сервисы:

```powershell
docker stack services laundry
```

Открыть:

- Swagger: http://localhost:8080/docs
- Gateway health: http://localhost:8080/health
- Jaeger UI: http://localhost:16686

Данные администратора по умолчанию:

```text
email: admin@example.com
password: Admin123
```

## Основной demo flow

1. Войти администратором через `POST /api/v1/auth/login`.
2. Посмотреть машины через `GET /api/v1/machines`.
3. Создать слоты через `POST /api/v1/admin/slots/generate`.
4. Зарегистрировать обычного пользователя.
5. Войти пользователем и получить JWT.
6. Посмотреть свободные слоты через `GET /api/v1/slots`.
7. Создать бронирование через `POST /api/v1/bookings`.
8. Подождать 1-3 секунды, пока `event-worker-service` выполнит mock payment.
9. Проверить бронирования через `GET /api/v1/bookings/my`.
10. Проверить audit и analytics через admin endpoints.
11. Открыть Jaeger и посмотреть trace.

## Rolling update

Скрипт пересобирает только `booking-service` как `v2` и обновляет сервис через Docker Swarm без остановки всего stack:

```powershell
.\scripts\demo-rolling-update.ps1
```

Linux/macOS:

```bash
./scripts/demo-rolling-update.sh
```

Наблюдать процесс:

```powershell
docker service ps laundry_booking-service
docker stack services laundry
```

## Smoke test

Когда stack поднялся и `gateway-service` отвечает, можно прогнать основной сценарий:

```powershell
.\scripts\smoke-test.ps1
```

Linux/macOS:

```bash
./scripts/smoke-test.sh
```

Скрипт логинится администратором, создает/берет машину, генерирует слоты, регистрирует тестового пользователя, создает бронирование и проверяет audit/analytics.


## Остановка

```powershell
.\scripts\remove-stack.ps1
```

Linux/macOS:

```bash
./scripts/remove-stack.sh
```

Volumes намеренно не удаляются, чтобы данные PostgreSQL/Redis/Kafka не пропадали случайно.
