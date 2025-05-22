# Crypto Data Microservice

Мікросервіс для отримання та обробки криптовалютних даних з бірж. Реалізований на Python з використанням FastAPI та CCXT.

## Особливості

- **Підтримка бірж**: Binance, Bybit, WhiteBit
- **REST API**: Отримання тікерів, OHLCV даних, ордербуків
- **WebSocket**: Реального часу потоки даних
- **Управління підключеннями**: Автоматичне перепідключення та обробка помилок
- **Rate Limiting**: Контроль лімітів API запитів
- **Моніторинг**: Prometheus метрики та Grafana дашборди
- **Контейнеризація**: Docker та Docker Compose

## Швидкий старт

### 1. Клонування репозиторію

```bash
git clone <repository-url>
cd crypto-microservice
```

### 2. Налаштування оточення

```bash
cp .env.example .env
# Відредагуйте .env файл з вашими налаштуваннями
```

### 3. Запуск через Docker Compose

```bash
docker-compose up -d
```

### 4. Або запуск в режимі розробки

```bash
# Встановлення залежностей
pip install -r requirements.txt

# Запуск сервісу
python -m app.main
```

## API Документація

Після запуску сервісу, API документація доступна за адресами:
- Swagger UI: http://localhost:8000/docs
- ReDoc: http://localhost:8000/redoc

### Основні ендпоінти

#### Інформація про біржі
- `GET /api/v1/exchanges` - Список підтримуваних бірж
- `GET /api/v1/exchanges/{exchange_id}/info` - Інформація про біржу
- `GET /api/v1/exchanges/{exchange_id}/status` - Статус підключення
- `GET /api/v1/exchanges/status` - Статус всіх бірж

#### Верифікація апі ключа
- `POST /api/v1/exchanges/{exchange_id}/verify` - Верифікація credentials

#### Ринкові дані
- `GET /api/v1/exchanges/{exchange_id}/ticker/{symbol}` - Тікер
- `GET /api/v1/exchanges/{exchange_id}/ohlcv/{symbol}` - OHLCV дані
- `GET /api/v1/exchanges/{exchange_id}/orderbook/{symbol}` - Ордербук
- `GET /api/v1/exchanges/{id}/timeframes` - Таймфрейми
- `GET /api/v1/exchanges/{id}/markets` - Ринки

#### WebSocket підписки
- `POST /api/v1/exchanges/{exchange_id}/subscribe` - Підписка на потік
- `DELETE /api/v1/exchanges/{exchange_id}/subscribe/{sub_id}` - Відписка

## Архітектура

```
├── app/
│   ├── api/                    # API маршрути
│   │   └── routes/
│   ├── exchanges/              # Адаптери для бірж
│   │   ├── base.py            # Базові класи
│   │   ├── binance.py         # Binance адаптер
│   │   ├── bybit.py           # Bybit адаптер
│   │   ├── whitebit.py        # WhiteBit адаптер
│   │   ├── factory.py         # Фабрика адаптерів
│   │   └── manager.py         # Менеджер підключень
│   ├── utils/                  # Утиліти
│   ├── config.py              # Конфігурація
│   └── main.py                # Точка входу
├── tests/                     # Тести
├── monitoring/                # Моніторинг конфігурації
├── docker-compose.yml         # Docker Compose
├── Dockerfile                 # Docker образ
└── requirements.txt           # Python залежності
```

## Модулі системи

### 1. Модуль підключення до бірж ✅ ЗАВЕРШЕНО
- [x] Базові адаптери для REST та WebSocket API
- [x] Адаптери для Binance, Bybit, WhiteBit
- [x] Управління підключеннями та обробка помилок
- [x] Rate limiting та автоматичне перепідключення
- [x] Фабрика та менеджер адаптерів
- [x] REST API ендпоінти для тестування

### 2. Модуль WebSocket для реального часу (Наступний)
- [ ] Буферизація та агрегація даних
- [ ] Підписка на множинні потоки
- [ ] Нормалізація повідомлень
- [ ] Push сповіщення до основного сервісу

### 3. Модуль отримання історичних даних
- [ ] Планувальник завдань
- [ ] Інкрементальне оновлення
- [ ] Пріоритезація запитів
- [ ] Механізм "догону"

### 4. Модуль зберігання та обробки даних
- [ ] TimescaleDB інтеграція
- [ ] Агрегація даних
- [ ] Очищення та валідація
- [ ] Архівування старих даних

### 5. Модуль комунікації з основним сервісом
- [ ] RabbitMQ інтеграція
- [ ] WebSocket сервер
- [ ] Підтвердження доставки
- [ ] Стиснення даних

### 6. Модуль кешування та оптимізації
- [ ] Redis інтеграція
- [ ] TTL управління
- [ ] Batch запити
- [ ] Адаптивне регулювання

### 7. Модуль управління та моніторингу
- [ ] Веб-інтерфейс
- [ ] Prometheus метрики
- [ ] Grafana дашборди
- [ ] Система оповіщень

## Тестування

```bash
# Запуск тестів
python -m pytest tests/

# Запуск з покриттям
python -m pytest tests/ --cov=app --cov-report=html
```

## Моніторинг

Після запуску Docker Compose, доступні:
- Prometheus: http://localhost:9090
- Grafana: http://localhost:3000 (admin/admin)
- RabbitMQ Management: http://localhost:15672 (guest/guest)

## Розробка

### Додавання нової біржі

1. Створіть новий адаптер в `app/exchanges/`
2. Наслідуйте від `RestExchangeAdapter` та `WebSocketExchangeAdapter`
3. Реалізуйте абстрактні методи
4. Додайте до `ExchangeFactory._adapters`
5. Додайте тести

### Структура повідомлень

Всі WebSocket повідомлення нормалізуються до стандартного формату:

```json
{
  "type": "ticker|orderbook|kline",
  "exchange": "binance|bybit|whitebit",
  "symbol": "BTC/USDT",
  "data": {
    // Нормалізовані дані
  }
}
```

## Сумісність з основним сервісом

Мікросервіс повністю сумісний зі структурами даних Laravel сервісу:
- Моделі Exchange, TradingPair, HistoricalData
- Формати API відповідей
- WebSocket схеми подій
- Часові таймфрейми

## Ліцензія

MIT License
