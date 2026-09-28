# Kafka Task: Producer + Single/Batch Consumers

Учебный проект на Java, демонстрирующий работу с Apache Kafka 3.x:
- продюсер с гарантией доставки **At Least Once**;
- два независимых консьюмера: **SingleMessageConsumer** (по одному сообщению, auto-commit) и **BatchMessageConsumer** (пачками ≥10, ручной commit);
- кластер Kafka из **3 брокеров** в KRaft-режиме, поднимается через Docker Compose;
- топик создаётся **вручную** из консоли;


## Описание классов

### `App.java` — точка входа

**Назначение:** запускает три независимых потока и ждёт их завершения.

- Читает переменные окружения `KAFKA_BOOTSTRAP` и `TOPIC`. Если они не заданы, использует значения по умолчанию:
    - `KAFKA_BOOTSTRAP = localhost:9094,localhost:9095,localhost:9096` ;
    - `TOPIC = MyTopic`.
- Определяет `instanceId` (из `HOSTNAME` или PID процесса) — используется в логах, чтобы отличать запуски.
- Создаёт и стартует три потока:
    - `producer` → `ProducerRunner`;
    - `single-consumer` → `SingleMessageConsumer`;
    - `batch-consumer` → `BatchMessageConsumer`.
- Вызывает `join()` для каждого потока, чтобы процесс не завершился.

---

### `Message.java` — модель сообщения

**Назначение:** POJO для сериализации/десериализации в JSON через Jackson.

| Поле               | Тип      | Описание                        |
|--------------------|----------|---------------------------------|
| `id`               | `long`   | Порядковый номер сообщения      |
| `payload`          | `String` | Полезная нагрузка               |
| `producerInstance` | `String` | ID экземпляра продюсера         |
| `timestamp`        | `long`   | Время создания (epoch millis)   |


---

### `ProducerRunner.java` — продюсер

**Назначение:** асинхронно отправляет JSON-сообщения в топик каждые 500 мс.

**Ключевые настройки:**

```java
props.put(ProducerConfig.ACKS_CONFIG, "all");                    // ждём все ISR
props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);     // ретраить бесконечно
props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120_000);   // общий таймаут
props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);       // защита от дублей

```

Логика:
Продюсер создает сообщение, сериализует его в Json, асинхронно отправляет.
Логируется сообщение перед отправкой, подтверждение от партиций, при ошибке сериализации сообщения.

---

### `SingleMessageConsumer.java` — консьюмер по одному сообщению
Читает по одному сообщению за poll(), коммитит оффсет автоматически.

**Ключевые настройки:**
```java
props.put(ConsumerConfig.GROUP_ID_CONFIG, "single-consumer-group"); // уникальная группа
props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);          // авто-коммит
props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000);     // каждую секунду
props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);               // по одному
props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");     // с начала
```
Логика:
Консьюмер подписывается на топик MyTopic, читакт сообщения в цикле. Для каждого сообщения выполняет десериализацию JSON и записывает сообщение в лог.
Коммит происходит автоматический (Kafka коммитит раз в секунду).

---
### `BatchMessageConsumer.java` — консьюмер пачками
Читает пачки до 10 сообщений за poll(), обрабатывает в цикле, коммитит оффсет вручную один раз на пачку.

Ключевые настройки:
```java
props.put(ConsumerConfig.GROUP_ID_CONFIG, "batch-consumer-group");  // другая группа
props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);         // ручной коммит
props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10);              // до 10 за poll
props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 1500);             // минимум байт
props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 1000);           // максимум ожидания
```

Подбор параметров fetch.min.bytes и fetch.max.wait.ms:

fetch.min.bytes=1500 - брокер не отдаст ответ, пока не накопит ~1500 байт (≈10 JSON-сообщений);
fetch.max.wait.ms=1000 - но не будет ждать дольше 1 секунды, даже если данных меньше.

Логика:

Вызов poll() с таймаутом 2 секунды. Если пачка пустая — повторная попытка.
Если нет - обрабатывает все записи в цикле (десериализация + логирование).
После успешной обработки один вызов consumer.commitSync() на всю пачку
