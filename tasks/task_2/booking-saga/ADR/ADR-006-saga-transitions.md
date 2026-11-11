# ADR-006: Состояния и переходы саги заказа и оплаты поездки

**Статус:** На рассмотрении  
**Дата:** Требует согласования

## Связанные решения

- [ADR-001: выбор саги](ADR-001-saga-pattern-selection.md) — асинхронная сага с оркестратором от заказа до оплаты.
- [ADR-002: шаги саги](ADR-002-booking-saga-steps.md) — операции, их типы и компенсации.
- [ADR-003: команды и события](ADR-003-commands-and-events.md) — сообщения между оркестратором и участниками.
- [ADR-004: топики и формат сообщений](ADR-004-kafka-topics-and-messages.md) — доставка через Kafka, повторы и DLQ.
- [ADR-005: мониторинг](ADR-005-monitoring.md) — наблюдение за состояниями, ошибками и ожиданием оплаты.
- [ADR-007: обработчики Kafka](ADR-007-kafka-consumers.md) — доставка результатов в BPMN-процесс Camunda.
- [Размещение оркестратора](ADR-orchestrator-placement.md) — предварительное решение о размещении и восстановлении оркестратора.

## Контекст и решение

Процесс BPMN в Camunda 8 Self-Managed хранит состояние саги от создания заказа до окончательной оплаты после поездки. Состояния ниже связаны с конкретными транзакциями из ADR-002 и названы по результату действия: например, после `CHECK_LOCATION` фиксируется `LOCATION_CHECKED`. Конкретная BPMN-модель, хранение результатов веток и сопоставление ответов Kafka с ожидаемыми шагами требуют проектирования и проверки.

Состояние саги не заменяет статус заказа в Booking или платежа в Payments. Подтверждение заказа не завершает сагу: она ожидает завершения поездки и оплаты. Предварительная блокировка средств, выплаты водителю, отмена подтверждённой поездки и возврат после завершённой оплаты в модель не входят и требуют отдельных решений.

**Параллельные шаги:** проверка Fraud, предварительный расчёт и поиск водителя могут выполняться одновременно, если получены необходимые данные и это разрешено бизнес-правилами. Поэтому оркестратор сохраняет **отдельное состояние каждой ветки**, а не один общий статус, который перезаписывался бы при каждом ответе. Таблицы ниже показывают переходы внутри веток; порядок прихода результатов между ветками не фиксирован. К подтверждению заказа можно перейти только после успешного завершения всех обязательных веток.

## Состояния

| Состояние | Связанная транзакция | Что значит |
|-----------|----------------------|------------|
| `BOOKING_CREATED` | `CREATE_BOOKING` | Booking создал заказ; сага начата. |
| `LOCATION_CHECKED` | `CHECK_LOCATION` | Точка подачи проверена, допустимая зона определена. |
| `LOCATION_REJECTED` | `CHECK_LOCATION` | Точка подачи или зона отклонена. |
| `PRICE_CALCULATED` | `CALCULATE_PRICE` | Предварительная стоимость рассчитана. |
| `PRICE_CALCULATION_FAILED` | `CALCULATE_PRICE` | Расчёт предварительной стоимости завершился ошибкой. |
| `BOOKING_FRAUD_CHECKED` | `CHECK_BOOKING_FRAUD` | Fraud разрешил заказ по согласованным правилам. |
| `BOOKING_FRAUD_REJECTED` | `CHECK_BOOKING_FRAUD` | Fraud отклонил заказ. |
| `DRIVER_CANDIDATES_FOUND` | `FIND_DRIVER` | Получены кандидаты для назначения. |
| `DRIVER_CANDIDATES_NOT_FOUND` | `FIND_DRIVER` | Подходящие кандидаты в текущем поиске не найдены. |
| `DRIVER_ASSIGNED` | `ASSIGN_DRIVER` | Водитель назначен заказу. |
| `DRIVER_ASSIGNMENT_FAILED` | `ASSIGN_DRIVER` | Назначить выбранного водителя не удалось. |
| `DRIVER_RELEASED` | `RELEASE_DRIVER` | Назначение данного водителя данному заказу освобождено. |
| `BOOKING_CANCELLED` | `CANCEL_BOOKING` | Booking подтвердил отмену неподтверждённого заказа. |
| `BOOKING_CONFIRMED` | `CONFIRM_BOOKING` | Booking подтвердил заказ; сага ожидает завершения поездки. |
| `TRIP_COMPLETION_WAIT_STARTED` | `WAIT_TRIP_COMPLETION` | Оркестратор сохранил ожидание события завершения поездки. |
| `TRIP_COMPLETED` | `WAIT_TRIP_COMPLETION` | Booking сообщил о подтверждённом завершении поездки. |
| `FINAL_PRICE_CALCULATED` | `CALCULATE_FINAL_PRICE` | Итоговая стоимость и валюта рассчитаны и зафиксированы для оплаты. |
| `FINAL_PRICE_CALCULATION_FAILED` | `CALCULATE_FINAL_PRICE` | Итоговую стоимость рассчитать не удалось. |
| `PAYMENT_SUCCEEDED` | `CHARGE_TRIP`, `CHECK_PAYMENT_STATUS` | Payments подтвердил успешную оплату; результат ещё нужно сохранить в Booking. |
| `PAYMENT_DECLINED` | `CHARGE_TRIP`, `CHECK_PAYMENT_STATUS` | Payments подтвердил отказ оплаты; результат ещё нужно сохранить в Booking. |
| `PAYMENT_STATUS_UNKNOWN` | `CHARGE_TRIP`, `CHECK_PAYMENT_STATUS` | Результат оплаты неизвестен; требуется сверка с провайдером. |
| `PAYMENT_STATUS_CHECKED` | `CHECK_PAYMENT_STATUS` | Проверка завершена; её результат — успех, отказ или всё ещё неизвестный статус — сохранён отдельно. |
| `PAYMENT_RESULT_RECORDED` | `RECORD_PAYMENT_RESULT` | Booking сохранил подтверждённый результат оплаты — успех или отказ. |
| `BOOKING_SUCCESS_NOTIFIED` | `NOTIFY_BOOKING_SUCCESS` | Подтверждена отправка уведомления об успешном заказе; правила подтверждения ещё согласуются. |
| `BOOKING_FAILURE_NOTIFIED` | `NOTIFY_BOOKING_FAILURE` | Подтверждена отправка уведомления об отмене заказа; правила подтверждения ещё согласуются. |
| `REVIEW_REQUIRED` | Соответствующий шаг или компенсация | Результат не удалось выяснить или обработать автоматически; требуется разбор. |
| `CANCELLED` | Итог компенсаций | Неподтверждённый заказ отменён и необходимые компенсации завершены. Конец саги. |
| `COMPLETED` | Итог оплаты | Успешная оплата подтверждена и её результат сохранён в Booking. Конец саги. |

Это предварительные имена состояний **оркестратора**, а не утверждённые статусы сервисов или названия событий Kafka. В таблице представлены результаты отдельных операций; они могут одновременно существовать у одного экземпляра саги. Неуспешные результаты и ожидание ручного разбора не считаются автоматически конечными.

## Переходы

### Создание и проверка заказа

| Исходное состояние | Следующее состояние | Событие или условие |
|--------------------|---------------------|---------------------|
| — | `BOOKING_CREATED` | `CREATE_BOOKING`: получен `BookingCreated`, оркестратор сохранил состояние саги. |
| `BOOKING_CREATED` | `LOCATION_CHECKED` | `CHECK_LOCATION`: получен `BookingLocationValidated`. |
| `BOOKING_CREATED` | `LOCATION_REJECTED` | `CHECK_LOCATION`: получен `BookingLocationRejected`; запускается отмена заказа. |
| `LOCATION_CHECKED` | `PRICE_CALCULATED` | `CALCULATE_PRICE`: получен `BookingPriceCalculated`. |
| `LOCATION_CHECKED` | `PRICE_CALCULATION_FAILED` | `CALCULATE_PRICE`: получен `BookingPriceCalculationFailed`; правила повторов и отмены согласуются. |
| `LOCATION_CHECKED` | `BOOKING_FRAUD_CHECKED` | `CHECK_BOOKING_FRAUD`: получен `BookingFraudCheckPassed`. |
| `LOCATION_CHECKED` | `BOOKING_FRAUD_REJECTED` | `CHECK_BOOKING_FRAUD`: получен `BookingFraudCheckRejected`; начинается отмена и при необходимости освобождение водителя. |
| `LOCATION_CHECKED` | `DRIVER_CANDIDATES_FOUND` | `FIND_DRIVER`: получен `DriverCandidatesFound`. |
| `LOCATION_CHECKED` | `DRIVER_CANDIDATES_NOT_FOUND` | `FIND_DRIVER`: получен `DriverCandidatesNotFound`; поиск повторяется или заказ отменяется по согласованным правилам. |
| `DRIVER_CANDIDATES_NOT_FOUND` | `DRIVER_CANDIDATES_FOUND` | Повторный `FIND_DRIVER` нашёл кандидатов. |
| `DRIVER_CANDIDATES_FOUND` | `DRIVER_ASSIGNED` | `ASSIGN_DRIVER`: получен `DriverAssigned`. |
| `DRIVER_CANDIDATES_FOUND` | `DRIVER_ASSIGNMENT_FAILED` | `ASSIGN_DRIVER`: получен `DriverAssignmentFailed`. |
| `DRIVER_ASSIGNMENT_FAILED` | `DRIVER_CANDIDATES_FOUND` | Повторный `FIND_DRIVER` нашёл кандидатов для новой попытки назначения. |
| Успешные состояния всех обязательных веток | `BOOKING_CONFIRMED` | `CONFIRM_BOOKING`: Booking подтвердил заказ событием `BookingConfirmed`. |

Строки с исходным `LOCATION_CHECKED` описывают начало **разных параллельных веток**, а не замену одного результата другим. После получения каждого ответа оркестратор сохраняет результат соответствующей ветки и проверяет, готовы ли остальные. Состав обязательных шагов уточняется.

### Ошибки и компенсации до подтверждения заказа

| Исходное состояние | Следующее состояние | Событие или условие |
|--------------------|---------------------|---------------------|
| `LOCATION_REJECTED`, `BOOKING_FRAUD_REJECTED` или окончательная ошибка обязательного шага | `BOOKING_CANCELLED` | `CANCEL_BOOKING`: Booking подтвердил отмену неподтверждённого заказа. |
| `DRIVER_ASSIGNED` | `DRIVER_RELEASED` | `RELEASE_DRIVER`: Driver подтвердил освобождение назначения этого заказа. |
| `BOOKING_CANCELLED` и, если необходимо, `DRIVER_RELEASED` | `CANCELLED` | Отмена подтверждена, все необходимые компенсации и ранее начатые действия учтены. Конец саги. |
| Любое состояние выполняемой компенсации | `REVIEW_REQUIRED` | Компенсация не выполнена после согласованных повторов или её результат неизвестен и требует разбора. |

Отмена Booking и освобождение водителя могут быть выполнены в согласованном порядке. Если после начала отмены пришёл поздний `DriverAssigned`, оркестратор должен учесть назначение и выполнить `RELEASE_DRIVER`. Сага не переходит в `CANCELLED`, пока не проверены результаты всех ранее начатых действий и необходимых компенсаций. Неизвестный результат `CONFIRM_BOOKING` сначала проверяется в Booking: отменять заказ как неподтверждённый на основании одного таймаута нельзя.

### Поездка и оплата

| Исходное состояние | Следующее состояние | Событие или условие |
|--------------------|---------------------|---------------------|
| `BOOKING_CONFIRMED` | `TRIP_COMPLETION_WAIT_STARTED` | `WAIT_TRIP_COMPLETION`: оркестратор надёжно сохранил ожидание завершения поездки. |
| `TRIP_COMPLETION_WAIT_STARTED` | `TRIP_COMPLETED` | Получен `TripCompleted` от Booking. |
| `TRIP_COMPLETED` | `FINAL_PRICE_CALCULATED` | `CALCULATE_FINAL_PRICE`: получен `FinalTripPriceCalculated`, сумма и валюта зафиксированы. |
| `TRIP_COMPLETED` | `FINAL_PRICE_CALCULATION_FAILED` | `CALCULATE_FINAL_PRICE`: получен `FinalTripPriceCalculationFailed`. |
| `FINAL_PRICE_CALCULATION_FAILED` | `FINAL_PRICE_CALCULATED` | Повторный расчёт завершился успешно. |
| `FINAL_PRICE_CALCULATION_FAILED` | `REVIEW_REQUIRED` | Согласованные попытки расчёта исчерпаны или необходим разбор. |
| `FINAL_PRICE_CALCULATED` | `PAYMENT_SUCCEEDED` | `CHARGE_TRIP`: получен подтверждённый `TripPaymentSucceeded`. |
| `FINAL_PRICE_CALCULATED` | `PAYMENT_DECLINED` | `CHARGE_TRIP`: получен подтверждённый `TripPaymentFailed`. |
| `FINAL_PRICE_CALCULATED` | `PAYMENT_STATUS_UNKNOWN` | Получен `TripPaymentStatusUnknown` или истёк срок ожидания результата оплаты. |
| `PAYMENT_STATUS_UNKNOWN` | `PAYMENT_STATUS_CHECKED` | `CHECK_PAYMENT_STATUS`: Payments завершил проверку и сохранил её результат. |
| `PAYMENT_STATUS_CHECKED` | `PAYMENT_STATUS_UNKNOWN` | Проверка не выяснила результат; сверка продолжается по согласованным правилам. |
| `PAYMENT_STATUS_CHECKED` | `PAYMENT_SUCCEEDED` | Проверка подтвердила успешную оплату. |
| `PAYMENT_STATUS_CHECKED` | `PAYMENT_DECLINED` | Проверка подтвердила отказ оплаты. |
| `PAYMENT_STATUS_UNKNOWN` | `REVIEW_REQUIRED` | За согласованное время не удалось выяснить результат или необходимо участие ответственного. |
| `PAYMENT_SUCCEEDED` или `PAYMENT_DECLINED` | `PAYMENT_RESULT_RECORDED` | `RECORD_PAYMENT_RESULT`: Booking подтвердил сохранение результата событием `TripPaymentResultRecorded`. Оркестратор сохраняет, какой именно результат записан. |
| `PAYMENT_RESULT_RECORDED` | `COMPLETED` | В Booking сохранён подтверждённый успешный платёж. Конец саги. |
| `PAYMENT_RESULT_RECORDED` | `REVIEW_REQUIRED` | Сохранён отказ оплаты, а автоматические попытки исчерпаны или необходим разбор. Дальнейшая политика согласуется. |
| `PAYMENT_RESULT_RECORDED` | `PAYMENT_SUCCEEDED`, `PAYMENT_DECLINED` или `PAYMENT_STATUS_UNKNOWN` | Сохранён отказ, правила разрешили новую попытку `CHARGE_TRIP`, и получен её результат либо результат неизвестен. Payments проверяет состояние предыдущей операции. |
| `PAYMENT_SUCCEEDED` или `PAYMENT_DECLINED` | `REVIEW_REQUIRED` | Booking не смог сохранить результат или его состояние не удалось выяснить после согласованных проверок. |

При отказе оплаты поездка остаётся завершённой и неоплаченной. Переход на новую попытку не означает автоматического или немедленного повтора: его условия определяются отдельно. При неизвестном результате повторно списывать деньги без сверки с провайдером нельзя.

### Уведомления и ручной разбор

| Исходное состояние | Следующее состояние | Событие или условие |
|--------------------|---------------------|---------------------|
| `BOOKING_CONFIRMED` | `BOOKING_SUCCESS_NOTIFIED` | `NOTIFY_BOOKING_SUCCESS`: Notification подтвердил отправку по согласованным правилам. Это отдельная ветка; сага продолжает ожидать завершения поездки. |
| `BOOKING_CANCELLED` | `BOOKING_FAILURE_NOTIFIED` | `NOTIFY_BOOKING_FAILURE`: Notification подтвердил отправку по согласованным правилам. Ошибка уведомления не повторяет отмену заказа. |
| `REVIEW_REQUIRED` | Состояние соответствующего шага | После разбора подтверждено фактическое состояние и разрешено продолжить шаг или компенсацию. Полномочия и правила требуют согласования. |

Контракты подтверждения уведомлений пока не определены в ADR-003; состояния уведомлений приведены как целевые результаты транзакций, а не как уже утверждённые события. Результат уведомления хранится отдельно и не перезаписывает состояние поездки или оплаты.

## Примеры сценариев

- **Успешный заказ и оплата:** `BOOKING_CREATED` → `LOCATION_CHECKED` → параллельные результаты `PRICE_CALCULATED`, `BOOKING_FRAUD_CHECKED`, `DRIVER_CANDIDATES_FOUND` → `DRIVER_ASSIGNED` → `BOOKING_CONFIRMED` → `TRIP_COMPLETION_WAIT_STARTED` → `TRIP_COMPLETED` → `FINAL_PRICE_CALCULATED` → `PAYMENT_SUCCEEDED` → `PAYMENT_RESULT_RECORDED` → `COMPLETED`.
- **Отказ до подтверждения:** например, `BOOKING_FRAUD_REJECTED` → `BOOKING_CANCELLED`; если водитель уже назначен — также `DRIVER_RELEASED` → после завершения всех компенсаций `CANCELLED`.
- **Неизвестный результат оплаты:** `PAYMENT_STATUS_UNKNOWN` → `PAYMENT_STATUS_CHECKED` → `PAYMENT_SUCCEEDED` → `PAYMENT_RESULT_RECORDED` → `COMPLETED`, если провайдер подтвердил успешное списание. Если результат не удалось выяснить — `REVIEW_REQUIRED`.
- **Подтверждённый отказ оплаты:** `PAYMENT_DECLINED` → `PAYMENT_RESULT_RECORDED`; поездка остаётся завершённой и неоплаченной. Повтор оплаты или завершение с ошибкой определяются отдельно.

## Открытые вопросы

- Согласовать состав и точные названия состояний, обязательные ветки и способ хранения их результатов без потери параллельных ответов.
- Определить состояния для отмены подтверждённой поездки и её связь с данной сагой; отдельный сценарий отмены пока не спроектирован.
- Согласовать условия завершения при сохранённом отказе платежа, число возможных попыток оплаты и действия при ручном разборе.
- Определить правила подтверждения уведомлений и соответствующие сообщения, если они потребуются оркестратору.
- Согласовать таймауты, длительность обычной поездки, критерии зависания и обработку поздних результатов и ошибок компенсаций.
- Уточнить контракт `TripPaymentResultRecorded` и безопасное сопоставление результатов нескольких попыток оплаты.
