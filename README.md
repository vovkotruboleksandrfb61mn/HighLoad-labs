# Проєктування високонавантажених систем: лабораторні роботи

Вовкотруб Олександр Віталійович, група ФБ-61мн
Навчально-науковий фізико-технічний інститут, кафедра інформаційної безпеки
КПІ ім. Ігоря Сікорського

Викладач: Родіонов А. М.

| № | Тема | Ноутбук | Звіт |
|---|------|---------|------|
| 1 | Веб-лічильник: пам'ять, диск, flame graph | [solution.ipynb](lab1/solution.ipynb) | [PDF](lab1/LR1_WebCounter_Vovkotrub_FB-61mn.pdf) |
| 2 | Лічильник на PostgreSQL: способи оновлення і втрата значень | [solution.ipynb](lab2/solution.ipynb) | [PDF](lab2/LR2_PostgreSQL_Vovkotrub_FB-61mn.pdf) |
| 3 | Лічильник на Hazelcast: розподілена мапа і IAtomicLong з CP Subsystem | [solution.ipynb](lab3/solution.ipynb) | [PDF](lab3/LR3_Hazelcast_Vovkotrub_FB-61mn.pdf) |
| 4 | Реплікація MongoDB: відмовостійкість і лічильник | [solution.ipynb](lab4/solution.ipynb) | [PDF](lab4/LR4_MongoDB_Vovkotrub_FB-61mn.pdf) |
| 5 | Cassandra: моделювання даних, реплікація і лічильник | [solution.ipynb](lab5/solution.ipynb) | [PDF](lab5/LR5_Cassandra_Vovkotrub_FB-61mn.pdf) |
| Adv 1 | Кешування лічильника в Hazelcast: read-through, write-through і write-behind | [solution.ipynb](adv1/solution.ipynb) | [PDF](adv1/LRAdv1_Caching_Vovkotrub_FB-61mn.pdf) |

Усі роботи будують один і той самий сервіс: HTTP-лічильник (`GET /inc`,
`GET /count`, у завданні Adv 1 ще `POST /inc?user_id=N`) на Swift 6 і SwiftNIO зі змінним сховищем (пам'ять, файл з
`fsync`, PostgreSQL, Hazelcast, MongoDB, Cassandra). Для Hazelcast і Cassandra написано власні клієнти на
Swift (Open Binary Client Protocol 2.x і CQL native protocol v4).

Ноутбуки опубліковано з результатами виконання: вони обчислюють усі таблиці
та рисунки звітів із виміряних даних і містять живу перевірку справжніх
бінарників на зменшеному навантаженні. Звіт кожної роботи оформлено як
протокол лабораторної роботи з титульною сторінкою КПІ.

## Вихідний код

| Тека | Що там |
|------|--------|
| `Sources/counter-server` | HTTP-лічильник на SwiftNIO: рушії `async`, `handler`, `raw` (власний розбір HTTP) і `uring` (io_uring) |
| `Sources/CounterCore` | сховища лічильника: пам'ять, файл з `fsync`, групування комітів, PostgreSQL, Hazelcast, MongoDB (retryable writes), Cassandra (`counter`), лічильники користувачів у PostgreSQL і в `IMap` з оптимістичним блокуванням |
| `Sources/HazelcastClient` | власний клієнт Hazelcast на Swift (Open Binary Client Protocol 2.x) |
| `Sources/CQLClient` | власний клієнт Cassandra на Swift (CQL native protocol v4, token-aware) |
| `Sources/loadgen` | генератор навантаження: N клієнтів, кожен зі своїм з'єднанням і послідовними запитами |
| `Sources/pg-bench`, `Sources/hz-bench`, `Sources/mongo-bench`, `Sources/cassandra-bench` | варіанти оновлення лічильника з робіт № 2...5 |
| `Sources/cache-check` | завдання Adv 1: порівняння `IMap` з таблицею PostgreSQL, read-through після `evictAll` |
| `Tests` | тести (swift-testing) |
| `scripts` | запуск вимірювань, PostgreSQL, кластера Hazelcast, replica set MongoDB, кластера Cassandra з розривом мережі, flame graph |
| `deploy` | конфігурація вузлів Hazelcast (разом з generic MapStore завдання Adv 1) і схема PostgreSQL |
| `reports` | виміряні дані (`data/*.csv`), flame graph'и, знімки терміналу, логи вузлів |

## Відтворення

Потрібні Swift 6, PostgreSQL 18, MongoDB 8.2 з mongosh, Cassandra 4.1 з cqlsh, Java 17+ і
[Hazelcast 5.4.0](https://repo1.maven.org/maven2/com/hazelcast/hazelcast-distribution/5.4.0/hazelcast-distribution-5.4.0.tar.gz),
розпакований у `.toolchain/hazelcast-5.4.0`. Завданню Adv 1 потрібен ще
[PostgreSQL JDBC Driver](https://jdbc.postgresql.org/): шлях до JAR у
змінній `HLS_PG_JDBC_JAR`. Для flame graph потрібні `perf` і
[FlameGraph](https://github.com/brendangregg/FlameGraph), для рушія `uring`
бібліотека liburing.

```bash
source scripts/env.sh
swift build -c release
swift test

scripts/task1.sh                 # робота № 1: пам'ять і диск, 1/2/5/10 клієнтів
scripts/pg.sh start && scripts/task2.sh && scripts/pg.sh stop
scripts/hz.sh start && scripts/task3.sh && scripts/hz.sh stop
scripts/task4-part1.sh && scripts/task4.sh   # replica set запускають і зупиняють самі
scripts/task5-part1.sh && scripts/task5-part2.sh && scripts/task5.sh   # кластер Cassandra запускають самі
scripts/cassandra.sh stop
scripts/pg.sh start && scripts/hz.sh start   # завдання Adv 1
scripts/taskadv1.sh && scripts/taskadv1x-lag.sh && scripts/taskadv1-demo.sh
```

Ноутбуки (Python 3.13):

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
jupyter notebook lab1/solution.ipynb   # або lab2/..., lab5/..., adv1/...
```

Ноутбук читає виміряні дані з `reports/data`, тож таблиці й рисунки
будуються без повторних вимірювань. Розділ «Жива перевірка» запускає зібрані
бінарники на зменшеному навантаженні; вимкнути його можна прапорцем
`RUN_LIVE = False` на початку ноутбука.
