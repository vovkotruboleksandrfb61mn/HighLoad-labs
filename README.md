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

Усі три роботи будують один і той самий сервіс: HTTP-лічильник (`GET /inc`,
`GET /count`) на Swift 6 і SwiftNIO зі змінним сховищем (пам'ять, файл з
`fsync`, PostgreSQL, Hazelcast). Для Hazelcast написано власний клієнт на
Swift поверх Open Binary Client Protocol 2.x.

Ноутбуки опубліковано з результатами виконання: вони обчислюють усі таблиці
та рисунки звітів із виміряних даних і містять живу перевірку справжніх
бінарників на зменшеному навантаженні. Звіт кожної роботи оформлено як
протокол лабораторної роботи з титульною сторінкою КПІ.

## Вихідний код

| Тека | Що там |
|------|--------|
| `Sources/counter-server` | HTTP-лічильник на SwiftNIO: рушії `async`, `handler`, `raw` (власний розбір HTTP) і `uring` (io_uring) |
| `Sources/CounterCore` | сховища лічильника: пам'ять, файл з `fsync`, групування комітів, PostgreSQL, Hazelcast |
| `Sources/HazelcastClient` | власний клієнт Hazelcast на Swift (Open Binary Client Protocol 2.x) |
| `Sources/loadgen` | генератор навантаження: N клієнтів, кожен зі своїм з'єднанням і послідовними запитами |
| `Sources/pg-bench`, `Sources/hz-bench` | варіанти оновлення лічильника з робіт № 2 і № 3 |
| `Tests` | тести (swift-testing) |
| `scripts` | запуск вимірювань, PostgreSQL, кластера Hazelcast, flame graph |
| `deploy` | конфігурація вузлів Hazelcast і схема PostgreSQL |
| `reports` | виміряні дані (`data/*.csv`), flame graph'и, логи вузлів |

## Відтворення

Потрібні Swift 6, PostgreSQL 18, Java 17+ і
[Hazelcast 5.4.0](https://repo1.maven.org/maven2/com/hazelcast/hazelcast-distribution/5.4.0/hazelcast-distribution-5.4.0.tar.gz),
розпакований у `.toolchain/hazelcast-5.4.0`. Для flame graph потрібні `perf` і
[FlameGraph](https://github.com/brendangregg/FlameGraph), для рушія `uring`
бібліотека liburing.

```bash
source scripts/env.sh
swift build -c release
swift test

scripts/task1.sh                 # робота № 1: пам'ять і диск, 1/2/5/10 клієнтів
scripts/pg.sh start && scripts/task2.sh && scripts/pg.sh stop
scripts/hz.sh start && scripts/task3.sh && scripts/hz.sh stop
```

Ноутбуки (Python 3.13):

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
jupyter notebook lab1/solution.ipynb   # або lab2/..., lab3/...
```

Ноутбук читає виміряні дані з `reports/data`, тож таблиці й рисунки
будуються без повторних вимірювань. Розділ «Жива перевірка» запускає зібрані
бінарники на зменшеному навантаженні; вимкнути його можна прапорцем
`RUN_LIVE = False` на початку ноутбука.
