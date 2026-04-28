# TM Monitor Test

GitHub-only MVP для тестової перевірки нових заявок на ТМ через API СІС/УКРНОІВІ.

## Файли

- `.github/workflows/tm-monitor-test.yml` — ручний GitHub Actions workflow.
- `scripts/tm_monitor_test.mjs` — Node.js скрипт збору, нормалізації та порівняння.
- `watchlist.json` — перелік ТМ для порівняння.
- `out/` — результати після запуску workflow.

## Запуск

1. Завантажити файли в репозиторій.
2. Відкрити GitHub → Actions → TM Monitor Test → Run workflow.
3. Вказати період у форматі `дд.мм.рррр`.
4. Завантажити artifact `tm-monitor-results`.

## Результати

- `raw_applications.json` — сирі записи API.
- `normalized_applications.json` — витягнуті поля: словесні елементи, класи, заявники, зображення.
- `matches.json` — знайдені збіги.
- `matches.csv` — таблиця збігів.
- `summary.md` — коротке резюме.
