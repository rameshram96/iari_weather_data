# 🌿 IARI Meteorological Dashboard

[![Visitors](https://visitor-badge.laobi.icu/badge?page_id=rameshram96.iari-weather-dashboard)](https://github.com/rameshram96/iari_weather_data)
[![Stars](https://img.shields.io/github/stars/rameshram96/iari-weather-dashboard?style=flat&color=3ddc84)](https://github.com/rameshram96/iari_weather_data/stargazers)
[![Data update](https://img.shields.io/badge/data%20update-daily%2009%3A00%20IST-3ddc84)](https://github.com/rameshram96/iari_weather_data/actions)

An automated dashboard that scrapes, archives, and visualises daily meteorological data from **ICAR–IARI, New Delhi** — including 5-day IMD forecasts and agronomic inference metrics (GDD, ET₀, heat stress, water balance).

🔗 **[Live Dashboard](https://rameshram96.github.io/iari_weather_data/)**

---

> **⚠️ Disclaimer**
> This project is built out of **personal academic interest** and is not affiliated with ICAR–IARI or IMD.
> Always verify data against the official source before use in research or publications:
> 📡 https://www.iari.res.in/bms/daily-weather/

---

## Stack
- **Scraper** — Python (`requests`, `beautifulsoup4`), runs daily at 09:00 IST via GitHub Actions
- **Storage** — flat CSV in this repo (Git history = audit trail)
- **Dashboard** — static HTML + Chart.js, hosted on GitHub Pages

## Quick Start
```bash
pip install -r scraper/requirements.txt
python scraper/scrape_iari.py --dry-run   # test without writing
python scraper/scrape_iari.py             # live run

*Data: IARI Agromet Observatory & IMD RMC, New Delhi.*
