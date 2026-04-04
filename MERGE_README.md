# mahwous-smart — النسخة المدموجة (v26 + 2030)

## ما تم دمجه
| المصدر | المكونات المُضافة |
|--------|-------------------|
| mahwous2030 | القاعدة الكاملة (app.py + engines + utils + Apify + sitemap + styles) |
| mahwous-smart-v26 | mahwous_ui/ (17 صفحة UI مستقلة) + mahwous_logging.py + 9 utils إضافية |

## الملفات الجديدة المُضافة من v26
- `mahwous_ui/` — واجهة مقسّمة: sidebar_nav, dashboard, upload, review, processed, ai_page, ...
- `mahwous_logging.py` — نظام تسجيل موحّد
- `utils/analysis_sections.py`, `utils/results_io.py`, `utils/session_pickle.py`
- `utils/api_providers_ui.py`, `utils/preset_competitors.py`, `utils/filter_ui.py`, ...
- `engines/ai_engine.py` — أُضيف `USER_MSG_AI_UNAVAILABLE` و `USER_MSG_VERIFY_UNAVAILABLE`
- `audit_tools_core.py` — أدوات التدقيق

## التشغيل

### 1. ضبط المفاتيح
```bash
cp .env.example .env
# عدّل .env وأضف مفاتيحك
```

### 2. تثبيت المتطلبات
```bash
pip install -r requirements.txt
```

### 3. تشغيل التطبيق
```bash
streamlit run app.py
```

### Windows
```
START_MAHWOUS_2030.bat
```

## اختبار الاستيراد
```bash
python -c "import app; print('OK')"
```

## المتطلبات الأساسية
- Python 3.11+
- مفتاح Gemini API واحد على الأقل
