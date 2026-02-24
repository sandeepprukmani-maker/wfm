# EML Transformation Engine

## Overview
A Flask-based web application for transforming email (.eml) files using configurable rules. Users can upload email templates, define transformation rules (static, dynamic, or random), preview results, and download processed emails individually or in bulk.

## Project Architecture
- **Backend**: Python 3.11 + Flask (app.py)
- **Frontend**: Server-rendered HTML templates (Jinja2) with vanilla JavaScript
- **Data Storage**: File-based JSON storage in `samples/`, `replacement_data/`, and `lists/` directories

### Key Files
- `app.py` - Main Flask application with all API routes
- `templates/index.html` - Templates management page
- `templates/lists.html` - Lists management page
- `requirements.txt` - Python dependencies

### API Endpoints
- `GET /` - Main templates page
- `GET /lists` - Lists manager page
- `GET/POST /api/templates` - List/upload templates
- `POST /api/templates/del/<name>` - Delete template
- `GET/POST /api/templates/rules/<name>` - Get/save rules
- `GET /api/templates/preview/<name>` - Preview transformation
- `GET /api/templates/download/<name>` - Download processed email(s)
- `GET/POST /api/lists` - List/create lists
- `POST /api/lists/update/<name>` - Update list
- `POST /api/lists/del/<name>` - Delete list

## Running
- Development: `python app.py` (runs on 0.0.0.0:5000)
- Production: `gunicorn --bind=0.0.0.0:5000 --reuse-port app:app`
