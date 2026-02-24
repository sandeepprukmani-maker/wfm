# ⚡ EML Transformation Engine

A MIME-aware, rule-driven email transformation system for structured, dynamic,
and random find-and-replace operations inside raw EML content — including
journaled and encoded emails.

## Quick Start

```bash
pip install -r requirements.txt
python app.py
```

Then open http://localhost:5000

## Features

- **Template Management** — Upload raw .eml/.txt files as named templates
- **Rule Engine** — Multiple find-and-replace rules per template, applied sequentially
- **3 Replacement Modes:**
  - **Static** — Fixed replacement text
  - **Dynamic** — Built-in functions: `generate_uuid()`, `generate_random_email()`, `current_timestamp()`
  - **Random** — Randomly pulls from a named list you manage in the Lists Manager
- **Header Targeting** — Direct header replacement (FROM, TO, CC, BCC, SUBJECT)
- **Body Replacement** — Plain text + HTML body support
- **Encoding Support** — Decodes base64/quoted-printable, applies replacements, re-encodes
- **Journaled Email** — Detects `message/rfc822` wrapper, applies rules to inner message
- **Preview & Download** — See transformed output, download as `.eml`

## File Structure

```
samples/              ← Raw .eml/.txt files (one per template)
replacement_data/     ← JSON rule configs (one per template)
lists/                ← Named value pools for random mode
templates/            ← HTML UI templates
app.py                ← Flask backend
```

## Rule JSON Format

```json
[
  {
    "find_what": "FROM",
    "mode": "static",
    "replace_with": "sender@newdomain.com"
  },
  {
    "find_what": "old-placeholder@test.com",
    "mode": "dynamic",
    "replace_with": "generate_random_email"
  },
  {
    "find_what": "TO",
    "mode": "random",
    "replace_with": "my_recipient_list"
  }
]
```
