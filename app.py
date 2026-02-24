from flask import Flask, request, jsonify, send_file, render_template
import os, json, uuid, random, string, re, zipfile
from email import policy
from email.parser import BytesParser
import quopri, base64
from datetime import datetime, timezone
from io import BytesIO

app = Flask(__name__)

SAMPLES_DIR = "samples"
RULES_DIR = "replacement_data"
LISTS_DIR = "lists"

for d in [SAMPLES_DIR, RULES_DIR, LISTS_DIR]:
    os.makedirs(d, exist_ok=True)

def generate_uuid(): return str(uuid.uuid4())
def generate_random_email():
    user = ''.join(random.choices(string.ascii_lowercase, k=7))
    return f"{user}@{random.choice(['example.com','testmail.org','demo.net','sample.io'])}"
def current_timestamp(): return datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S +0000")

def build_subject(all_rules=None):
    """
    Builds a subject string by scanning all configured header rules.
    Format: FROM_<replacewith>_TO_<replacewith>_CC_<replacewith>_BCC_<replacewith>
    If a header is configured   → uses its replace_with name/key (never resolved value)
    If a header is not configured → leaves empty: HEADER_
    Example: FROM_sandeep@gmail.com_TO_monitoredEmployee_list_CC__BCC_
    """
    HEADER_ORDER = ["FROM", "TO", "CC", "BCC"]
    # Build a lookup: header → replace_with key
    configured = {}
    if all_rules:
        for r in all_rules:
            fw = (r.get("find_what") or "").upper()
            if fw in HEADER_ORDER:
                configured[fw] = r.get("replace_with") or ""
    # Assemble subject parts
    parts = []
    for h in HEADER_ORDER:
        rw = configured.get(h, "")  # empty string if not configured
        parts.append(h)
        parts.append(rw)
    return "_".join(parts)

DYNAMIC_FUNCTIONS = {
    "generate_uuid": generate_uuid,
    "generate_random_email": generate_random_email,
    "current_timestamp": current_timestamp,
    "build_subject": build_subject,   # context-aware — called with all_rules
}

def load_rules(name):
    path = os.path.join(RULES_DIR, f"{name}.json")
    return json.load(open(path)) if os.path.exists(path) else []

def save_rules(name, rules):
    with open(os.path.join(RULES_DIR, f"{name}.json"), "w") as f: json.dump(rules, f, indent=2)

def load_list(name):
    path = os.path.join(LISTS_DIR, f"{name}.json")
    return json.load(open(path)) if os.path.exists(path) else []

def save_list(name, items):
    with open(os.path.join(LISTS_DIR, f"{name}.json"), "w") as f: json.dump(items, f, indent=2)

def get_replacement_value(rule, count=1, all_rules=None):
    mode, rw = rule.get("mode","static"), rule.get("replace_with","")
    if mode == "static": return rw if count==1 else [rw]*count
    if mode == "dynamic":
        fn = DYNAMIC_FUNCTIONS.get(rw)
        if fn:
            # build_subject is context-aware and needs all_rules
            import inspect
            if 'all_rules' in inspect.signature(fn).parameters:
                val = fn(all_rules=all_rules)
            else:
                val = fn()
            return val if count==1 else [fn(all_rules=all_rules) if 'all_rules' in inspect.signature(fn).parameters else fn() for _ in range(count)]
        return rw
    if mode == "random":
        items = load_list(rw)
        if not items: return rw
        return random.choice(items) if count==1 else random.sample(items, min(count, len(items)))
    return rw

HEADERS = {"FROM","TO","CC","BCC","SUBJECT"}

def apply_rule(msg, rule, all_rules=None):
    fw = rule.get("find_what","")
    fu = fw.upper()
    if fu in HEADERS:
        if fu in ("TO","CC","BCC"):
            hk = fu.capitalize()
            existing = msg.get(hk,"")
            recs = [r.strip() for r in re.split(r",\s*", existing) if r.strip()] if existing else []
            n = max(len(recs),1)
            vals = get_replacement_value(rule, n, all_rules=all_rules)
            if isinstance(vals, str): vals = [vals]*n
            del msg[hk]; msg[hk] = ", ".join(vals[:n])
        else:
            hk = "From" if fu=="FROM" else fu.capitalize()
            v = get_replacement_value(rule, all_rules=all_rules)
            if isinstance(v,list): v=v[0]
            del msg[hk]; msg[hk] = v
    else:
        for part in msg.walk():
            if part.get_content_maintype() in ("multipart","application"): continue
            enc = part.get("Content-Transfer-Encoding","").lower()
            payload = part.get_payload(decode=True)
            if payload is None: continue
            try:
                cs = part.get_content_charset() or "utf-8"
                text = payload.decode(cs, errors="replace")
            except: continue
            v = get_replacement_value(rule, all_rules=all_rules)
            if isinstance(v,list): v=v[0]
            new_text = text.replace(fw, v)
            if new_text == text: continue
            enc_bytes = new_text.encode(cs, errors="replace")
            if enc == "base64":
                part.set_payload(base64.b64encode(enc_bytes).decode("ascii"))
                part.replace_header("Content-Transfer-Encoding","base64")
            elif enc == "quoted-printable":
                part.set_payload(quopri.encodestring(enc_bytes).decode("ascii"))
                part.replace_header("Content-Transfer-Encoding","quoted-printable")
            else:
                part.set_payload(enc_bytes, charset=cs)

def process_email(raw, rules):
    msg = BytesParser(policy=policy.compat32).parsebytes(raw)
    journaled = next((p for p in msg.walk() if p.get_content_type()=="message/rfc822"), None)
    if journaled:
        inner = journaled.get_payload(decode=False)
        if isinstance(inner, list): inner_msg = inner[0]
        else: inner_msg = BytesParser(policy=policy.compat32).parsebytes(inner.encode() if isinstance(inner,str) else inner)
        for r in rules: apply_rule(inner_msg, r, all_rules=rules)
        journaled.set_payload([inner_msg])
    else:
        for r in rules: apply_rule(msg, r, all_rules=rules)
    return msg.as_bytes()

@app.route("/")
def index(): return render_template("index.html")

@app.route("/lists")
def lists_page(): return render_template("lists.html")

@app.route("/api/templates", methods=["GET"])
def list_templates():
    result = []
    for f in sorted(os.listdir(SAMPLES_DIR)):
        if f.endswith(".txt"):
            nm = f[:-4]; rules = load_rules(nm)
            result.append({"name": nm, "rule_count": len(rules), "rules": rules})
    return jsonify(result)

@app.route("/api/templates", methods=["POST"])
def upload_template():
    name = request.form.get("name","").strip()
    if not name: return jsonify({"error":"Name required"}), 400
    if not re.match(r'^[\w\-]+$', name): return jsonify({"error":"Invalid name"}), 400
    f = request.files.get("file")
    if not f: return jsonify({"error":"File required"}), 400
    with open(os.path.join(SAMPLES_DIR, f"{name}.txt"), "wb") as fh: fh.write(f.read())
    if not os.path.exists(os.path.join(RULES_DIR, f"{name}.json")): save_rules(name, [])
    return jsonify({"success": True, "name": name})

@app.route("/api/templates/del/<name>", methods=["POST"])
def delete_template(name):
    for p in [os.path.join(SAMPLES_DIR,f"{name}.txt"), os.path.join(RULES_DIR,f"{name}.json")]:
        if os.path.exists(p): os.remove(p)
    return jsonify({"success": True})

@app.route("/api/templates/rules/<name>", methods=["GET"])
def get_rules(name):
    return jsonify(load_rules(name))

@app.route("/api/templates/rules/<name>", methods=["POST"])
def save_rules_api(name):
    save_rules(name, request.json)
    return jsonify({"success": True})

@app.route("/api/templates/preview/<name>", methods=["GET"])
def preview(name):
    sp = os.path.join(SAMPLES_DIR, f"{name}.txt")
    if not os.path.exists(sp): return jsonify({"error":"Not found"}), 404
    with open(sp,"rb") as f: raw = f.read()
    result = process_email(raw, load_rules(name))
    return jsonify({"content": result.decode("utf-8", errors="replace")})

@app.route("/api/templates/download/<name>", methods=["GET"])
def download(name):
    count = max(1, min(int(request.args.get("count",1)), 500))
    sp = os.path.join(SAMPLES_DIR, f"{name}.txt")
    if not os.path.exists(sp): return jsonify({"error":"Not found"}), 404
    with open(sp,"rb") as f: raw = f.read()
    rules = load_rules(name)
    if count == 1:
        buf = BytesIO(process_email(raw, rules)); buf.seek(0)
        return send_file(buf, as_attachment=True, download_name=f"{name}_001.txt", mimetype="text/plain")
    zip_buf = BytesIO()
    with zipfile.ZipFile(zip_buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for i in range(1, count+1): zf.writestr(f"{name}_{i:04d}.txt", process_email(raw, rules))
    zip_buf.seek(0)
    return send_file(zip_buf, as_attachment=True, download_name=f"{name}_{count}_samples.zip", mimetype="application/zip")

@app.route("/api/lists", methods=["GET"])
def get_lists():
    result = []
    for f in sorted(os.listdir(LISTS_DIR)):
        if f.endswith(".json"):
            nm=f[:-5]; items=load_list(nm); result.append({"name":nm,"count":len(items),"items":items})
    return jsonify(result)

@app.route("/api/lists", methods=["POST"])
def create_list():
    data = request.json; name = data.get("name","").strip()
    if not name or not re.match(r'^[\w\-]+$', name): return jsonify({"error":"Invalid name"}), 400
    save_list(name, data.get("items",[])); return jsonify({"success":True})

@app.route("/api/lists/update/<name>", methods=["POST"])
def update_list(name):
    save_list(name, request.json.get("items",[])); return jsonify({"success":True})

@app.route("/api/lists/del/<name>", methods=["POST"])
def delete_list(name):
    p = os.path.join(LISTS_DIR, f"{name}.json")
    if os.path.exists(p): os.remove(p)
    return jsonify({"success":True})

if __name__ == "__main__":
    app.run(debug=True, port=5000)
