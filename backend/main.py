import os
import sys
import smtplib
from email.mime.text import MIMEText
import psycopg2
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

def get_db_conn():
    url = os.getenv("DATABASE_URL")
    return psycopg2.connect(url)

def fetch_sites(conn, site_id=None):
    cur = conn.cursor()
    if site_id:
        cur.execute("SELECT id, name, url, keywords FROM sites WHERE id=%s", (site_id,))
    else:
        cur.execute("SELECT id, name, url, keywords FROM sites")
    rows = cur.fetchall()
    cur.close()
    return rows

def parse_entries(html, base_url, keywords):
    soup = BeautifulSoup(html, "html.parser")
    kws = []
    if keywords:
        kws = [k.strip().lower() for k in keywords.split(',') if k.strip()]
    results = []
    for a in soup.find_all('a'):
        text = (a.get_text() or '').strip()
        href = a.get('href') or ''
        if not href:
            continue
        link = urljoin(base_url, href)
        if kws:
            t = text.lower()
            if not any(k in t for k in kws):
                continue
        if text:
            results.append({"title": text, "link": link})
    return results

def insert_if_new(conn, site_name, title, link):
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM postings WHERE site_name=%s AND title=%s AND link=%s", (site_name, title, link))
    exists = cur.fetchone()
    if not exists:
        cur.execute("INSERT INTO postings (title, site_name, link) VALUES (%s, %s, %s) RETURNING found_at", (title, site_name, link))
        found_at = cur.fetchone()[0]
        cur.close()
        return True, found_at
    cur.close()
    return False, None

def send_email(new_items):
    host = os.getenv("SMTP_HOST")
    port = int(os.getenv("SMTP_PORT", "587"))
    user = os.getenv("SMTP_USER")
    password = os.getenv("SMTP_PASS")
    sender = os.getenv("EMAIL_FROM")
    recipients = os.getenv("EMAIL_TO", "").split(',')
    if not host or not sender or not recipients or not any(r.strip() for r in recipients):
        return
    lines = []
    for item in new_items:
        lines.append(f"Site: {item['site_name']}\nTitle: {item['title']}\nFound: {item.get('found_at')}\nURL: {item['link']}\n")
    body = "\n\n".join(lines) if lines else ""
    if not body:
        return
    msg = MIMEText(body)
    msg['Subject'] = 'New scraped entries'
    msg['From'] = sender
    msg['To'] = ", ".join([r.strip() for r in recipients if r.strip()])
    with smtplib.SMTP(host, port) as server:
        server.starttls()
        if user and password:
            server.login(user, password)
        server.sendmail(sender, [r.strip() for r in recipients if r.strip()], msg.as_string())

def run(site_id=None):
    conn = get_db_conn()
    new_items = []
    try:
        sites = fetch_sites(conn, site_id)
        for s in sites:
            sid, name, url, keywords = s
            try:
                resp = requests.get(url, timeout=30)
                resp.raise_for_status()
                entries = parse_entries(resp.text, url, keywords)
                for e in entries:
                    inserted, ts = insert_if_new(conn, name, e['title'], e['link'])
                    if inserted:
                        new_items.append({"site_name": name, "title": e['title'], "link": e['link'], "found_at": ts})
                conn.commit()
            except Exception:
                conn.rollback()
        if new_items:
            send_email(new_items)
    finally:
        conn.close()

if __name__ == "__main__":
    sid = None
    if len(sys.argv) > 1 and sys.argv[1].isdigit():
        sid = int(sys.argv[1])
    run(sid)