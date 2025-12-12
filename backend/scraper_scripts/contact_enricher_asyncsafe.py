#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
contact_enricher_asyncsafe.py
- Reads latest CSV
- Adds LinkedIn (Clearbit) if missing
- Adds Enriched Emails (Hunter + pattern guesses)
- Keeps directory/domain/email safety
- Outputs same 13 columns schema
"""
import os, re, csv, glob, json, time, logging, urllib.parse, urllib.request
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List, Tuple, Set

APP="contact_enricher_asyncsafe"
DIRECTORY_DOMAINS = {"indiamart.com","tradeindia.com","justdial.com","tiimg.com","lens.indiamart.com","3dcondl.com","exportersindia.com"}
FREE_MAIL = {"gmail.com","yahoo.com","outlook.com","hotmail.com","rediffmail.com","live.com","icloud.com","aol.com","proton.me","protonmail.com","yandex.com","zoho.com","gmx.com"}
EMAIL_RE = re.compile(r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$")

def is_valid_email(e:str)->bool: return bool(e and EMAIL_RE.match(e))

def safe_mkdir(p:str): os.makedirs(p, exist_ok=True)

def load_env(path: str):
    if not os.path.isfile(path): return
    for line in open(path,"r",encoding="utf-8"):
        s=line.strip()
        if not s or s.startswith("#") or "=" not in s: continue
        k,v=s.split("=",1); os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

def domain_of(url: str)->Optional[str]:
    try:
        host = urllib.parse.urlparse(url).netloc.lower()
        return host[4:] if host.startswith("www.") else host
    except: return None

def latest_csv(exports_dir: str)->Optional[str]:
    files = sorted(glob.glob(os.path.join(exports_dir, "b2b_leads_*.csv")))
    return files[-1] if files else None

def read_rows(p:str)->List[Dict[str,str]]:
    return list(csv.DictReader(open(p,"r",encoding="utf-8")))

def write_rows(p:str, rows: List[Dict[str,str]]):
    headers = ["ID","Company Name","Contact Person","Email","Phone","Website","Industry",
               "Location","Company Type","Source","Date","LinkedIn URL","Enriched Emails"]
    with open(p,"w",newline="",encoding="utf-8") as f:
        w=csv.DictWriter(f, fieldnames=headers); w.writeheader()
        for r in rows: w.writerow({h:r.get(h,"") for h in headers})

def http_get(url: str, headers: Optional[Dict[str,str]]=None, timeout:int=12)->Optional[bytes]:
    try:
        req = urllib.request.Request(url, headers=headers or {}, method="GET")
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            if resp.status>=400: return None
            return resp.read()
    except: return None

def hunter_emails(domain: str, limit:int=10)->List[str]:
    key=os.environ.get("HUNTER_API_KEY") or ""
    if not key: return []
    url="https://api.hunter.io/v2/domain-search?" + urllib.parse.urlencode({"domain":domain,"api_key":key,"limit":limit})
    data=http_get(url, headers={"Accept":"application/json"})
    if not data: return []
    try:
        js=json.loads(data.decode("utf-8","ignore"))
        out=[]
        for e in js.get("data",{}).get("emails",[]) or []:
            val=e.get("value")
            if val and is_valid_email(val):
                out.append(val)
        return list(dict.fromkeys(out))
    except: return []

def clearbit_linkedin(domain: str)->str:
    key=os.environ.get("CLEARBIT_API_KEY") or ""
    if not key: return ""
    url="https://company.clearbit.com/v2/companies/find?domain=" + urllib.parse.quote(domain)
    data=http_get(url, headers={"Authorization": f"Bearer {key}", "Accept":"application/json"})
    if not data: return ""
    try:
        js=json.loads(data.decode("utf-8","ignore"))
        site = js.get("site",{}) if isinstance(js,dict) else {}
        return site.get("linkedin") or ""
    except: return ""

def guess_patterns(first: str, last: str, domain: str)->List[str]:
    pats = ["{f}{last}@{d}","{first}.{last}@{d}","{first}{l}@{d}","{first}@{d}","{first}_{last}@{d}","{f}.{last}@{d}","{first}{last}@{d}"]
    first_l=(first or "").lower(); last_l=(last or "").lower()
    f=first_l[:1]; l=last_l[:1]
    outs=[]
    for p in pats:
        e=p.format(f=f,l=l,first=first_l,last=last_l,d=domain)
        if is_valid_email(e): outs.append(e)
    return list(dict.fromkeys(outs))

def split_name(name: str)->Tuple[str,str]:
    if not name: return ("","")
    name=re.sub(r"[^A-Za-z\s\.]"," ",name)
    toks=[t for t in name.split() if t.lower() not in ("mr","mrs","ms","md","dr","sir","shri","sri")]
    if not toks: return ("","")
    if len(toks)==1: return (toks[0].title(),"")
    return (toks[0].title(), toks[-1].title())

def run():
    script_dir=os.path.dirname(os.path.abspath(__file__))
    base_dir=os.path.dirname(script_dir)
    load_env(os.path.join(base_dir,".env"))
    exports=os.path.join(base_dir,"Exports"); safe_mkdir(exports)
    inp=latest_csv(exports)
    if not inp:
        print("No CSV in Exports. Run scraper first."); return
    rows=read_rows(inp)
    out=[]
    for r in rows:
        web=r.get("Website",""); dom = domain_of(web) if web else None
        li = r.get("LinkedIn URL","").strip()
        email=r.get("Email","").strip()
        enriched = [email] if email else []
        if dom and not any(dom.endswith(d) for d in DIRECTORY_DOMAINS):
            # Hunter
            for e in hunter_emails(dom, limit=10):
                if is_valid_email(e) and not any(e.lower().endswith("@"+d) for d in DIRECTORY_DOMAINS):
                    enriched.append(e)
            # Guess from contact name
            fn, ln = split_name(r.get("Contact Person","") or "")
            if fn or ln:
                for e in guess_patterns(fn, ln, dom):
                    if not any(e.lower().endswith("@"+d) for d in DIRECTORY_DOMAINS):
                        enriched.append(e)
            # LinkedIn if missing
            if not li:
                li = clearbit_linkedin(dom) or ""

        # rank enriched
        uniq=[]
        for e in enriched:
            if e and e not in uniq and is_valid_email(e): uniq.append(e)
        if dom:
            uniq = sorted(uniq, key=lambda x: (x.split("@")[-1].lower()==dom and 1 or 0, x not in FREE_MAIL), reverse=True)

        out.append({
            "ID": r.get("ID",""),
            "Company Name": r.get("Company Name",""),
            "Contact Person": r.get("Contact Person",""),
            "Email": uniq[0] if uniq else r.get("Email",""),
            "Phone": r.get("Phone",""),
            "Website": web,
            "Industry": r.get("Industry",""),
            "Location": r.get("Location",""),
            "Company Type": r.get("Company Type",""),
            "Source": r.get("Source",""),
            "Date": r.get("Date",""),
            "LinkedIn URL": li,
            "Enriched Emails": ", ".join(uniq) if uniq else (r.get("Email","") or "")
        })
    ts=datetime.now().strftime("%Y%m%d_%H%M%S")
    out_csv=os.path.join(exports, f"b2b_leads_enriched_{ts}.csv")
    write_rows(out_csv, out)
    print(f"Enriched CSV: {out_csv}")

if __name__=="__main__":
    run()
