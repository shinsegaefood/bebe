"""
🔥 베베딜 서버 v2 (Railway 배포용)
크롤링 소스: 뽐뿌, 펨코, 퀘사이존, 알리, 네이버쇼핑, 쿠팡
"""
import os, re, sqlite3, asyncio, random, logging
from datetime import datetime
from contextlib import asynccontextmanager
import httpx
from bs4 import BeautifulSoup
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.asyncio import AsyncIOScheduler

DATABASE = os.getenv("DATABASE_PATH", "bebedeal.db")
CRAWL_INTERVAL = int(os.getenv("CRAWL_INTERVAL_MINUTES", "15"))
NAVER_CLIENT_ID = os.getenv("NAVER_CLIENT_ID", "")
NAVER_CLIENT_SECRET = os.getenv("NAVER_CLIENT_SECRET", "")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger("bebedeal")

# ===== DB =====
def init_db():
    c = sqlite3.connect(DATABASE)
    c.execute("""CREATE TABLE IF NOT EXISTS deals (
        id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT NOT NULL, url TEXT UNIQUE NOT NULL,
        image_url TEXT, price INTEGER, original_price INTEGER, discount_rate INTEGER DEFAULT 0,
        domain TEXT, category TEXT, category_label TEXT, source TEXT NOT NULL, source_label TEXT,
        comments INTEGER DEFAULT 0, likes INTEGER DEFAULT 0, views INTEGER DEFAULT 0,
        is_active BOOLEAN DEFAULT 1, crawled_at DATETIME DEFAULT CURRENT_TIMESTAMP)""")
    c.execute("""CREATE TABLE IF NOT EXISTS price_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT, deal_id INTEGER NOT NULL, price INTEGER NOT NULL,
        checked_at DATETIME DEFAULT CURRENT_TIMESTAMP, FOREIGN KEY (deal_id) REFERENCES deals(id))""")
    for idx in ["domain","category","source","crawled_at","discount_rate"]:
        c.execute(f"CREATE INDEX IF NOT EXISTS idx_{idx} ON deals({idx})")
    c.commit(); c.close(); log.info("✅ DB 준비 완료")

def save_deal(deal: dict) -> bool:
    c = sqlite3.connect(DATABASE)
    try:
        row = c.execute("SELECT id, price FROM deals WHERE url=?", (deal["url"],)).fetchone()
        if row:
            did, old_p = row
            new_p = deal.get("price")
            if new_p and old_p and new_p != old_p:
                c.execute("INSERT INTO price_history (deal_id,price) VALUES (?,?)", (did, new_p))
            c.execute("""UPDATE deals SET title=?,price=?,original_price=?,discount_rate=?,
                comments=?,likes=?,views=?,is_active=1,crawled_at=CURRENT_TIMESTAMP WHERE id=?""",
                (deal.get("title"),new_p,deal.get("original_price"),deal.get("discount_rate",0),
                 deal.get("comments",0),deal.get("likes",0),deal.get("views",0),did))
            c.commit(); return False
        else:
            c.execute("""INSERT INTO deals (title,url,image_url,price,original_price,discount_rate,
                domain,category,category_label,source,source_label,comments,likes,views)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (deal.get("title"),deal["url"],deal.get("image_url"),deal.get("price"),
                 deal.get("original_price"),deal.get("discount_rate",0),deal.get("domain"),
                 deal.get("category"),deal.get("category_label"),deal["source"],
                 deal.get("source_label"),deal.get("comments",0),deal.get("likes",0),deal.get("views",0)))
            did = c.execute("SELECT last_insert_rowid()").fetchone()[0]
            if deal.get("price"):
                c.execute("INSERT INTO price_history (deal_id,price) VALUES (?,?)", (did,deal["price"]))
            c.commit(); return True
    finally: c.close()

def query_deals(domain=None,category=None,source=None,search=None,sort="latest",page=1,limit=20):
    c = sqlite3.connect(DATABASE); c.row_factory = sqlite3.Row
    w, p = ["is_active=1"], []
    if domain: w.append("domain=?"); p.append(domain)
    if category: w.append("category=?"); p.append(category)
    if source: w.append("source=?"); p.append(source)
    if search: w.append("title LIKE ?"); p.append(f"%{search}%")
    wh = " AND ".join(w)
    order = {"latest":"crawled_at DESC","discount":"discount_rate DESC","price_low":"price ASC",
             "price_high":"price DESC","popular":"likes DESC","comments":"comments DESC"}.get(sort,"crawled_at DESC")
    total = c.execute(f"SELECT COUNT(*) FROM deals WHERE {wh}",p).fetchone()[0]
    rows = c.execute(f"SELECT * FROM deals WHERE {wh} ORDER BY {order} LIMIT ? OFFSET ?",p+[limit,(page-1)*limit]).fetchall()
    deals = []
    for r in rows:
        d = dict(r)
        h = c.execute("SELECT price,checked_at FROM price_history WHERE deal_id=? ORDER BY checked_at DESC LIMIT 7",(d["id"],)).fetchall()
        d["price_history"] = [{"price":x[0],"date":x[1]} for x in h]
        deals.append(d)
    c.close()
    return {"deals":deals,"total":total,"page":page,"limit":limit,"pages":(total+limit-1)//limit}

def get_stats(domain=None):
    c = sqlite3.connect(DATABASE)
    w,p = "is_active=1",[]
    if domain: w+=" AND domain=?"; p.append(domain)
    today = datetime.now().strftime("%Y-%m-%d")
    total = c.execute(f"SELECT COUNT(*) FROM deals WHERE {w}",p).fetchone()[0]
    today_n = c.execute(f"SELECT COUNT(*) FROM deals WHERE {w} AND crawled_at>=?",p+[today]).fetchone()[0]
    avg = c.execute(f"SELECT AVG(discount_rate) FROM deals WHERE {w} AND discount_rate>0",p).fetchone()[0] or 0
    cats = c.execute(f"SELECT category,category_label,COUNT(*) FROM deals WHERE {w} AND category IS NOT NULL GROUP BY category ORDER BY 3 DESC",p).fetchall()
    srcs = c.execute(f"SELECT source,source_label,COUNT(*) FROM deals WHERE {w} GROUP BY source ORDER BY 3 DESC",p).fetchall()
    c.close()
    return {"total":total,"today":today_n,"avg_discount":round(avg),
            "categories":[{"key":x[0],"label":x[1],"count":x[2]} for x in cats],
            "sources":[{"key":x[0],"label":x[1],"count":x[2]} for x in srcs]}

# ===== 카테고리 분류 =====
CATS = {
    "stroller":{"d":"baby","l":"유모차/카시트","k":["유모차","카시트","바운서","힙시트","아기띠","웨건","맥시코시","브라이택스","싸이벡스","순성","듀클","부가부","보행기","점퍼루"]},
    "formula":{"d":"baby","l":"분유/이유식","k":["분유","이유식","젖병","수유","앱솔루트","명작","매일","남양","아이배냇","베베쿡","이유식마스터"]},
    "diaper":{"d":"baby","l":"기저귀/물티슈","k":["기저귀","물티슈","팬티기저귀","하기스","팸퍼스","마미포코","보솜이"]},
    "toy":{"d":"baby","l":"장난감/교구","k":["장난감","교구","레고","듀플로","블럭","블록","퍼즐","인형","킥보드","뽀로로","핑크퐁","놀이매트"]},
    "babygear":{"d":"baby","l":"유아용품","k":["아기옷","유아복","바디수트","턱받이","아기신발","체온계","젖병소독기","유아세제","아기로션"]},
    "soup":{"d":"food","l":"국/탕/찌개","k":["국","탕","찌개","곰탕","사골","설렁탕","갈비탕","삼계탕","닭곰탕","육개장","비비고","하림"]},
    "processed":{"d":"food","l":"가공식품","k":["라면","진라면","신라면","만두","냉동","통조림","스팸","김","반찬","밀키트","즉석밥","햄","소시지"]},
    "snack":{"d":"food","l":"간식/음료","k":["과자","초콜릿","음료","주스","커피","꼬북칩","시리얼","견과","프로틴","사탕","젤리"]},
    "fresh":{"d":"food","l":"신선식품","k":["소고기","돼지고기","닭고기","차돌박이","삼겹살","갈비","한우","과일","사과","딸기","채소","쌀","계란","연어","새우"]},
}
def classify(title):
    t=title.lower(); best,bs=None,0
    for k,v in CATS.items():
        s=sum(len(kw) for kw in v["k"] if kw in t)
        if s>bs: bs,best=s,k
    if best: return {"domain":CATS[best]["d"],"category":best,"category_label":CATS[best]["l"]}
    return {"domain":None,"category":None,"category_label":None}

# ===== 공통 유틸 =====
UAS=["Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/125.0.0.0 Safari/537.36",
     "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
     "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:126.0) Gecko/20100101 Firefox/126.0"]
def hdr(ref=None):
    h={"User-Agent":random.choice(UAS),"Accept":"text/html,application/xhtml+xml,*/*;q=0.8","Accept-Language":"ko-KR,ko;q=0.9"}
    if ref: h["Referer"]=ref
    return h
def pprice(text):
    if not text: return None
    n=re.sub(r"[^\d]","",str(text))
    return int(n) if n and 100<=int(n)<=50000000 else None
def disc(p,o):
    return round((1-p/o)*100) if p and o and o>p else 0
def title_price(t):
    m=re.search(r"(\d{1,3}[,]?\d{3,})\s*원",t)
    if m:
        v=int(m.group(1).replace(",",""))
        if 100<=v<=50000000: return v
    return None
def mkdeal(title,url,src,slabel,price=None,orig=None,img=None,cmt=0,likes=0,views=0,
           fb_domain=None,fb_cat=None,fb_cat_label=None):
    cl=classify(title)
    return {"title":title.strip(),"url":url,"image_url":img,"price":price,"original_price":orig,
            "discount_rate":disc(price,orig),
            "domain":cl["domain"] or fb_domain,"category":cl["category"] or fb_cat,
            "category_label":cl.get("category_label") or fb_cat_label,
            "source":src,"source_label":slabel,
            "comments":cmt,"likes":likes,"views":views}
async def sget(url,params=None,headers=None,timeout=15,enc=None):
    for i in range(3):
        try:
            async with httpx.AsyncClient(headers=headers or hdr(),follow_redirects=True,timeout=timeout) as c:
                r=await c.get(url,params=params)
                if enc: r.encoding=enc
                r.raise_for_status(); return r
        except:
            if i==2: raise
            await asyncio.sleep(random.uniform(1,3))

def _nums(el):
    if not el: return 0
    n=re.findall(r"\d+",el.get_text())
    return int(n[0]) if n else 0

# ===== 🕷️ 뽐뿌 =====
async def crawl_ppomppu():
    deals=[]
    for pg in range(1,4):
        try:
            r=await sget("https://www.ppomppu.co.kr/zboard/zboard.php",params={"id":"ppomppu","page":pg},enc="euc-kr")
            soup=BeautifulSoup(r.text,"lxml")
            for row in soup.select("tr.baseList,tr.baseList_bold"):
                a=row.select_one("a[href*='view.php']")
                if not a: continue
                t=a.get_text(strip=True)
                if any(x in t for x in ["[품절]","[종료]","[매진]"]): continue
                href=a.get("href","")
                url=f"https://www.ppomppu.co.kr/zboard/{href}" if not href.startswith("http") else href
                deals.append(mkdeal(t,url,"ppomppu","뽐뿌",price=title_price(t),
                    likes=_nums(row.select_one("td.baseList-rec")),cmt=_nums(row.select_one("span.baseList-c"))))
            await asyncio.sleep(random.uniform(1.5,3))
        except Exception as e: log.warning(f"뽐뿌 p{pg}: {e}")
    return deals

# ===== 🕷️ 펨코 =====
async def crawl_fmkorea():
    deals=[]
    try:
        r=await sget("https://www.fmkorea.com/hotdeal",headers=hdr("https://www.fmkorea.com/"))
        soup=BeautifulSoup(r.text,"lxml")
        for li in soup.select("li.li"):
            try:
                a=li.select_one("h3.title a, a.hx")
                if not a: continue
                t=a.get_text(strip=True)
                if not t or any(x in t for x in ["품절","종료","매진"]): continue
                href=a.get("href","")
                url=href if href.startswith("http") else f"https://www.fmkorea.com{href}"
                price_el=li.select_one(".hotdeal_info span:nth-child(2)")
                price=pprice(price_el.get_text()) if price_el else title_price(t)
                mall_el=li.select_one(".hotdeal_info span:first-child")
                mall=mall_el.get_text(strip=True) if mall_el else ""
                ft=f"[{mall}] {t}" if mall else t
                deals.append(mkdeal(ft,url,"fmkorea","펨코",price=price,
                    likes=_nums(li.select_one(".voted_count")),cmt=_nums(li.select_one(".comment_count"))))
            except: continue
    except Exception as e: log.warning(f"펨코: {e}")
    return deals

# ===== 🕷️ 퀘사이존 =====
async def crawl_quasarzone():
    deals=[]
    try:
        r=await sget("https://quasarzone.com/bbs/qb_saleinfo",headers=hdr("https://quasarzone.com/"))
        soup=BeautifulSoup(r.text,"lxml")
        for item in soup.select("div.market-info-list-cont"):
            try:
                a=item.select_one("a.subject-link, .ellipsis-with-reply-cnt a")
                if not a: continue
                t=a.get_text(strip=True)
                if not t or any(x in t for x in ["품절","종료","매진"]): continue
                href=a.get("href","")
                url=href if href.startswith("http") else f"https://quasarzone.com{href}"
                price_el=item.select_one(".market-info-sub span em")
                price=pprice(price_el.get_text()) if price_el else title_price(t)
                cat_el=item.select_one(".market-info-sub a")
                mall=cat_el.get_text(strip=True) if cat_el else ""
                ft=f"[{mall}] {t}" if mall else t
                deals.append(mkdeal(ft,url,"quasarzone","퀘사이존",price=price,
                    likes=_nums(item.select_one(".vote-num")),cmt=_nums(item.select_one(".reply-cnt .cnt"))))
            except: continue
    except Exception as e: log.warning(f"퀘사이존: {e}")
    return deals

# ===== 🕷️ 알리익스프레스 =====
async def crawl_ali():
    deals=[]
    kw_map=[
        ("baby stroller","baby","stroller","유모차/카시트"),
        ("baby diaper","baby","diaper","기저귀/물티슈"),
        ("baby bottle","baby","formula","분유/이유식"),
        ("baby toy set","baby","toy","장난감/교구"),
        ("korean ramen bulk","food","processed","가공식품"),
        ("snack box korea","food","snack","간식/음료"),
    ]
    for kw,dom,cat,cat_label in kw_map:
        try:
            r=await sget(f"https://ko.aliexpress.com/w/wholesale-{kw.replace(' ','-')}.html",
                         headers=hdr("https://ko.aliexpress.com/"),timeout=20)
            soup=BeautifulSoup(r.text,"lxml")
            for item in soup.select("[class*='card-wrapper'], [class*='gallery']")[:8]:
                try:
                    te=item.select_one("h3, [class*='titleText']")
                    if not te: continue
                    t=te.get_text(strip=True)
                    le=item.select_one("a[href]")
                    href=le.get("href","") if le else ""; 
                    url=href if href.startswith("http") else f"https://ko.aliexpress.com{href}"
                    url=url.split("?")[0]
                    pe=item.select_one("[class*='price-sale']")
                    price=pprice(pe.get_text()) if pe else None
                    oe=item.select_one("[class*='price-original']")
                    orig=pprice(oe.get_text()) if oe else None
                    ie=item.select_one("img")
                    img=ie.get("src","") if ie else None
                    if img and img.startswith("//"): img=f"https:{img}"
                    deals.append(mkdeal(f"[알리] {t}",url,"aliexpress","알리익스프레스",price=price,orig=orig,img=img,
                        fb_domain=dom,fb_cat=cat,fb_cat_label=cat_label))
                except: continue
            await asyncio.sleep(random.uniform(2,4))
        except Exception as e: log.warning(f"알리 '{kw}': {e}")
    return deals

# ===== 🕷️ 네이버쇼핑 =====
async def crawl_naver():
    if not NAVER_CLIENT_ID or NAVER_CLIENT_ID=="your_client_id_here":
        log.warning("⚠️ 네이버 API 키 미설정 → 건너뜀")
        return []
    deals=[]
    # 키워드별로 카테고리를 미리 지정
    kw_map=[
        ("유모차","baby","stroller","유모차/카시트"),
        ("카시트","baby","stroller","유모차/카시트"),
        ("아기띠 힙시트","baby","stroller","유모차/카시트"),
        ("분유","baby","formula","분유/이유식"),
        ("이유식","baby","formula","분유/이유식"),
        ("젖병","baby","formula","분유/이유식"),
        ("기저귀","baby","diaper","기저귀/물티슈"),
        ("물티슈 대량","baby","diaper","기저귀/물티슈"),
        ("아기 장난감","baby","toy","장난감/교구"),
        ("레고 듀플로","baby","toy","장난감/교구"),
        ("유아 교구","baby","toy","장난감/교구"),
        ("아기 로션","baby","babygear","유아용품"),
        ("젖병 소독기","baby","babygear","유아용품"),
        ("사골곰탕","food","soup","국/탕/찌개"),
        ("갈비탕","food","soup","국/탕/찌개"),
        ("삼계탕","food","soup","국/탕/찌개"),
        ("라면 박스","food","processed","가공식품"),
        ("만두 대용량","food","processed","가공식품"),
        ("즉석밥","food","processed","가공식품"),
        ("밀키트","food","processed","가공식품"),
        ("과자 박스","food","snack","간식/음료"),
        ("주스 대량","food","snack","간식/음료"),
        ("커피 대용량","food","snack","간식/음료"),
        ("삼겹살 특가","food","fresh","신선식품"),
        ("한우","food","fresh","신선식품"),
        ("계란 대란","food","fresh","신선식품"),
    ]
    headers={**hdr(),"X-Naver-Client-Id":NAVER_CLIENT_ID,"X-Naver-Client-Secret":NAVER_CLIENT_SECRET}
    for kw,dom,cat,cat_label in kw_map:
        try:
            r=await sget("https://openapi.naver.com/v1/search/shop.json",
                         params={"query":kw,"display":15,"sort":"date"},headers=headers)
            for item in r.json().get("items",[]):
                t=re.sub(r"<[^>]+>","",item.get("title",""))
                mall=item.get("mallName","")
                p=int(item.get("lprice",0)) or None
                hp=int(item.get("hprice",0)) or None
                title=f"[{mall}] {t}" if mall else t
                # classify 시도, 실패하면 키워드 기반 카테고리 사용
                cl=classify(title)
                d_dom=cl["domain"] or dom
                d_cat=cl["category"] or cat
                d_label=cl.get("category_label") or cat_label
                deal={"title":title.strip(),"url":item.get("link",""),"image_url":item.get("image"),
                       "price":p,"original_price":hp,"discount_rate":disc(p,hp),
                       "domain":d_dom,"category":d_cat,"category_label":d_label,
                       "source":"naver","source_label":"네이버쇼핑",
                       "comments":0,"likes":0,"views":0}
                deals.append(deal)
            await asyncio.sleep(0.3)
        except Exception as e: log.warning(f"네이버 '{kw}': {e}")
    return deals

# ===== 🕷️ 쿠팡 =====
async def crawl_coupang():
    deals=[]
    kw_map=[
        ("유모차","baby","stroller","유모차/카시트"),
        ("기저귀 대용량","baby","diaper","기저귀/물티슈"),
        ("분유","baby","formula","분유/이유식"),
        ("곰탕 대용량","food","soup","국/탕/찌개"),
        ("라면 박스","food","processed","가공식품"),
        ("과자 대용량","food","snack","간식/음료"),
    ]
    for kw,dom,cat,cat_label in kw_map:
        try:
            r=await sget("https://www.coupang.com/np/search",
                         params={"q":kw,"channel":"user"},headers=hdr("https://www.coupang.com/"))
            soup=BeautifulSoup(r.text,"lxml")
            for item in soup.select("li.search-product")[:10]:
                ne=item.select_one(".name")
                if not ne: continue
                le=item.select_one("a.search-product-link")
                href=le.get("href","") if le else ""
                url=f"https://www.coupang.com{href}" if href.startswith("/") else href
                pe=item.select_one(".price-value"); price=pprice(pe.get_text()) if pe else None
                be=item.select_one(".base-price"); orig=pprice(be.get_text()) if be else None
                ie=item.select_one("img.search-product-wrap-img")
                img=ie.get("src","") if ie else ""
                if img.startswith("//"): img=f"https:{img}"
                deals.append(mkdeal(ne.get_text(strip=True),url,"coupang","쿠팡",
                    price=price,orig=orig,img=img,cmt=_nums(item.select_one(".rating-total-count")),
                    fb_domain=dom,fb_cat=cat,fb_cat_label=cat_label))
            await asyncio.sleep(random.uniform(3,6))
        except Exception as e: log.warning(f"쿠팡 '{kw}': {e}")
    return deals

# ===== 🕷️ 카카오쇼핑 (톡딜) =====
async def crawl_kakao():
    deals=[]
    kw_map=[
        ("유모차","baby","stroller","유모차/카시트"),
        ("카시트","baby","stroller","유모차/카시트"),
        ("분유","baby","formula","분유/이유식"),
        ("기저귀","baby","diaper","기저귀/물티슈"),
        ("물티슈","baby","diaper","기저귀/물티슈"),
        ("장난감","baby","toy","장난감/교구"),
        ("곰탕","food","soup","국/탕/찌개"),
        ("라면","food","processed","가공식품"),
        ("만두","food","processed","가공식품"),
        ("과자","food","snack","간식/음료"),
        ("삼겹살","food","fresh","신선식품"),
        ("밀키트","food","processed","가공식품"),
    ]
    for kw,dom,cat,cat_label in kw_map:
        try:
            r=await sget("https://store.kakao.com/search/result",
                         params={"keyword":kw,"tab":"PRODUCT","sortType":"RECENT"},
                         headers=hdr("https://store.kakao.com/"))
            soup=BeautifulSoup(r.text,"lxml")
            for item in soup.select("[class*='product-card'], [class*='item-card'], .search-product")[:10]:
                try:
                    te=item.select_one("[class*='title'], [class*='name'], h3, .tit")
                    if not te: continue
                    t=te.get_text(strip=True)
                    if not t: continue
                    le=item.select_one("a[href]")
                    href=le.get("href","") if le else ""
                    url=href if href.startswith("http") else f"https://store.kakao.com{href}"
                    pe=item.select_one("[class*='sale-price'], [class*='discount'], .price-sale")
                    price=pprice(pe.get_text()) if pe else None
                    oe=item.select_one("[class*='origin'], [class*='before'], .price-origin")
                    orig=pprice(oe.get_text()) if oe else None
                    de=item.select_one("[class*='rate'], [class*='percent']")
                    ie=item.select_one("img")
                    img=ie.get("src","") if ie else None
                    if img and img.startswith("//"): img=f"https:{img}"
                    deals.append(mkdeal(f"[톡딜] {t}",url,"kakao","카카오쇼핑",
                        price=price,orig=orig,img=img,
                        fb_domain=dom,fb_cat=cat,fb_cat_label=cat_label))
                except: continue
            await asyncio.sleep(random.uniform(2,4))
        except Exception as e: log.warning(f"카카오 '{kw}': {e}")
    return deals

# ===== 크롤링 실행 =====
async def run_all():
    log.info("="*50)
    log.info(f"🕷️ 크롤링 시작 [{datetime.now().strftime('%H:%M:%S')}]")
    crawlers=[("뽐뿌",crawl_ppomppu),("펨코",crawl_fmkorea),("퀘사이존",crawl_quasarzone),
              ("알리",crawl_ali),("네이버쇼핑",crawl_naver),("쿠팡",crawl_coupang),("카카오쇼핑",crawl_kakao)]
    tf,tn=0,0
    for name,func in crawlers:
        try:
            deals=await func()
            new=sum(1 for d in deals if d.get("domain") and save_deal(d))
            cl=sum(1 for d in deals if d.get("domain"))
            log.info(f"  ✅ {name}: {len(deals)}개 수집 → {cl}개 분류 → {new}개 신규")
            tf+=len(deals); tn+=new
        except Exception as e: log.error(f"  ❌ {name}: {e}")
        await asyncio.sleep(2)
    log.info(f"🏁 완료! 총 {tf}개 수집, {tn}개 신규")
    log.info("="*50)
    return {"found":tf,"new":tn}

# ===== FastAPI =====
scheduler=AsyncIOScheduler()
@asynccontextmanager
async def lifespan(app:FastAPI):
    init_db()
    scheduler.add_job(run_all,"interval",minutes=CRAWL_INTERVAL)
    scheduler.start()
    log.info(f"🔥 베베딜 v2 시작! 간격:{CRAWL_INTERVAL}분 | 소스: 뽐뿌,펨코,퀘사이존,알리,네이버,쿠팡")
    await asyncio.sleep(10)
    asyncio.create_task(run_all())
    yield
    scheduler.shutdown()

app=FastAPI(title="🔥 베베딜 API v2",lifespan=lifespan)
app.add_middleware(CORSMiddleware,allow_origins=["*"],allow_credentials=True,allow_methods=["*"],allow_headers=["*"])

@app.get("/")
def root(): return {"service":"🔥 베베딜 API v2","docs":"/docs","sources":["뽐뿌","펨코","퀘사이존","알리","네이버쇼핑","쿠팡","카카오쇼핑"]}

@app.get("/api/deals")
def api_deals(domain:str=None,category:str=None,source:str=None,search:str=None,
              sort:str="latest",page:int=Query(1,ge=1),limit:int=Query(20,ge=1,le=100)):
    return query_deals(domain,category,source,search,sort,page,limit)

@app.get("/api/deals/stats")
def api_stats(domain:str=None): return get_stats(domain)

@app.get("/api/deals/{deal_id}")
def api_deal(deal_id:int):
    c=sqlite3.connect(DATABASE); c.row_factory=sqlite3.Row
    row=c.execute("SELECT * FROM deals WHERE id=?",(deal_id,)).fetchone()
    if not row: c.close(); raise HTTPException(404,"Not found")
    d=dict(row)
    h=c.execute("SELECT price,checked_at FROM price_history WHERE deal_id=? ORDER BY checked_at",(deal_id,)).fetchall()
    d["price_history"]=[{"price":x[0],"date":x[1]} for x in h]
    c.close(); return d

@app.post("/api/crawl")
async def api_crawl():
    r=await run_all(); return {"message":"완료",**r}

@app.get("/api/sources")
def api_sources():
    return {"sources":[
        {"key":"ppomppu","label":"뽐뿌","api_key":False},
        {"key":"fmkorea","label":"펨코","api_key":False},
        {"key":"quasarzone","label":"퀘사이존","api_key":False},
        {"key":"aliexpress","label":"알리익스프레스","api_key":False},
        {"key":"naver","label":"네이버쇼핑","api_key":True,"note":"NAVER_CLIENT_ID 필요"},
        {"key":"coupang","label":"쿠팡","api_key":False,"note":"봇 차단 가능성"},
        {"key":"kakao","label":"카카오쇼핑","api_key":False},
    ]}

@app.get("/api/health")
def api_health():
    try:
        c=sqlite3.connect(DATABASE)
        cnt=c.execute("SELECT COUNT(*) FROM deals WHERE is_active=1").fetchone()[0]
        srcs=c.execute("SELECT source,COUNT(*) FROM deals WHERE is_active=1 GROUP BY source").fetchall()
        c.close()
        return {"status":"ok","total_deals":cnt,"by_source":{s[0]:s[1] for s in srcs},
                "scheduler":scheduler.running,"interval":f"{CRAWL_INTERVAL}분",
                "naver_api":"설정됨" if NAVER_CLIENT_ID and NAVER_CLIENT_ID!="your_client_id_here" else "미설정"}
    except Exception as e: return {"status":"error","error":str(e)}
