"""NAVER API HUB discovery + public source text. No generated POI content."""
import argparse,csv,getpass,hashlib,html,json,os,re,sqlite3,time
import threading
from concurrent.futures import ThreadPoolExecutor,wait,FIRST_COMPLETED
from pathlib import Path
from urllib.request import Request,urlopen
from urllib.parse import urlencode,urlsplit,urlunsplit,parse_qs
from urllib.error import HTTPError,URLError
from jeju_free import Fetcher,street_match,norm

BASE='https://naverapihub.apigw.ntruss.com/search/v1/'
VERSION='api-research-v2.2'
def clean(s): return re.sub(r'\s+',' ',html.unescape(re.sub('<[^>]+>',' ',s or ''))).strip()
def terms(row):
    c=row.get('place_sub_category','')
    groups=[('공방|체험학습','원데이클래스','체험 프로그램'),('서핑|스쿠버|다이빙|수상|요트|승마|낚시','체험 프로그램','예약 이용방법'),('렌터카|대여','대여 서비스','인수 반납'),('기념품|빈티지|서점|쇼핑','판매 상품','매장 소개'),('미용|네일|피부','시술 서비스','예약'),('사우나|찜질|목욕','시설 이용','이용요금'),('보관|택배','이용방법','보관 배송 요금'),('세탁','세탁 서비스','이용방법'),('여행|버스','여행 상품','예약 서비스')]
    for pattern,a,b in groups:
        if re.search(pattern,c): return [a,b,'영업시간 휴무 주차']
    return ['서비스 소개','이용방법','영업시간 휴무 주차']

def queries(row):
    name=clean(row['name']); address=row.get('road_address') or row.get('jibun_address','')
    city='서귀포시' if '서귀포시' in address else '제주시'
    area=re.search(r'([가-힣0-9]+(?:읍|면|동))\s',row.get('jibun_address','')+' ')
    area=area.group(1) if area else city
    short=re.sub(r'\s+제주\d*호점$|\s+제주연동점$','',name).strip()
    return list(dict.fromkeys([f'{name} {city}',f'{short} {area}',f'{short} 제주',*[f'{short} {area} {term.split()[0]}' for term in terms(row)]]))

def identity(row,text):
    lines=[x.strip() for x in text.splitlines() if x.strip()]
    name=norm(row['name']); addresses=[row.get(k,'') for k in ('road_address','jibun_address') if row.get(k,'')]
    for i,line in enumerate(lines):
        if name and name in norm(line):
            passage=' '.join(lines[max(0,i-6):i+7])
            if any(norm(a) in norm(passage) or street_match(a,passage) for a in addresses): return '상호주소근접일치'
    return '상호만일치' if name and name in norm(text) else '상호미확인'

def public_blog_url(url):
    p=urlsplit(url)
    if p.hostname=='blog.naver.com':
        q=parse_qs(p.query)
        if q.get('blogId') and q.get('logNo'):
            return 'https://m.blog.naver.com/'+q['blogId'][0]+'/'+q['logNo'][0]
        if re.fullmatch(r'/[^/]+/\d+',p.path): return urlunsplit(('https','m.blog.naver.com',p.path,'',''))
    return url

class API:
    def __init__(self,db,cid,secret,budget):
        self.db,self.cid,self.secret,self.budget=db,cid,secret,budget
        self.calls=0;self.last=0
        self.lock=threading.Lock();self.stop=threading.Event()
        self.rate_lock=threading.Lock();self.key_guard=threading.Lock();self.key_locks={}
        self.cooldown=0.0
    def reserve_request(self):
        while True:
            if self.stop.is_set():raise RuntimeError('중단 요청됨')
            with self.rate_lock:
                now=time.monotonic()
                delay=max(self.last+0.25,self.cooldown)-now
                if delay<=0:
                    if self.calls>=self.budget:
                        self.stop.set()
                        raise RuntimeError('이번 실행 API 호출 한도 도달. 같은 명령으로 재개 가능')
                    self.calls+=1;self.last=now
                    return
            if self.stop.wait(delay):raise RuntimeError('중단 요청됨')
    def search(self,section,query):
        key=section+'|'+query
        with self.key_guard:query_lock=self.key_locks.setdefault(key,threading.Lock())
        with query_lock:
            if self.stop.is_set():raise RuntimeError('중단 요청됨')
            try:return self._search(section,query)
            except Exception:
                self.stop.set();raise
    def _search(self,section,query):
        key=section+'|'+query
        with self.lock:
            hit=self.db.execute('SELECT payload FROM searches WHERE key=?',(key,)).fetchone()
        if hit:return json.loads(hit[0])
        for attempt in range(4):
            if self.stop.is_set():raise RuntimeError('중단 요청됨')
            params={'query':query,'display':5 if section=='local' else 10,'start':1,'sort':'random' if section=='local' else 'sim'}
            req=Request(BASE+section+'?'+urlencode(params),headers={'X-NCP-APIGW-API-KEY-ID':self.cid,'X-NCP-APIGW-API-KEY':self.secret})
            self.reserve_request()
            try:
                with urlopen(req,timeout=30) as response: data=json.load(response)
                if 'items' not in data: raise RuntimeError('API 응답에 items가 없음; 결과를 성공 처리하지 않음')
                with self.lock:
                    self.db.execute('INSERT OR REPLACE INTO searches VALUES (?,?)',(key,json.dumps(data,ensure_ascii=False)));self.db.commit()
                return data
            except HTTPError as exc:
                if exc.code in (401,403): raise RuntimeError(f'API HUB 인증/권한 오류 HTTP {exc.code}; 저장된 키와 API 권한 확인') from None
                if exc.code==429 or exc.code>=500:
                    if attempt<3:
                        delay=2**(attempt+1)
                        if exc.code==429:
                            retry=exc.headers.get('Retry-After','') if exc.headers else ''
                            try:delay=max(delay,float(retry))
                            except ValueError:pass
                            with self.rate_lock:self.cooldown=max(self.cooldown,time.monotonic()+delay)
                        if self.stop.wait(delay):raise RuntimeError('중단 요청됨')
                        continue
                raise RuntimeError(f'API HUB HTTP {exc.code}; 검색 캐시 보존 후 중단') from None
            except (URLError,TimeoutError,OSError):
                if attempt==3:raise RuntimeError('API 연결 실패; 검색 캐시 보존 후 중단') from None
                if self.stop.wait(2**attempt):raise RuntimeError('중단 요청됨')

class SharedFetcher:
    """Share each host's pacing and robots cache across all workers."""
    def __init__(self,stop):
        self.stop=stop;self.guard=threading.Lock();self.locks={};self.last={}
        owner=self
        class LimitedFetcher(Fetcher):
            def raw(self,url,delay=2):
                origin=self.check_url(url)
                with owner.guard:lock=owner.locks.setdefault(origin,threading.Lock())
                with lock:
                    if owner.stop.is_set():raise RuntimeError('중단 요청됨')
                    self.last=owner.last
                    return super().raw(url,delay)
        self.local=threading.local();self.factory=LimitedFetcher
    def fetch(self,url):
        if self.stop.is_set():raise RuntimeError('중단 요청됨')
        if not hasattr(self.local,'fetcher'):self.local.fetcher=self.factory(None)
        return self.local.fetcher.fetch(url)

def run_worker(row,api,fetcher,dbpath,max_pages):
    db=sqlite3.connect(dbpath,timeout=60)
    try:return collect(row,api,fetcher,db,max_pages)
    finally:db.close()

def collect(row,api,fetcher,db,max_pages):
    qlist=queries(row)
    local=api.search('local',qlist[0])['items']
    sources={};search_log=[]
    for query in qlist:
        for section in ('webkr','blog'):
            items=api.search(section,query)['items']
            search_log.append({'section':section,'query':query,'count':len(items)})
            for item in items:
                url=item.get('link','')
                if not url:continue
                hit={'title':clean(item.get('title')),'snippet':clean(item.get('description')),'date':item.get('postdate',''),'section':section,'query':query}
                sources.setdefault(url,{'url':url,'hits':[]})['hits'].append(hit)
    def relevant(source):
        name=norm(row['name']); short=norm(re.sub(r'\s+제주\d*호점$|\s+제주연동점$','',row['name']))
        return any(name in norm(h['title']+' '+h['snippet']) or (len(short)>=3 and short in norm(h['title']+' '+h['snippet'])) for h in source['hits'])
    def rank(s):
        host=urlsplit(s['url']).hostname or ''
        return (not relevant(s),not host.endswith('blog.naver.com'),not any(norm(row['name']) in norm(h['title']) for h in s['hits']))
    ordered=sorted(sources.values(),key=rank)
    attempted=0
    for source in ordered:
        source['body_status']='미조회'; source['identity']='미확인'
        if not relevant(source):source['body_status']='상호연결검토필요';continue
        if attempted>=max_pages:continue
        url=public_blog_url(source['url'])
        if (urlsplit(url).hostname or '').endswith('place.naver.com'):source['body_status']='플레이스상세조회제외';continue
        attempted+=1
        hit=db.execute('SELECT payload FROM pages WHERE url=?',(url,)).fetchone()
        try:
            body=json.loads(hit[0]) if hit else fetcher.fetch(url)
            if not hit:db.execute('INSERT OR IGNORE INTO pages VALUES (?,?)',(url,json.dumps(body,ensure_ascii=False)));db.commit()
            source['body']=body;source['body_status']='본문수집';source['identity']=identity(row,body['text'])
        except (ValueError,OSError) as exc:source['body_status']='본문수집실패';source['error']=str(exc)
    verified=sum(s['identity']=='상호주소근접일치' for s in ordered)
    bodies=sum(s['body_status']=='본문수집' for s in ordered)
    related=sum(relevant(s) for s in ordered)
    return {'merge_key':row['merge_key'],'place_id':row['place_id'],'name':row['name'],'place':row,'local_candidates':local,'searches':search_log,'sources':ordered,'status':'본문근거확보' if verified else '본문동일성검토' if bodies else '검색요약만확보' if related else '관련검색결과없음','verified_sources':verified,'body_count':bodies,'source_count':len(ordered),'related_count':related}

def export(db,out):
    fields=['merge_key','place_id','name','status','source_count','related_count','body_count','verified_sources']
    with (out/'evidence.jsonl.tmp').open('w',encoding='utf-8') as evidence,(out/'status.csv.tmp').open('w',encoding='utf-8-sig',newline='') as status:
        writer=csv.DictWriter(status,fieldnames=fields);writer.writeheader()
        for (raw,) in db.execute('SELECT payload FROM results ORDER BY rowid'):
            r=json.loads(raw);evidence.write(raw+'\n');writer.writerow({k:r[k] for k in fields})
    (out/'evidence.jsonl.tmp').replace(out/'evidence.jsonl');(out/'status.csv.tmp').replace(out/'status.csv')

def main():
    p=argparse.ArgumentParser();p.add_argument('--input',default='targets.csv');p.add_argument('--output',default='result_v22');p.add_argument('--reuse-cache');p.add_argument('--limit',type=int,default=0);p.add_argument('--max-pages',type=int,default=6);p.add_argument('--api-budget',type=int,default=30000);p.add_argument('--workers',type=int,default=3);a=p.parse_args()
    if not 1<=a.workers<=6:p.error('--workers는 1~6')
    out=Path(a.output);out.mkdir(parents=True,exist_ok=True)
    raw=Path(a.input).read_bytes()
    with Path(a.input).open(encoding='utf-8-sig',newline='') as f:rows=list(csv.DictReader(f))
    if len({r['merge_key'] for r in rows})!=len(rows):raise SystemExit('중복 merge_key: 입력 확인 필요')
    cid=os.getenv('NAVER_CLIENT_ID','').strip();secret=os.getenv('NAVER_CLIENT_SECRET','').strip()
    if not cid or not secret:
        with open('/dev/tty','r+') as tty:
            if not cid:tty.write('NAVER API HUB Client ID: ');tty.flush();cid=tty.readline().strip()
            if not secret:secret=getpass.getpass('NAVER API HUB Client Secret: ',stream=tty)
    db=sqlite3.connect(out/'checkpoint.sqlite3',timeout=60)
    db.execute('PRAGMA journal_mode=WAL')
    for sql in ['CREATE TABLE IF NOT EXISTS searches(key TEXT PRIMARY KEY,payload TEXT)','CREATE TABLE IF NOT EXISTS pages(url TEXT PRIMARY KEY,payload TEXT)','CREATE TABLE IF NOT EXISTS results(key TEXT PRIMARY KEY,payload TEXT)','CREATE TABLE IF NOT EXISTS meta(value TEXT)']:db.execute(sql)
    config=hashlib.sha256(raw).hexdigest()+'|'+VERSION+'|'+str(a.max_pages)
    prior=db.execute('SELECT value FROM meta').fetchone()
    if prior and prior[0]!=config:raise SystemExit('입력 또는 설정 변경: 다른 --output 경로 사용')
    if not prior:db.execute('INSERT INTO meta VALUES (?)',(config,));db.commit()
    if a.reuse_cache:
        cache=Path(a.reuse_cache)/'checkpoint.sqlite3'
        if cache.resolve()==(out/'checkpoint.sqlite3').resolve():raise SystemExit('재사용 캐시와 output은 다른 경로여야 함')
        if cache.exists():
            old=sqlite3.connect(cache.resolve().as_uri()+'?mode=ro',uri=True)
            for table,key in [('searches','key'),('pages','url')]:
                for k,v in old.execute(f'SELECT {key},payload FROM {table}'):
                    db.execute(f'INSERT OR IGNORE INTO {table} VALUES (?,?)',(k,v))
            db.commit();old.close()
    done={x[0] for x in db.execute('SELECT key FROM results')}
    pending=[r for r in rows if r['merge_key'] not in done]
    if a.limit:pending=pending[:a.limit]
    print(f'targets={len(rows)} saved={len(done)} pending_this_run={len(pending)} workers={a.workers} api_concurrent=ON',flush=True)
    api_db=sqlite3.connect(out/'checkpoint.sqlite3',timeout=60,check_same_thread=False)
    api=API(api_db,cid,secret,a.api_budget);fetcher=SharedFetcher(api.stop)
    pool=ThreadPoolExecutor(max_workers=a.workers);active={};iterator=iter(pending);completed=0
    def fill():
        while len(active)<a.workers and not api.stop.is_set():
            row=next(iterator,None)
            if row is None:break
            active[pool.submit(run_worker,row,api,fetcher,out/'checkpoint.sqlite3',a.max_pages)]=row
    def save(future):
        nonlocal completed
        row=active.pop(future)
        try:result=future.result()
        except Exception as exc:
            api.stop.set();print(f'중단: {row["name"]}: {exc} (미완료 건은 다음 실행에 재시도)',flush=True);return
        db.execute('INSERT OR REPLACE INTO results VALUES (?,?)',(row['merge_key'],json.dumps(result,ensure_ascii=False)));db.commit()
        completed+=1
        print(f'[{completed}/{len(pending)}] {row["name"]}: {result["status"]} / 상호관련 {result["related_count"]} / 본문 {result["body_count"]} / 주소대응 {result["verified_sources"]}',flush=True)
        if completed%10==0:export(db,out)
    try:
        fill()
        while active:
            finished,_=wait(active,timeout=1,return_when=FIRST_COMPLETED)
            for future in finished:save(future)
            fill()
    except KeyboardInterrupt:
        api.stop.set();print('중단 요청: 진행 중 요청 종료 후 저장합니다. 기다려 주세요.',flush=True)
    finally:
        api.stop.set();pool.shutdown(wait=True)
        for future in list(active):save(future)
        export(db,out);api_db.close();db.close()

if __name__=='__main__':main()
