"""Free-only pipeline: manual Naver discovery -> permitted pages -> local Ollama."""
import argparse
import copy
import hashlib
import html
from html.parser import HTMLParser
import ipaddress
import json
import re
import socket
import ssl
import sys
import time
from pathlib import Path
from urllib import request, error, robotparser
from urllib.parse import urlsplit, urljoin, quote

import jeju_enrich as core

MODEL = 'qwen3:4b'
LOCAL = 'http://127.0.0.1:11434'
UA = 'JejuContentResearch/1.0'
MAX_BYTES = 2_000_000
SCHEMA = copy.deepcopy(core.SCHEMA)
SCHEMA['properties']['facts']['items']['properties']['quote'] = core.STR
SCHEMA['properties']['facts']['items']['required'].append('quote')
SCHEMA['properties']['identity_proof'] = core.obj({'url': core.STR, 'quote': core.STR})
SCHEMA['properties']['identity_proof']['properties']['address_quote'] = core.STR
SCHEMA['properties']['identity_proof']['required'].append('address_quote')
SCHEMA['required'].append('identity_proof')
RULES = core.WRITE_RULES + '''
조사보고서 대신 sources에 실제 수집된 원문이 제공된다. 웹에 접속하거나 기억으로 보충하지 않는다.
facts의 quote는 해당 urls 원문에 실제 있는 짧은 연속 구절 그대로 적는다.
identity_proof에는 동일 장소의 이름과 전체 주소를 확인할 수 있는 원문 구절과 URL을 적는다.
identity_proof.quote에는 상호명 구절, address_quote에는 주소 구절을 각각 원문 그대로 적는다.
구체적 사실 2개 이상, 문장 2~6개, 태그 2~6개. 문장 하나에 근거 없는 주장을 덧붙이지 않는다.
quote에 해당 주장의 직접 근거가 없으면 그 주장을 쓰지 않는다. 카페를 식당으로 바꾸지 않는다.
원문의 '때마다'를 '매일'로 바꾸는 등 빈도/수량/조건을 강화하지 않는다.
태그는 2~10자의 명사형 핵심어만 쓴다. 예: 사이폰커피, 로스터리, 원두판매, 커스텀블렌딩.
태그에 '제공', '접근', '대상 서비스'처럼 설명을 붙이지 않는다. 원문에 없는 예시 태그는 금지.
본문은 '~이다', '~한다' 설명체로 쓰고 주소를 길게 반복하지 않는다.
예약 날짜, 특정 예약 가격, 이용자 한 명이 지불한 금액, 영업시간은 본문과 태그에서 제외한다.
객실별 차이가 있을 수 있으므로 '전 객실', '모든 객실', '항상' 같은 전체 보장 표현은 쓰지 않는다.
facts는 문장과 태그에서 실제 사용하는 2~4개만 작성한다. 원문에 없는 접속사를 quote에 넣지 않는다.
출처당 인용은 간결하게 최소한만. 반환 JSON은 주어진 schema를 따른다. /no_think
'''


def norm(text):
    return re.sub(r'[^0-9a-z가-힣]', '', text.lower())


class TextParser(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.hidden = 0
        self.parts = []

    def handle_starttag(self, tag, attrs):
        if tag in ('script', 'style', 'noscript', 'svg'):
            self.hidden += 1
        if tag in ('p', 'div', 'br', 'li', 'h1', 'h2', 'h3', 'tr'):
            self.parts.append('\n')

    def handle_endtag(self, tag):
        if tag in ('script', 'style', 'noscript', 'svg'):
            self.hidden = max(0, self.hidden - 1)

    def handle_data(self, data):
        if not self.hidden:
            self.parts.append(data)

    def text(self):
        return '\n'.join(filter(None, (re.sub(r'\s+', ' ', line).strip()
                                      for line in ''.join(self.parts).splitlines())))


class NoRedirect(request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class Fetcher:
    def __init__(self, domains):
        self.domains = set(domains) if domains is not None else None
        self.robots = {}
        self.last = {}
        try:
            import truststore
            context = truststore.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        except ImportError:
            context = ssl.create_default_context()
        self.opener = request.build_opener(NoRedirect(), request.HTTPSHandler(context=context))

    def check_url(self, url):
        p = urlsplit(url)
        if p.scheme not in ('https', 'http') or p.username or p.password or p.port not in (None, 80, 443):
            raise ValueError('허용하지 않는 URL 형식')
        if not p.hostname or (self.domains is not None and p.hostname not in self.domains):
            raise ValueError('허용 도메인에 없음: ' + str(p.hostname))
        # Naver search is navigated by the user, never harvested through another surface.
        if p.hostname in ('search.naver.com', 'm.search.naver.com'):
            raise ValueError('네이버 검색 결과는 자동 수집하지 않습니다.')
        addresses = socket.getaddrinfo(p.hostname, p.port or (443 if p.scheme == 'https' else 80))
        if not addresses or any(not ipaddress.ip_address(a[4][0]).is_global for a in addresses):
            raise ValueError('사설/로컬/예약 주소 수집 금지')
        return p.scheme + '://' + p.netloc

    def raw(self, url, delay=2):
        origin = self.check_url(url)
        time.sleep(max(0, delay - (time.monotonic() - self.last.get(origin, 0))))
        self.last[origin] = time.monotonic()
        with self.opener.open(request.Request(url, headers={'User-Agent': UA}), timeout=30) as res:
            body = res.read(MAX_BYTES + 1)
            if len(body) > MAX_BYTES:
                raise ValueError('본문이 2MB를 초과해 자동 수집 제외')
            return body, res.headers.get_content_type(), res.headers.get_content_charset()

    def rules(self, origin):
        if origin not in self.robots:
            rp = robotparser.RobotFileParser(origin + '/robots.txt')
            try:
                body, _, _ = self.raw(origin + '/robots.txt')
                rp.parse(body.decode('utf-8', errors='replace').splitlines())
            except error.HTTPError as exc:
                if exc.code == 404:
                    rp.parse(['User-agent: *', 'Allow: /'])
                else:
                    raise ValueError('robots.txt 확인 실패: HTTP ' + str(exc.code)) from None
            self.robots[origin] = rp
        return self.robots[origin]

    def fetch(self, url):
        # Search services may return decoded Korean paths; HTTP request targets must be ASCII.
        url = quote(url, safe=':/?&=%#@+;,~!*()-._')
        for _ in range(5):
            origin = self.check_url(url)
            rp = self.rules(origin)
            if not rp.can_fetch(UA, url):
                raise ValueError('robots.txt 수집 불허')
            delay = max(2, rp.crawl_delay(UA) or 0)
            rate = rp.request_rate(UA)
            if rate and rate.requests:
                delay = max(delay, rate.seconds / rate.requests)
            if delay > 60:
                raise ValueError('사이트 요청 간격이 60초 초과: 수동 검토')
            try:
                body, mime, encoding = self.raw(url, delay)
                break
            except error.HTTPError as exc:
                if exc.code in (301, 302, 303, 307, 308):
                    url = urljoin(url, exc.headers.get('Location', ''))
                    continue  # Each redirect rechecks domain, address and robots.
                raise ValueError('본문 HTTP ' + str(exc.code) + ': 차단/로그인은 우회하지 않음') from None
        else:
            raise ValueError('리다이렉트 횟수 초과')
        if mime not in ('text/html', 'application/xhtml+xml', 'text/plain'):
            raise ValueError('HTML/텍스트만 지원: ' + mime)
        if not encoding:
            meta = re.search(br'charset\s*=\s*["\x27]?([a-zA-Z0-9_-]+)', body[:4096])
            encoding = meta.group(1).decode() if meta else 'utf-8'
        try:
            decoded = body.decode(encoding)
        except (UnicodeError, LookupError):
            decoded = body.decode('cp949', errors='replace')
        if mime == 'text/plain':
            text = decoded
        else:
            parser = TextParser()
            parser.feed(decoded)
            text = parser.text()
        if len(text.strip()) < 80:
            raise ValueError('본문 부족 또는 JavaScript 전용 페이지')
        if any(s in text.lower() for s in ('verify you are human', 'access denied', '비정상적인 접근', '자동입력 방지')):
            raise ValueError('접근 차단/인증 페이지: 우회하지 않음')
        return {'url': url, 'text': text[:14000], 'truncated': len(text) > 14000,
                'fetched_at': core.datetime.now(core.timezone.utc).isoformat()}


class LocalModel:
    def __init__(self, model):
        self.model = model
        # Never use environment-configured proxies or remote endpoints for inference.
        self.opener = request.build_opener(request.ProxyHandler({}), NoRedirect())

    def api(self, route, payload=None):
        req = request.Request(LOCAL + route, data=core.dumps(payload).encode() if payload is not None else None,
                              headers={'Content-Type': 'application/json'})
        with self.opener.open(req, timeout=300) as res:
            return json.load(res)

    def check(self):
        models = self.api('/api/tags').get('models', [])
        found = next((m for m in models if m.get('name') == self.model), None)
        if not found or found.get('remote_host') or found.get('remote_model') or 'cloud' in self.model.lower():
            raise ValueError('다운로드된 로컬 모델만 허용: ' + self.model)
        details = self.api('/api/show', {'model': self.model})
        if details.get('remote_host') or details.get('remote_model') or details.get('details', {}).get('format') != 'gguf':
            raise ValueError('로컬 GGUF 모델임을 확인하지 못했습니다.')
        return found.get('digest', '')

    def generate(self, row, sources, feedback=None):
        # Bound total source text to fit the local model context; retain both page body and footer address.
        budget = 6000 // max(1, len(sources))
        model_sources = []
        for source in sources:
            text = source['text']
            if len(text) > budget:
                head = budget * 2 // 3
                text = text[:head] + '\n[중간 생략]\n' + text[-(budget - head):]
            model_sources.append({'url': source['url'], 'text': text})
        data = {'place': {k: row.get(k, '') for k in core.IDENTITY}, 'sources': model_sources}
        if feedback:
            data['previous_validation_error'] = feedback
        result = self.api('/api/generate', {'model': self.model, 'stream': False, 'think': False,
            'system': RULES, 'prompt': core.dumps(data), 'format': SCHEMA,
            'options': {'temperature': 0, 'num_ctx': 12288, 'num_predict': 2200}, 'keep_alive': '5m'})
        if not result.get('done') or result.get('done_reason') == 'length':
            raise ValueError('로컬 모델 응답 미완료/길이 초과')
        return json.loads(result['response']), {k: result.get(k) for k in
            ('total_duration', 'eval_count', 'prompt_eval_count')}


def normalize_draft(draft, sources):
    """Drop unused facts and restore literal whitespace; never change a factual claim."""
    used = {i for p in draft.get('sentences', []) + draft.get('tags', []) for i in p.get('fact_ids', [])}
    draft['facts'] = [f for f in draft.get('facts', []) if f['id'] in used]
    texts = {s['url']: s['text'] for s in sources}
    for fact in draft['facts']:
        quote_text = fact.get('quote', '')
        if not quote_text.strip():
            continue
        pattern = r'\s+'.join(re.escape(p) for p in quote_text.split())
        for url in fact.get('urls', []):
            found = re.search(pattern, texts.get(url, ''))
            if found:
                fact['quote'] = found.group(0)
                break
    return draft


def validate(draft, row, sources):
    texts = {s['url']: s['text'] for s in sources}
    content, tags = core.validate(draft, {}, allowed_urls=texts)
    proof = draft['identity_proof']
    quote_text = proof['quote']
    if not quote_text or quote_text not in texts.get(proof['url'], ''):
        raise ValueError('장소 확인 구절이 원문에 없음')
    if norm(row['name']) not in norm(quote_text):
        raise ValueError('장소 확인 구절에서 이름 불일치')
    address_quote = proof.get('address_quote', quote_text)
    if not address_quote or address_quote not in texts.get(proof['url'], ''):
        raise ValueError('주소 확인 구절이 원문에 없음')
    addresses = [norm(row.get(k, '')) for k in ('road_address', 'jibun_address') if row.get(k, '').strip()]
    if not addresses or not (any(address in norm(address_quote) for address in addresses) or
                            street_match(row.get('road_address', ''), address_quote)):
        raise ValueError('장소 확인 구절에서 전체 주소 미확인')
    for fact in draft['facts']:
        q = fact['quote']
        if len(q.strip()) < 4 or not any(q in texts.get(url, '') for url in fact['urls']):
            raise ValueError('사실의 원문 구절 미확인: ' + fact['id'])
    fact_map = {f['id']: f for f in draft['facts']}
    substantive = [f for f in draft['facts'] if not re.search(r'(?:대로|로|길)\s*\d+', f['quote'])]
    if len(substantive) < 2:
        raise ValueError('주소 나열을 제외하면 구체적 근거가 2개 미만')
    for part in draft['sentences'] + draft['tags']:
        if any(term in part['text'] for term in ('전 객실', '모든 객실')):
            raise ValueError('객실 전체 보장 표현 제외 필요')
        evidence = ' '.join(fact_map[i]['quote'] for i in part['fact_ids'])
        for qualifier in ('매일', '항상', '무료', '24시간', '최초', '최대', '유일', '무제한', '직접'):
            if qualifier in part['text'] and qualifier not in evidence:
                  raise ValueError('근거 없이 추가된 조건/빈도: ' + qualifier)
    address_text = norm(row.get('road_address', '') + row.get('jibun_address', ''))
    for tag in draft['tags']:
        if norm(tag['text']) and norm(tag['text']) in address_text:
            raise ValueError('주소/지역명 나열 태그 제외 필요')
    return content, tags


def street_match(expected, actual):
    """Allow omitted province only with same municipality + exact street and building number."""
    # A shared road address can contain separately registered buildings/units.
    units = re.findall(r'(?<![가-힣a-zA-Z0-9])([A-Za-z0-9]+\s*(?:동|호))(?![가-힣a-zA-Z0-9])', expected)
    if any(norm(unit) not in norm(actual) for unit in units):
        return False
    def parts(s):
        match = re.search(r'([가-힣0-9·]+(?:대로|로|길))\s*(\d+(?:-\d+)?)(?!\d)', s)
        city = re.search(r'(제주시|서귀포시)', s)
        return (city.group(1), match.group(1), match.group(2)) if city and match else None
    return parts(expected) is not None and parts(expected) == parts(actual)


def prepare(args):
    _, rows, digest = core.read_csv(args.input)
    out = Path(args.output_dir)
    out.mkdir(parents=True, exist_ok=True)
    target = out / 'sources.json'
    if target.exists():
        raise ValueError('sources.json이 이미 있습니다. 수정 내용을 보호하기 위해 새 폴더를 사용하세요.')
    chosen = [(i, row) for i, row in enumerate(rows) if i + 1 >= args.start and
              (not args.category or row['place_main_category'] == args.category) and
              (not args.name or args.name in row['name'])][:args.limit]
    records, cards = [], []
    for i, row in chosen:
        query = ' '.join((row['name'], row.get('road_address') or row.get('jibun_address', ''), row['place_main_category']))
        url = 'https://search.naver.com/search.naver?query=' + quote(query)
        records.append({'row': i + 1, 'name': row['name'], 'urls': []})
        cards.append('<article><b>' + html.escape(str(i + 1) + '. ' + row['name']) + '</b><p>' +
                     html.escape(query) + '</p><a target="_blank" rel="noopener noreferrer" href="' + url + '">네이버에서 검색</a></article>')
    target.write_text(core.dumps({'input_sha256': digest, 'places': records}), encoding='utf-8')
    (out / 'search.html').write_text('<!doctype html><html lang="ko"><meta charset="utf-8"><title>제주 출처 찾기</title>'
        '<style>body{max-width:950px;margin:40px auto;font:16px system-ui;background:#f6f8fa}article{background:white;padding:20px;margin:16px 0;border-radius:12px}a{color:#008744}</style>'
        '<h1>제주 장소 출처 찾기</h1><p>네이버 검색은 직접 확인합니다. 찾은 공식 원문 URL을 sources.json의 해당 행 urls에 넣으세요. '
        '본문 수집·활용이 허용된 출처만 등록하세요. 검색 결과 문구 자체는 수집하지 않습니다.</p>' + ''.join(cards) + '</html>', encoding='utf-8')
    print(f'{len(records)}곳 준비: {out / "search.html"}, {target}')


def run(args):
    fields, rows, digest = core.read_csv(args.input)
    manifest = json.loads(Path(args.sources).read_text(encoding='utf-8-sig'))
    if manifest.get('input_sha256') != digest:
        raise ValueError('원본 파일이 출처 목록 생성 시점과 다릅니다. 새 목록을 생성하세요.')
    places = manifest['places']
    indices = [p['row'] for p in places]
    if len(set(indices)) != len(indices):
        raise ValueError('출처 목록의 행 번호 중복')
    for p in places:
        if type(p['row']) is not int or not 1 <= p['row'] <= len(rows) or p['name'] != rows[p['row'] - 1]['name']:
            raise ValueError('출처 목록 행/이름 불일치')
        if not isinstance(p['urls'], list) or len(p['urls']) > 3 or not all(isinstance(u, str) for u in p['urls']):
            raise ValueError('urls는 원문 URL 최대 3개의 목록이어야 합니다.')
    out = Path(args.output_dir).resolve()
    if Path(args.input).resolve() == out / 'enriched.csv':
        raise ValueError('원본 덮어쓰기 금지')
    out.mkdir(parents=True, exist_ok=True)
    cache = out / 'cache'
    cache.mkdir(exist_ok=True)
    model = LocalModel(args.model)
    model_digest = model.check()
    fetcher = Fetcher(args.allow_domain)
    records = []
    processed = 0
    interrupted = False
    try:
        for place in places:
            row = rows[place['row'] - 1]
            key = hashlib.sha256(core.dumps([digest, place, model_digest, RULES, SCHEMA, sorted(args.allow_domain)]).encode()).hexdigest()
            path = cache / (key + '.json')
            if path.exists() and not args.retry_review:
                records.append(json.loads(path.read_text(encoding='utf-8')))
                continue
            if processed >= args.limit:
                continue
            processed += 1
            rec = {'row': place['row'], 'name': place['name'], 'status': 'review', 'sources': [], 'fetch_errors': []}
            try:
                for url in place['urls']:
                    try:
                        rec['sources'].append(fetcher.fetch(url))
                    except (ValueError, OSError) as exc:
                        rec['fetch_errors'].append({'url': url, 'reason': str(exc)})
                if not rec['sources']:
                    raise ValueError('수집 가능한 출처 없음. 네이버에서 공식 원문 URL을 찾아 등록하세요.')
                draft, usage = model.generate(row, rec['sources'])
                rec.update(draft=draft, usage=usage)
                content, tags = validate(draft, row, rec['sources'])
                rec.update(status='accepted', content=content, tags=tags)
            except (ValueError, KeyError, TypeError, OSError) as exc:
                rec['reason'] = str(exc)
            tmp = path.with_suffix('.tmp')
            tmp.write_text(core.dumps(rec), encoding='utf-8')
            tmp.replace(path)
            records.append(rec)
            print(f'{place["row"]} {place["name"]}: {rec["status"]} {rec.get("reason", "")}', flush=True)
    except KeyboardInterrupt:
        interrupted = True
    finally:
        # Include all matching cached results, including entries after a user interruption.
        by_row = {r['row']: r for r in records}
        for place in places:
            key = hashlib.sha256(core.dumps([digest, place, model_digest, RULES, SCHEMA, sorted(args.allow_domain)]).encode()).hexdigest()
            path = cache / (key + '.json')
            if place['row'] not in by_row and path.exists():
                by_row[place['row']] = json.loads(path.read_text(encoding='utf-8'))
        records = list(by_row.values())
        output = [dict(r) for r in rows]
        for rec in records:
            if rec['status'] == 'accepted':
                output[rec['row'] - 1].update(content=rec['content'], tags=rec['tags'])
        assert all(a[k] == b[k] for a, b in zip(rows, output) for k in fields if k not in ('content', 'tags'))
        core.write_csv(out / 'enriched.csv', fields, output)
        core.write_csv(out / 'review.csv', ['row', 'name', 'reason'],
                       [{k: r.get(k, '') for k in ('row', 'name', 'reason')} for r in records if r['status'] != 'accepted'])
        (out / 'evidence.jsonl').write_text(''.join(core.dumps(r) + '\n' for r in records), encoding='utf-8')
    print(core.dumps({'processed_total': len(records), 'accepted': sum(r['status'] == 'accepted' for r in records),
                     'output': str(out / 'enriched.csv')}))
    if interrupted:
        raise SystemExit(130)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    prep = sub.add_parser('prepare', help='무료 네이버 검색 링크와 출처 입력 목록 생성')
    prep.add_argument('input', type=Path)
    prep.add_argument('--output-dir', default='free_queue')
    prep.add_argument('--category')
    prep.add_argument('--name')
    prep.add_argument('--start', type=int, default=1)
    prep.add_argument('--limit', type=int, default=5)
    run_parser = sub.add_parser('run', help='허용 원문 수집 및 로컬 AI 작성')
    run_parser.add_argument('input', type=Path)
    run_parser.add_argument('--sources', required=True)
    run_parser.add_argument('--allow-domain', action='append', default=[], help='자동 수집을 허용할 정확한 호스트명')
    run_parser.add_argument('--output-dir', default='free_output')
    run_parser.add_argument('--model', default=MODEL)
    run_parser.add_argument('--limit', type=int, default=5)
    run_parser.add_argument('--retry-review', action='store_true', help='캐시를 무시하고 다시 수집/작성')
    check = sub.add_parser('check', help='로컬 모델 연결 확인, 유료 API 없음')
    check.add_argument('--model', default=MODEL)
    args = parser.parse_args()
    try:
        if hasattr(args, 'limit') and args.limit < 1:
            raise ValueError('--limit은 1 이상이어야 합니다.')
        if args.command == 'prepare':
            if args.start < 1:
                raise ValueError('--start는 1 이상이어야 합니다.')
            prepare(args)
        elif args.command == 'run':
            run(args)
        else:
            print('로컬 모델 확인: ' + LocalModel(args.model).check())
    except (ValueError, OSError, KeyError) as exc:
        parser.exit(1, f'오류: {exc}\n')


if __name__ == '__main__':
    main()
