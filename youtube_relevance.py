##### Made by Paleshift, DFRC #####

##### 기본 라이브러리 #####
import time
import sqlite3
import datetime
import re
import json
from typing import List, Set, Optional, Tuple

##### 유튜브 비공식 라이브러리 1 (yt-dlp) #####
import yt_dlp

##### 유튜브 비공식 라이브러리 2 (Selenium) #####
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import (
    StaleElementReferenceException,
    ElementClickInterceptedException,
    ElementNotInteractableException,
)

##### 유튜브 공식 라이브러리 (YouTube Data API v3) #####
import requests

##########

##### 사용자 입력 기반 기본 설정 #####

QUERY = "USA" # TODO: 원하는 검색어로 교체

SEARCH_FILTER_SP = "CAASBAgBEAE%253D" # 필터: 업로드 날짜(지난 1시간) + 구분(동영상) + 정렬기준(관련성)

DATABASE_FILE = "youtube_data.db" # 검색 결과가 저장될 DB의 파일명

SCROLL_PAUSE = 2.0 # 한 번 스크롤 내린 후 2초 대기
MAX_SCROLL_TRIES = 100 # 최대 100번까지만 스크롤하고 정지

KST = datetime.timezone(datetime.timedelta(hours=9)) # 모든 시간은 KST를 기준으로 저장

YOUTUBE_API_KEY = "YouTube Data API v3" # TODO: 본인의 YouTube Data API v3 키로 교체

##########

##### DB 초기화 #####

conn = sqlite3.connect(DATABASE_FILE) # DB 파일에 연결
cursor = conn.cursor() # SQL 명령어를 실행해줄 Cursor 생성 

cursor.execute("""
CREATE TABLE IF NOT EXISTS videos (
    id TEXT PRIMARY KEY, 
    title TEXT,          
    channel TEXT,        
    publish_time TEXT,   
    description TEXT,    
    duration TEXT,       
    status TEXT,         
    url TEXT             
);
""") # videos 테이블(영상의 vid, 중복 불가 / 영상의 제목 / 영상을 업로드한 채널의 이름 / 영상을 업로드한 시간 / 영상에 대한 설명 / 영상의 길이 / 영상의 종류 / 영상의 url) 

cursor.execute("""
CREATE TABLE IF NOT EXISTS comments (
    video_id TEXT,                              
    comment TEXT,                               
    author_name TEXT,                           
    author_channel_id TEXT,                     
    author_channel_url TEXT,                    
    comment_time_kst TEXT,                      
    FOREIGN KEY(video_id) REFERENCES videos(id)
);
""") # comments 테이블(댓글이 속해있는 영상의 vid / 댓글 내용 / 댓글 작성자 채널의 이름 / 댓글 작성자 채널의 id / 댓글 작성자 채널의 url / 댓글을 작성한 시간 / videos 테이블의 id column을 참조해 videos_id를 foreign key로 설정)

cursor.execute("""
CREATE TABLE IF NOT EXISTS video_raw (
    id TEXT PRIMARY KEY,                  
    raw_json TEXT,                        
    FOREIGN KEY(id) REFERENCES videos(id)
);
""") # video_raw 테이블(영상의 vid, 중복 불가 / JSON 원본 텍스트 / videos 테이블의 id column을 참조해 id를 foreign key로 설정)

conn.commit()

##### 3개의 테이블 (videos, comments, video_raw) 각각에 수정 사항 기록을 위한 column 추가 #####

##### videos.revised_contents: videos 테이블의 수정 사항 기록 #####
##### video_raw.revised_count: video_raw 테이블의 수정 사항 발생 횟수 기록 #####
##### comments.revised_contents: comments 테이블의 수정 사항 기록 #####

# 테이블에 특정 column이 없으면 해당 column을 추가하는 함수
def _add_column_if_missing(table_name: str, column_def: str) -> None:
    col_name = column_def.split()[0] # Ex) revised_contents TEXT --> revised_contents
    cursor.execute(f"PRAGMA table_info({table_name})") # 현재 테이블의 column 정보 조회
    cols = [r[1] for r in cursor.fetchall()] # 존재하는 column 이름들의 리스트 생성 
    if col_name not in cols: # 만약 특정 column이 리스트에 없으면 해당 column을 추가
        try:
            cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_def}")
            conn.commit()
        except Exception:
            pass # 작업이 실패해도 프로그램이 종료되지 않게 그냥 넘어감

# 각 테이블에 있어야 할 column
_add_column_if_missing("videos", "revised_contents TEXT")
_add_column_if_missing("video_raw", "revised_count INTEGER DEFAULT 0")
_add_column_if_missing("comments", "revised_contents TEXT")

##########

##### Selenium 설정 #####

chrome_options = Options()
chrome_options.add_argument("--headless=new")     # 화면을 띄우지 않고 백그라운드에서 실행
chrome_options.add_argument("--disable-gpu")      # GPU 가속 끄기
chrome_options.add_argument("--no-sandbox")       # 리눅스 환경에선 보안 샌드박스 끄기

driver = webdriver.Chrome(options=chrome_options) # 위 설정대로 크롬 실행

##### 유틸 함수 #####

def build_search_url(query: str) -> str: # 검색어와 필터를 합쳐 유튜브 검색 URL 생성
    return f"https://www.youtube.com/results?search_query={query}&sp={SEARCH_FILTER_SP}"


def get_publish_time_kst(info: dict) -> str: # yt-dlp를 통해 얻은 JSON 원본 텍스트에서 업로드 시간을 찾아 KST로 변환
    dt = None

    # 먼저 정확한 초 단위 타임스탬프가 있는지 확인
    ts = info.get("timestamp") or info.get("release_timestamp")
    if ts:
        try: # UTC 시간 객체로 변환
            dt = datetime.datetime.fromtimestamp(int(ts), tz=datetime.timezone.utc)
        except Exception:
            dt = None

    # 타임스탬프가 없으면 날짜 문자열 확인
    if dt is None:
        upload_date = info.get("upload_date")
        if upload_date:
            try:
                dt = datetime.datetime.strptime(upload_date, "%Y%m%d")
                dt = dt.replace(tzinfo=datetime.timezone.utc)
            except Exception:
                dt = None

    if dt is None:
        return "" # 둘 다 없으면 빈 문자열 반환

    # 찾은 시간을 KST로 변환해 문자열로 반환
    return dt.astimezone(KST).strftime("%Y-%m-%dT%H:%M:%S%z")


def classify_video_type(info: dict) -> str: # 영상의 종류를 분류
    live_status = info.get("live_status")
    if info.get("is_live") or live_status == "is_live": # 라이브 방송 여부
        return "live"
    if live_status in ("is_upcoming", "upcoming"): # 라이브 방송 예정 여부
        return "upcoming_live"
    if info.get("was_live") or live_status in ("was_live", "post_live"): # 라이브 방송 종료 여부
        return "live_archive"

    # 영상의 길이가 1분 미만이면 쇼츠로 분류
    duration = int(info.get("duration") or 0)
    if duration < 60:
        return "short"

    # 나머지는 일반 영상으로 분류
    return "vod"


def iso8601_to_kst(dt_str: str) -> str: # YouTube Data API v3의 publishedAt이 반환해주는 시간(ISO8601, UTC)을 파싱해 KST 문자열로 변환
    if not dt_str:
        return ""
    try:
        # Z(UTC)를 +00:00으로 바꿔 fromisoformat 사용
        dt = datetime.datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
        return dt.astimezone(KST).strftime("%Y-%m-%dT%H:%M:%S%z")
    except Exception:
        return ""


def _get_next_revision_number(revised_contents: str) -> int: # 기존 revised_contents의 문자열을 보고 다음 수정 번호 계산
    if not revised_contents or not revised_contents.strip():
        return 1
    # 정규식으로 "숫자." 패턴의 개수를 셈
    rounds = re.findall(r"\b(\d+)\.", revised_contents)
    try:
        return len(rounds) + 1
    except Exception:
        return 1


def _append_revision(revised_contents: str, changes: List[str], revision_number: int) -> str: # 변경 사항들을 기존 revised_contents의 문자열에 추가
    if not changes:
        return revised_contents
    new_entry = f"{revision_number}. " + ", ".join(changes)
    if not revised_contents or not revised_contents.strip():
        return new_entry
    else: # 각 라운드는 공백으로 구분
        return f"{revised_contents} {new_entry}"


def _update_video_raw_revised_count(video_id: str, raw_json_str: str, changed: bool) -> None: # video_raw 테이블의 raw_json과 revised_count를 갱신
    cursor.execute("SELECT revised_count FROM video_raw WHERE id = ?", (video_id,))
    row = cursor.fetchone()
    if row: # UPDATE하기
        current_count = row[0] or 0
        new_count = current_count + 1 if changed else current_count
        cursor.execute(
            "UPDATE video_raw SET raw_json = ?, revised_count = ? WHERE id = ?",
            (raw_json_str, new_count, video_id),
        )
    else:   # INSERT하기
        init_count = 1 if changed else 0
        cursor.execute(
            "INSERT INTO video_raw (id, raw_json, revised_count) VALUES (?, ?, ?)",
            (video_id, raw_json_str, init_count),
        )
    conn.commit()


def scroll_and_collect_video_ids(query: str) -> List[str]: # 검색 결과 페이지를 스크롤해 영상 ID만 수집
    url = build_search_url(query)
    print(f"🔍 검색 URL: {url}")
    driver.get(url) # 브라우저 주소창에 검색 URL을 입력해 이동
    time.sleep(3)   # 검색 결과 페이지가 뜰 때까지 3초 대기

    video_ids: Set[str] = set() # 중복되는 ID는 자동으로 제거되도록 집합(Set) 사용

    # 스크롤 높이를 측정해 더 내려갈 곳이 있는지 확인  
    last_height = driver.execute_script("return document.documentElement.scrollHeight")
    last_count = 0
    stable_rounds = 0

    # 최대 MAX_SCROLL_TRIES 만큼 반복하며 스크롤
    for i in range(MAX_SCROLL_TRIES):
        # 화면에 보이는 영상들의 링크를 탐색
        elements = driver.find_elements(By.XPATH, '//a[@id="video-title"]')
        for elem in elements:
            href = elem.get_attribute("href")
            # 링크가 존재하고 "/watch?v="의 형식이라면 vid 추출
            if href and "/watch?v=" in href:
                vid = href.split("watch?v=")[1].split("&")[0]
                video_ids.add(vid)

        print(f" --> 스크롤 {i+1}회차 / 수집된 영상 수: {len(video_ids)}개")

        # 자바스크립트로 화면을 맨 아래로 내림
        driver.execute_script("window.scrollTo(0, document.documentElement.scrollHeight);")
        time.sleep(SCROLL_PAUSE)

        # 스크롤 후 높이와 영상 개수의 변화 확인 
        new_height = driver.execute_script("return document.documentElement.scrollHeight")
        new_count = len(video_ids)

        # 높이와 영상 개수의 변화가 없고, 이를 포함해 3번 연속 변화가 없으면 스크롤 종료
        if new_height == last_height and new_count == last_count:
            stable_rounds += 1
            if stable_rounds >= 3:
                print("📌 더 이상 로딩되는 영상 없음 --> 스크롤 종료")
                break
        else:
            stable_rounds = 0 # 변화가 있으면 초기화

        last_height = new_height
        last_count = new_count

    print(f"📦 최종 수집된 영상 수: {len(video_ids)}개")
    return list(video_ids) # Set을 List로 변환해 반환


def fetch_and_store_video_metadata(video_id: str) -> bool: # yt-dlp를 통해 DB에 영상 메타데이터 저장
    video_url = f"https://www.youtube.com/watch?v={video_id}"

    # yt-dlp 설정: 다운로드는 하지 않고 JSON 정보만 가져옴
    ydl_opts = {
        "quiet": True,
        "skip_download": True,
        "forcejson": True,
        "extract_flat": False,
    }

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(video_url, download=False)
    except Exception as e:
        print(f"[Error! 영상 메타데이터 추출 실패] {video_id}: {e}")
        return False

    # 메타데이터 추출 및 전처리
    title = info.get("title", "") or ""
    channel = info.get("channel", "") or ""
    description = info.get("description", "") or ""
    duration_sec = info.get("duration", 0) or 0

    status = classify_video_type(info)
    publish_time = get_publish_time_kst(info)

    minutes, seconds = divmod(duration_sec, 60)
    duration_str = f"{minutes}분 {seconds}초"

    # JSON을 문자열로 변환
    raw_json_str = json.dumps(info, ensure_ascii=False)

    # DB에 해당 영상이 이미 저장되어 있는지 확인
    cursor.execute(
        "SELECT id, title, channel, publish_time, description, duration, status, url, revised_contents FROM videos WHERE id = ?",
        (video_id,),
    )
    existing = cursor.fetchone()

    # 새로 저장하려는 값들
    new_values = {
        "title": title,
        "channel": channel,
        "publish_time": publish_time,
        "description": description,
        "duration": duration_str,
        "status": status,
        "url": video_url,
    }

    changed = False
    revision_changes: List[str] = []

    # 해당 영상이 이미 저장되어 있다면 변경 사항 비교
    if existing:
        # existing = (id, title, channel, publish_time, description, duration, status, url, revised_contents)
        old_values = {
            "title": existing[1] or "",
            "channel": existing[2] or "",
            "publish_time": existing[3] or "",
            "description": existing[4] or "",
            "duration": existing[5] or "",
            "status": existing[6] or "",
            "url": existing[7] or "",
        }

        # 하나하나 변경 사항 확인
        for key in new_values:
            old_val = old_values.get(key, "")
            new_val = new_values[key] or ""
            if old_val != new_val:
                changed = True
                # 변경 사항 기록
                revision_changes.append(f"({key}: {old_val} --> {new_val})")

        # 변경 사항이 있으면 revised_contents 갱신
        old_revised = existing[8] or ""
        if changed:
            revision_number = _get_next_revision_number(old_revised)
            new_revised = _append_revision(old_revised, revision_changes, revision_number)
        else:
            new_revised = old_revised
        
        # DB 갱신
        cursor.execute(
            "UPDATE videos SET title = ?, channel = ?, publish_time = ?, description = ?, duration = ?, status = ?, url = ?, revised_contents = ? WHERE id = ?",
            (
                new_values["title"],
                new_values["channel"],
                new_values["publish_time"],
                new_values["description"],
                new_values["duration"],
                new_values["status"],
                new_values["url"],
                new_revised,
                video_id,
            ),
        )
    else:
        # 처음 보는 영상이라면 저장
        revision_changes = []
        new_revised = ""
        cursor.execute(
            "INSERT INTO videos (id, title, channel, publish_time, description, duration, status, url, revised_contents) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                video_id,
                new_values["title"],
                new_values["channel"],
                new_values["publish_time"],
                new_values["description"],
                new_values["duration"],
                new_values["status"],
                new_values["url"],
                new_revised,
            ),
        )
        changed = False
    
    # video_raw 테이블 업데이트
    _update_video_raw_revised_count(video_id, raw_json_str, changed)
    conn.commit()
    
    print(f"✅ 메타데이터 저장 완료: {video_id} / {title}")
    print(f"  --> publish_time(KST): {publish_time}, status: {status}")
    if changed and revision_changes:
        print(f"  --> 변경사항: {', '.join(revision_changes)}")
    return True


def fetch_comments_via_api(video_id: str) -> List[Tuple[str, str, str, str, str, str]]: # YouTube Data API v3(commentThreads)를 통해 댓글과 답글 수집
    rows: List[Tuple[str, str, str, str, str, str]] = []
    base_url = "https://www.googleapis.com/youtube/v3/commentThreads"
    params = {
        "key": YOUTUBE_API_KEY,
        "part": "snippet,replies", # snippet(댓글), replies(답글)
        "videoId": video_id,
        "maxResults": 100,
        "textFormat": "plainText", 
        "order": "time",           # 최신순
    }

    while True:
        resp = requests.get(base_url, params=params) # request 전송
        if resp.status_code != 200:
            print(f"[Error! commentThreads 호출 실패 ({resp.status_code}): {resp.text[:200]}]")
            break

        data = resp.json()
        items = data.get("items", []) # 댓글 목록

        for item in items:
            # 댓글 처리
            try:
                top = item["snippet"]["topLevelComment"]["snippet"]
            except KeyError:
                continue

            # 정보 추출
            comment_text = top.get("textDisplay") or top.get("textOriginal") or ""
            author_name = top.get("authorDisplayName", "")

            author_channel_id = ""
            author_channel_url = ""
            ch_obj = top.get("authorChannelId") or {}
            if isinstance(ch_obj, dict):
                author_channel_id = ch_obj.get("value", "") or ""
            if author_channel_id:
                author_channel_url = f"https://www.youtube.com/channel/{author_channel_id}"

            published_at = top.get("publishedAt", "")
            comment_time_kst = iso8601_to_kst(published_at)

            # 결과 리스트에 추가
            rows.append(
                (
                    video_id,
                    comment_text,
                    author_name,
                    author_channel_id,
                    author_channel_url,
                    comment_time_kst,
                )
            )

            # 답글 처리
            replies = (item.get("replies") or {}).get("comments") or []
            for rep in replies:
                rs = rep.get("snippet", {})

                # 정보 추출
                r_text = rs.get("textDisplay") or rs.get("textOriginal") or ""
                r_author_name = rs.get("authorDisplayName", "")

                r_author_channel_id = ""
                r_author_channel_url = ""
                r_ch_obj = rs.get("authorChannelId") or {}
                if isinstance(r_ch_obj, dict):
                    r_author_channel_id = r_ch_obj.get("value", "") or ""
                if r_author_channel_id:
                    r_author_channel_url = f"https://www.youtube.com/channel/{r_author_channel_id}"

                r_published_at = rs.get("publishedAt", "")
                r_comment_time_kst = iso8601_to_kst(r_published_at)

                rows.append(
                    (
                        video_id,
                        r_text,
                        r_author_name,
                        r_author_channel_id,
                        r_author_channel_url,
                        r_comment_time_kst,
                    )
                )

        # 다음 페이지가 있는지(댓글이 100개가 넘는지) 확인
        page_token = data.get("nextPageToken")
        if not page_token:
            break # 없으면 종료
        params["pageToken"] = page_token # 다음 페이지 토큰 설정 후 루프

    return rows


def scroll_and_collect_all_comments(video_id: str) -> None: # 수집한 댓글과 답글을 DB에 저장
    print(f"  --> 댓글 수집 시작 (YouTube Data API v3): {video_id}")

    rows = fetch_comments_via_api(video_id)
    print(f"  --> API로 가져온 댓글(+답글) 개수: {len(rows)}개")

    for row in rows:
        vid, comment_text, author_name, author_channel_id, author_channel_url, comment_time_kst = row
        # 댓글은 고유한 ID가 따로 없기에 [영상ID + 작성자채널ID + 작성시간]을 복합키로 사용해 식별
        cursor.execute(
            "SELECT comment, author_name, author_channel_url, comment_time_kst, revised_contents FROM comments WHERE video_id = ? AND author_channel_id = ? AND comment_time_kst = ?",
            (vid, author_channel_id or "", comment_time_kst),
        )
        existing = cursor.fetchone()
        new_values = {
            "comment": comment_text or "",
            "author_name": author_name or "",
            "author_channel_url": author_channel_url or "",
            "comment_time_kst": comment_time_kst or "",
        }
        changed = False
        revision_changes: List[str] = []
        if existing:
            # existing = (comment, author_name, author_channel_url, comment_time_kst, revised_contents)
            old_values = {
                "comment": existing[0] or "",
                "author_name": existing[1] or "",
                "author_channel_url": existing[2] or "",
                "comment_time_kst": existing[3] or "",
            }

            # 하나하나 변경 사항 확인
            for key in new_values:
                old_val = old_values.get(key, "")
                new_val = new_values[key]
                if old_val != new_val:
                    changed = True
                    # 변경 사항 기록
                    revision_changes.append(f"({key}: {old_val} --> {new_val})")
            old_revised = existing[4] or ""
            if changed:
                revision_number = _get_next_revision_number(old_revised)
                new_revised = _append_revision(old_revised, revision_changes, revision_number)
                
                # DB 갱신
                cursor.execute(
                    "UPDATE comments SET comment = ?, author_name = ?, author_channel_url = ?, comment_time_kst = ?, revised_contents = ? WHERE video_id = ? AND author_channel_id = ? AND comment_time_kst = ?",
                    (
                        new_values["comment"],
                        new_values["author_name"],
                        new_values["author_channel_url"],
                        new_values["comment_time_kst"],
                        new_revised,
                        vid,
                        author_channel_id or "",
                        comment_time_kst,
                    ),
                )
            
        else:
            # 처음 보는 댓글이라면 저장
            cursor.execute(
                "INSERT INTO comments (video_id, comment, author_name, author_channel_id, author_channel_url, comment_time_kst, revised_contents) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    vid,
                    new_values["comment"],
                    new_values["author_name"],
                    author_channel_id or "",
                    new_values["author_channel_url"],
                    new_values["comment_time_kst"],
                    "",
                ),
            )
    conn.commit()
    print(f"💬 [{video_id}] 최종 댓글(+답글) {len(rows)}개 저장 완료")

##### 전체 파이프라인 #####

def run_pipeline(query: str):
    video_ids = scroll_and_collect_video_ids(query)

    for idx, vid in enumerate(video_ids, 1):
        print(f"\n====== {idx} / {len(video_ids)} 처리 중: {vid} ======")
        ok = fetch_and_store_video_metadata(vid)
        if not ok:
            continue
        try:
            scroll_and_collect_all_comments(vid)
        except Exception as e:
            print(f"[Error! 댓글 수집 실패] {vid}: {e}")

##### main #####

if __name__ == "__main__":
    try:
        run_pipeline(QUERY)
    finally:
        driver.quit()
        conn.close()
        print("\n✅ 종료")