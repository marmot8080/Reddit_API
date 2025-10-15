import praw
import csv
import time
import json
import os
from praw.exceptions import RedditAPIException

# API 키 파일 경로
TOKEN_FILE = 'private/reddit_api_token.json'
# 수집 진행 상황 기록 파일 경로
PROGRESS_FILE = 'temp/progress_log.json'
# 데이터 저장 디렉터리 및 파일 이름
OUTPUT_DIR = 'result'
OUTPUT_FILE = os.path.join(OUTPUT_DIR, 'reddit_posts.csv')

# 수집할 서브레딧 목록 정의
SUBREDDITS = [
    'Thetruthishere', 'Glitch_in_the_Matrix', 'UnresolvedMysteries',
    'learnprogramming', 'cscareerquestions', 'SideProject',
    'TrueFilm', 'booksuggestions', 'TrueGaming'
]
# 목표 수집 개수
COLLECTION_LIMIT = 1000 
# 한 번의 API 요청으로 가져올 수 있는 최대 개수 (Reddit 규정상 100)
REQUEST_LIMIT = 100 

# API 키 로드
def load_api_keys(file_path):
    """지정된 JSON 파일에서 API 키를 읽어옵니다."""
    try:
        with open(file_path, 'r') as f:
            keys = json.load(f)
        return keys
    except FileNotFoundError:
        print(f"오류: API 토큰 파일 '{file_path}'을 찾을 수 없습니다. 파일을 생성하고 키를 입력해주세요.")
        return None
    except json.JSONDecodeError:
        print(f"오류: '{file_path}' 파일 형식이 올바르지 않습니다 (JSON 형식 확인).")
        return None

# 진행 상황 로그 관리
def load_progress(file_path):
    """진행 상황 로그를 로드합니다."""
    if os.path.exists(file_path):
        with open(file_path, 'r') as f:
            # last_submission_id: 마지막으로 수집된 게시물의 ID (다음 요청의 after 파라미터로 사용)
            return json.load(f)
    # 로그 파일이 없으면 초기 상태 반환
    return {sub: {'count': 0, 'last_submission_id': None} for sub in SUBREDDITS}

def save_progress(file_path, progress_data):
    """현재 진행 상황을 로그 파일에 저장합니다."""
    with open(file_path, 'w') as f:
        json.dump(progress_data, f, indent=4)

# 메인 수집 함수
def collect_reddit_data():
    """Reddit 데이터를 수집하고 진행 상황을 기록합니다."""
    
    # API 키 로드
    keys = load_api_keys(TOKEN_FILE)
    if not keys:
        return

    # Reddit API 설정 초기화
    reddit = praw.Reddit(
        client_id=keys.get('client_id'),
        client_secret=keys.get('client_secret'),
        user_agent=keys.get('user_agent')
    )

    # 출력 디렉터리 확인 및 생성
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # 진행 상황 로드
    progress = load_progress(PROGRESS_FILE)
    
    # CSV 파일을 'a' (추가) 모드로 열기. 파일이 없으면 헤더 작성
    file_exists = os.path.exists(OUTPUT_FILE)
    if not file_exists:
        with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['subreddit', 'title', 'text'])
            
    # 데이터를 추가 모드로 열어서 작성 준비
    with open(OUTPUT_FILE, 'a', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)

        for subreddit_name in SUBREDDITS:
            sub_progress = progress[subreddit_name]
            collected_count = sub_progress['count']
            last_id = sub_progress['last_submission_id']
            
            print(f"\n--- r/{subreddit_name}: 수집 시작 (현재 {collected_count}개, 마지막 ID: {last_id}) ---")

            if collected_count >= COLLECTION_LIMIT:
                print(f"r/{subreddit_name}는 이미 목표 ({COLLECTION_LIMIT}개)를 달성했습니다. 건너뜁니다.")
                continue

            subreddit = reddit.subreddit(subreddit_name)
            newly_collected = 0
            
            # 목표 개수에 도달할 때까지 반복
            while collected_count + newly_collected < COLLECTION_LIMIT:
                # after 인자 설정
                params = {'after': last_id} if last_id else {}
                
                # API 요청
                try:
                    submissions = subreddit.new(limit=REQUEST_LIMIT, params=params)
                except RedditAPIException as e:
                    print(f"  [API 오류 발생]: {e}. 30초 대기 후 다음 서브레딧으로 이동합니다.")
                    time.sleep(30)
                    break 
                except Exception as e:
                    print(f"  [예상치 못한 오류]: {e}")
                    time.sleep(10)
                    break


                # 리스트로 변환
                submissions_list = list(submissions)

                # 더 이상 새로운 게시물이 없으면 루프 종료
                if not submissions_list:
                    print(f"r/{subreddit_name}: 더 이상 새로운 게시물이 없습니다.")
                    break

                for submission in submissions_list:
                    # 목표 개수에 도달하면 루프 종료
                    if collected_count + newly_collected >= COLLECTION_LIMIT:
                        break
                        
                    # 본문이 있고, 50자 이상인 경우에만 수집
                    # if submission.selftext and len(submission.selftext) > 50: 
                    title = submission.title.replace('\n', ' ').strip()
                    text = submission.selftext.replace('\n', ' ').strip()
                    
                    writer.writerow([subreddit_name, title, text])
                    newly_collected += 1
                    
                    # 마지막 수집 ID 업데이트
                    last_id = submission.fullname 
                        
                # 현재 수집한 마지막 ID를 로그에 저장
                sub_progress['last_submission_id'] = last_id
                
                # 수집 개수 업데이트 및 로그 저장
                sub_progress['count'] = collected_count + newly_collected
                print(f"  -> 현재 {sub_progress['count']}개 수집 (새로 {newly_collected}개)")
                save_progress(PROGRESS_FILE, progress)
                
                # API 요청 간격 조절 (Reddit 규정: 1분당 60회)
                time.sleep(1) 
            
            print(f"r/{subreddit_name}: 최종 {sub_progress['count']}개 수집 완료.")


    print(f"\n데이터 수집 완료. 데이터는 '{OUTPUT_FILE}'에 저장되었습니다.")
    print("스크립트를 다시 실행하면 이어서 수집을 시도합니다.")

if __name__ == "__main__":
    collect_reddit_data()
