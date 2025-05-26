import os

from fastapi import FastAPI
import uvicorn
import requests
from datetime import datetime, timedelta
import pandas as pd

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

basetime = ['0200', '0500', '0800', '1100', '1400', '1700', '2000', '2300']
url = 'http://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getVilageFcst'
service_key = 'cnFWOksdH2rQuZ9YQs2IR3frMjm2kgy8eauRY4ujdTSTvGEeDGXulTzCIJtU7htSZeFnoof4l6RGh3EpVIbo1Q=='  # 인증키 (URL Encode 필요 없음)
base_time = '0200'
nx = 37.5606111111111  # 예보지점 X 좌표
ny = 127.039  # 예보지점 Y 좌표

app = FastAPI()
scheduler = BackgroundScheduler()

def calculate_base_time():
    """현재 시간 기준으로 가장 최근 base_time 반환"""
    now = datetime.now()
    current_hour = now.hour

    base_times = ['0200', '0500', '0800', '1100', '1400', '1700', '2000', '2300']
    base_hours = [2, 5, 8, 11, 14, 17, 20, 23]

    # 현재 시각보다 이전인 발표 시각들 중 가장 최근 것
    for i in range(len(base_hours) - 1, -1, -1):  # 뒤에서부터 검색
        if current_hour >= base_hours[i]:
            base_date = datetime.today().strftime('%Y%m%d')
            return base_date, base_times[i]


    if current_hour < 2:
        # 현재 시각이 02:00보다 이르면 전날 23:00
        base_date = (now - timedelta(days=1)).strftime('%Y%m%d')
        return base_date, '2300'

@app.get("/")
def main():
    return {"message": "Hello World"}

@app.get("/ultra_short_data")
def get_ultra_short_data(nx, ny, base_date, base_time):
    # 요청 파라미터 구성
    df_final = pd.DataFrame()  # 최종 데이터프레임 초기화
    for i in range(1,2):
        params = {
            'serviceKey': service_key,
            'numOfRows': '1000',
            'pageNo': i,
            'dataType': 'JSON',  # JSON 또는 XML
            'base_date': base_date,
            'base_time': base_time,
            'nx': nx,
            'ny': ny
        }

        # 요청 및 응답 처리
        try:
            response = requests.get(url, params=params)
            if response.status_code == 200:
                data = response.json()  # JSON 응답 파싱
                print(data)
                result_json = data['response']['body']['items']['item']
                result_df = pd.DataFrame(result_json)
                df_final = pd.concat([df_final, result_df], ignore_index=True)  # 데이터프레임 합치기
            else:
                return ["요청 실패:", response.status_code]
        except Exception as e:
            print("❌ 요청 실패:", e)

    return df_final.to_json(force_ascii=False)  # 최종 데이터프레임 반환
    df_final.to_csv('ultra_short_data.csv')  # CSV 파일로 저장

@app.get("/short_term_data")
def get_short_term_data():
    base_date, base_time = calculate_base_time()
    params = {
        'serviceKey': service_key,  # 인증키 (URL 인코딩 안해도 됨)
        'numOfRows': '50',  # 한 페이지 결과 수
        'pageNo': '1',  # 페이지 번호
        'dataType': 'JSON',  # 응답 형식 (JSON or XML)
        'base_date': base_date,  # 발표일자 (YYYYMMDD)
        'base_time': base_time,  # 발표시각 (HHMM)
        'nx': nx,  # 예보지점 X 좌표
        'ny': ny  # 예보지점 Y 좌표
    }

    response = requests.get(url, params=params)

    if response.status_code == 200:
        data = response.json()

    else:
        print("❌ 요청 실패:", response.status_code)

# 스케줄러 상태 확인 엔드포인트 추가
@app.get("/scheduler/status")
def get_scheduler_status():
    """스케줄러 상태 확인"""
    jobs = []
    for job in scheduler.get_jobs():
        jobs.append({
            "id": job.id,
            "name": job.name,
            "next_run": str(job.next_run_time),
            "trigger": str(job.trigger)
        })
    return {
        "running": scheduler.running,
        "jobs": jobs
    }

def download_ultra_short_data():
    print("🐻기상 데이터 수집 시작")
    os.makedirs("data", exist_ok=True)  # 데이터 저장 폴더 생성
    region_code_df = pd.read_csv('지역_코드_정리.csv', encoding='utf-8-sig')
    base_date, base_time = calculate_base_time()
    now_year = str(datetime.now().year)
    now_month = str(datetime.now().month)

    os.makedirs(os.path.join('data', now_year), exist_ok=True)  # 데이터 저장 폴더 생성
    if os.path.exists(os.path.join('data', now_year, f"{now_year}_{now_month}.csv")):
        check_region_df = pd.read_csv(os.path.join('data', now_year, f"{now_year}_{now_month}.csv"), encoding='utf-8-sig')

        for index, row in region_code_df.iterrows():
            # 각 지역 코드에 대해 반복
            nx, ny = row['격자 X'], row['격자 Y']
            region_df_check = region_code_df[(region_code_df['격자 X'] == nx & region_code_df['격자 Y'] == ny
                                              & region_code_df['baseTime'] == base_time & region_code_df[
                                                  'baseDate'] == base_date)]
            if region_df_check != 835 or region_df_check != 943:
                data = pd.read_json(get_ultra_short_data(nx, ny, base_date, base_time), orient='records')
                data = data[data['category'] == 'SKY'].reset_index().drop(columns=['index'])  # 'SKY' 카테고리 데이터만 필터링
                data['baseTime'] = data['baseTime'].astype(str).apply(lambda x: x.zfill(4))  # 문자열로 변환하고 0으로 채우기
                data['fcstTime'] = data['fcstTime'].astype(str).apply(lambda x: x.zfill(4))  # 문자열로 변환하고 0으로 채우기

                df_all_region = pd.concat([df_all_region, data], ignore_index=True)  # 모든 지역의 데이터 합치기

    else:
        for index, row in region_code_df.iterrows():
            # 각 지역 코드에 대해 반복
            nx, ny = row['격자 X'], row['격자 Y']
            data = pd.read_json(get_ultra_short_data(nx, ny, base_date, base_time), orient='records')
            data = data[data['category']=='SKY'].reset_index().drop(columns=['index'])  # 'SKY' 카테고리 데이터만 필터링
            data['baseTime'] = data['baseTime'].astype(str).apply(lambda x: x.zfill(4)) # 문자열로 변환하고 0으로 채우기
            data['fcstTime'] = data['fcstTime'].astype(str).apply(lambda x: x.zfill(4)) # 문자열로 변환하고 0으로 채우기

            df_all_region = pd.concat([df_all_region, data], ignore_index=True)  # 모든 지역의 데이터 합치기


    if not os.path.exists(os.path.join('data', now_year, f"{now_year}_{now_month}.csv")):
        df_all_region.to_csv(os.path.join('data', now_year, f"{now_year}_{now_month}.csv"), header=False, index=False)
    else:
        data_final = pd.read_csv(os.path.join('data', now_year, f"{now_year}_{now_month}.csv"))
        data_final = pd.concat([data_final, df_all_region], ignore_index=True)
        data_final.drop_duplicates(inplace=True)
        data_final.to_csv(os.path.join('data', now_year, f"{now_year}_{now_month}.csv"), mode='a', header=False,
                          index=False)
    print(f"🐻✅기상 데이터 수집 완료 - {base_date} {base_time} 기준")


scheduler.add_job(
    func=download_ultra_short_data,
    trigger=CronTrigger(minute=11),  # 매시간 10분에 실행
    id='weather_job',
    name='Weather API Job'
)

# FastAPI 이벤트 핸들러
@app.on_event("startup")
async def startup_event():
    scheduler.start()  # 🔥 중요: 스케줄러 시작!
    print("📅 스케줄러가 시작되었습니다.")
    print("⏰ 매시간 10분마다 기상 데이터를 수집합니다.")

@app.on_event("shutdown")
async def shutdown_event():
    scheduler.shutdown()
    print("📅 스케줄러가 종료되었습니다.")

if __name__ == "__main__":
    uvicorn.run("main:app", host='0.0.0.0', reload=True, port=8000)