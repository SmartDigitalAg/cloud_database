import os
from fastapi import FastAPI
import uvicorn
import requests
from datetime import datetime, timedelta
import pandas as pd
from tqdm import tqdm
from io import StringIO
from zoneinfo import ZoneInfo  # Python 3.9 이상


basetime = ['0200', '0500', '0800', '1100', '1400', '1700', '2000', '2300']
url = 'http://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getVilageFcst'
service_key = 'cnFWOksdH2rQuZ9YQs2IR3frMjm2kgy8eauRY4ujdTSTvGEeDGXulTzCIJtU7htSZeFnoof4l6RGh3EpVIbo1Q=='  # 인증키 (URL Encode 필요 없음)
base_time = '0200'
nx = 37.5606111111111  # 예보지점 X 좌표
ny = 127.039  # 예보지점 Y 좌표


def calculate_base_time():
    """현재 시간 기준으로 가장 최근 base_time 반환"""
    # now = datetime.now()
    now = datetime.now(ZoneInfo("Asia/Seoul"))

    current_hour = now.hour

    base_times = ['0200', '0500', '0800', '1100', '1400', '1700', '2000', '2300']
    base_hours = [2, 5, 8, 11, 14, 17, 20, 23]

    # 현재 시각보다 이전인 발표 시각들 중 가장 최근 것
    for i in range(len(base_hours) - 1, -1, -1):  # 뒤에서부터 검색
        if current_hour >= base_hours[i]:
            base_date = datetime.now(ZoneInfo("Asia/Seoul")).strftime('%Y%m%d')
            return base_date, base_times[i]

    if current_hour < 2:
        # 현재 시각이 02:00보다 이르면 전날 23:00
        base_date = (now - timedelta(days=1)).strftime('%Y%m%d')
        return base_date, '2300'


def get_ultra_short_data(nx, ny, base_date, base_time):
    # 요청 파라미터 구성
    df_final = pd.DataFrame()  # 최종 데이터프레임 초기화
    for i in range(1, 2):
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
        response = requests.get(url, params=params)
        # 요청 및 응답 처리
        try:
            if response.status_code == 200:
                data = response.json()  # JSON 응답 파싱
                result_json = data['response']['body']['items']['item']
                # print(f"✅ 요청 성공: {response.status_code} - 페이지 {i}")
                result_df = pd.DataFrame(result_json)
                df_final = pd.concat([df_final, result_df], ignore_index=True)  # 데이터프레임 합치기
            else:
                return ["요청 실패:", response.status_code]
        except requests.exceptions.JSONDecodeError as e:
            print("❌ JSON 디코딩 실패:", e)
            print("요청 파라미터", params)
            print("응답 내용:", response.text[:500])  # 내용 미리보기
            return None
        except requests.exceptions.RequestException as e:
            print("❌ 요청 실패:", e)
            return None
    return df_final.to_json(force_ascii=False)  # 최종 데이터프레임 반환

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


def download_ultra_short_data():
    print("🐻기상 데이터 수집 시작")
    os.makedirs("data", exist_ok=True)  # 데이터 저장 폴더 생성
    region_code_df = pd.read_csv('지역_코드_정리.csv', encoding='utf-8-sig')

    base_date, base_time = calculate_base_time()
    now_year = str(datetime.now(ZoneInfo("Asia/Seoul")).year)
    now_month = str(datetime.now(ZoneInfo("Asia/Seoul")).month)

    os.makedirs(os.path.join('data', now_year), exist_ok=True)  # 데이터 저장 폴더 생성

    if os.path.exists(os.path.join('data', now_year, f"{now_year}_{now_month}.csv")):
        already_save_df = pd.read_csv(os.path.join('data', now_year, f"{now_year}_{now_month}_ultra.csv"),
                                      encoding='utf-8-sig')

        # tqdm으로 진행률 표시
        for index, row in tqdm(region_code_df.iterrows(),
                               total=len(region_code_df),
                               desc="🌤️  기상 데이터 확인 중"):
            # 각 지역 코드에 대해 반복
            nx, ny = row['격자 X'], row['격자 Y']
            try:
                now_target_df = already_save_df[(already_save_df['nx'] == nx) &
                                                (already_save_df['ny'] == ny) &
                                                (already_save_df['baseTime'] == base_time) &
                                                (already_save_df['baseDate'] == base_date)]
            except:
                now_target_df = pd.DataFrame()

            try:
                if len(now_target_df) != 835 and len(now_target_df) != 943:
                    json_data = get_ultra_short_data(nx, ny, base_date, base_time)
                    data = pd.read_json(StringIO(json_data), orient='records')
            except:
                with open('error_log.txt', 'a') as f:
                    f.write(f"Error for nx: {nx}, ny: {ny} at {base_date} {base_time}\n")

            try:
                data = data[data['category'] == 'SKY'].reset_index().drop(columns=['index'])  # 'SKY' 카테고리 데이터만 필터링
                data['baseTime'] = data['baseTime'].astype(str).apply(lambda x: x.zfill(4))  # 문자열로 변환하고 0으로 채우기
                data['fcstTime'] = data['fcstTime'].astype(str).apply(lambda x: x.zfill(4))  # 문자열로 변환하고 0으로 채우기

                already_save_df = pd.concat([already_save_df, data], ignore_index=True)  # 모든 지역의 데이터 합치기
            except Exception as e:
                print(data)
                print(e)

        print("💾 데이터 저장 중...")
        already_save_df.to_csv(os.path.join('data', now_year, f"{now_year}_{now_month}.csv"), header=True, index=False)

    else:
        already_save_df = pd.DataFrame()  # 모든 지역의 데이터를 저장할 데이터프레임 초기화
        # tqdm으로 진행률 표시 (전체 데이터 수집)
        for index, row in tqdm(region_code_df.iterrows(),
                               total=len(region_code_df),
                               desc="🌤️  전체 기상 데이터 수집 중"):
            # 각 지역 코드에 대해 반복
            nx, ny = row['격자 X'], row['격자 Y']
            data = pd.read_json(get_ultra_short_data(nx, ny, base_date, base_time), orient='records')
            data = data[data['category'] == 'SKY'].reset_index().drop(columns=['index'])  # 'SKY' 카테고리 데이터만 필터링
            data['baseTime'] = data['baseTime'].astype(str).apply(lambda x: x.zfill(4))  # 문자열로 변환하고 0으로 채우기
            data['fcstTime'] = data['fcstTime'].astype(str).apply(lambda x: x.zfill(4))  # 문자열로 변환하고 0으로 채우기

            already_save_df = pd.concat([already_save_df, data], ignore_index=True)  # 모든 지역의 데이터 합치기

        print("💾 데이터 저장 중...")
        already_save_df.to_csv(os.path.join('data', now_year, f"{now_year}_{now_month}.csv"), header=True, index=False)

    print(f"🐻✅기상 데이터 수집 완료 - {base_date} {base_time} 기준")


def download_short_term_data():
    print("🐻기상 데이터 수집 시작")
    os.makedirs("data", exist_ok=True)  # 데이터 저장 폴더 생성
    region_code_df = pd.read_csv('지역_코드_정리.csv', encoding='utf-8-sig')

    df_all_region = pd.DataFrame()  # 모든 지역의 데이터를 저장할 데이터프레임 초기화

    base_date, base_time = calculate_base_time()
    now_year = str(datetime.now(ZoneInfo("Asia/Seoul")).year)
    now_month = str(datetime.now(ZoneInfo("Asia/Seoul")).month)

    os.makedirs(os.path.join('data', now_year), exist_ok=True)  # 데이터 저장 폴더 생성

    if os.path.exists(os.path.join('data', now_year, f"{now_year}_{now_month}.csv")):
        check_region_df = pd.read_csv(os.path.join('data', now_year, f"{now_year}_{now_month}.csv"),
                                      encoding='utf-8-sig')

        # tqdm으로 진행률 표시
        for index, row in tqdm(region_code_df.iterrows(),
                               total=len(region_code_df),
                               desc="🌤️  기상 데이터 확인 중"):
            # 각 지역 코드에 대해 반복
            nx, ny = row['격자 X'], row['격자 Y']
            try:
                region_df_check = check_region_df[(check_region_df['nx'] == nx) &
                                                  (check_region_df['ny'] == ny) &
                                                  (check_region_df['baseTime'] == base_time) &
                                                  (check_region_df['baseDate'] == base_date)]
            except:
                region_df_check = pd.DataFrame()

            try:
                if len(region_df_check) != 835 or len(region_df_check) != 943:
                    json_data = get_ultra_short_data(nx, ny, base_date, base_time)
                    data = pd.read_json(StringIO(json_data), orient='records')
                    data = data[data['category'] == 'SKY'].reset_index().drop(columns=['index'])  # 'SKY' 카테고리 데이터만 필터링
                    data['baseTime'] = data['baseTime'].astype(str).apply(lambda x: x.zfill(4))  # 문자열로 변환하고 0으로 채우기
                    data['fcstTime'] = data['fcstTime'].astype(str).apply(lambda x: x.zfill(4))  # 문자열로 변환하고 0으로 채우기

                    df_all_region = pd.concat([df_all_region, data], ignore_index=True)  # 모든 지역의 데이터 합치기
            except Exception as e:
                raise e

    else:
        # tqdm으로 진행률 표시 (전체 데이터 수집)
        for index, row in tqdm(region_code_df.iterrows(),
                               total=len(region_code_df),
                               desc="🌤️  전체 기상 데이터 수집 중"):
            # 각 지역 코드에 대해 반복
            nx, ny = row['격자 X'], row['격자 Y']
            data = pd.read_json(get_ultra_short_data(nx, ny, base_date, base_time), orient='records')
            data = data[data['category'] == 'SKY'].reset_index().drop(columns=['index'])  # 'SKY' 카테고리 데이터만 필터링
            data['baseTime'] = data['baseTime'].astype(str).apply(lambda x: x.zfill(4))  # 문자열로 변환하고 0으로 채우기
            data['fcstTime'] = data['fcstTime'].astype(str).apply(lambda x: x.zfill(4))  # 문자열로 변환하고 0으로 채우기

            df_all_region = pd.concat([df_all_region, data], ignore_index=True)  # 모든 지역의 데이터 합치기

    # 데이터 저장 과정에도 진행률 표시 (선택사항)
    print("💾 데이터 저장 중...")
    if not os.path.exists(os.path.join('data', now_year, f"{now_year}_{now_month}.csv")):
        df_all_region.to_csv(os.path.join('data', now_year, f"{now_year}_{now_month}.csv"), header=True, index=False)
    else:
        data_final = pd.read_csv(os.path.join('data', now_year, f"{now_year}_{now_month}.csv"))
        data_final = pd.concat([data_final, df_all_region], ignore_index=True)
        data_final.drop_duplicates(inplace=True)
        data_final.to_csv(os.path.join('data', now_year, f"{now_year}_{now_month}_short_term.csv"), index=False)


def main():
    download_ultra_short_data()
    # download_short_term_data()


if __name__ == "__main__":
    main()
