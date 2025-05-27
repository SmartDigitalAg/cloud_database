import os
from datetime import datetime, timedelta
import pandas as pd
from tqdm import tqdm
from io import StringIO
from zoneinfo import ZoneInfo
import requests
import logging

# 설정 상수들
BASE_TIMES = ['0200', '0500', '0800', '1100', '1400', '1700', '2000', '2300']
BASE_HOURS = [2, 5, 8, 11, 14, 17, 20, 23]
URL = 'http://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getVilageFcst'
SERVICE_KEY = 'cnFWOksdH2rQuZ9YQs2IR3frMjm2kgy8eauRY4ujdTSTvGEeDGXulTzCIJtU7htSZeFnoof4l6RGh3EpVIbo1Q=='
SEOUL_TZ = ZoneInfo("Asia/Seoul")

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class WeatherDataCollector:
    def __init__(self, region_csv_path='지역_코드_정리.csv'):
        self.region_df = pd.read_csv(region_csv_path, encoding='utf-8-sig')
        self.base_date, self.base_time = self._calculate_base_time()
        self.now_year = str(datetime.now(SEOUL_TZ).year)
        self.now_month = str(datetime.now(SEOUL_TZ).month)
        self.data_dir = os.path.join('data', self.now_year)
        os.makedirs(self.data_dir, exist_ok=True)

    def _calculate_base_time(self):
        """현재 시간 기준으로 가장 최근 base_time 반환"""
        now = datetime.now(SEOUL_TZ)
        current_hour = now.hour

        # 현재 시각보다 이전인 발표 시각들 중 가장 최근 것
        for i in range(len(BASE_HOURS) - 1, -1, -1):
            if current_hour >= BASE_HOURS[i]:
                return now.strftime('%Y%m%d'), BASE_TIMES[i]

        # 현재 시각이 02:00보다 이르면 전날 23:00
        yesterday = now - timedelta(days=1)
        return yesterday.strftime('%Y%m%d'), '2300'

    def _make_api_request(self, nx, ny, num_rows=1000):
        """API 요청을 보내고 응답을 처리"""
        params = {
            'serviceKey': SERVICE_KEY,
            'numOfRows': str(num_rows),
            'pageNo': '1',
            'dataType': 'JSON',
            'base_date': self.base_date,
            'base_time': self.base_time,
            'nx': nx,
            'ny': ny
        }

        try:
            response = requests.get(URL, params=params, timeout=30)
            if response.status_code == 200:
                data = response.json()
                items = data['response']['body']['items']['item']
                return pd.DataFrame(items)
            else:
                logger.error(f"API 요청 실패: {response.status_code} for nx={nx}, ny={ny}")
                return pd.DataFrame()
        except requests.exceptions.RequestException as e:
            logger.error(f"요청 실패 (nx={nx}, ny={ny}): {e}")
            return pd.DataFrame()
        except (KeyError, ValueError) as e:
            logger.error(f"응답 파싱 실패 (nx={nx}, ny={ny}): {e}")
            return pd.DataFrame()

    def _process_weather_data(self, df):
        """기상 데이터 전처리"""
        if df.empty:
            return df

        # SKY 카테고리만 필터링
        df = df[df['category'] == 'SKY'].copy()

        # 시간 형식 통일
        df['baseTime'] = df['baseTime'].astype(str).str.zfill(4)
        df['fcstTime'] = df['fcstTime'].astype(str).str.zfill(4)

        return df.reset_index(drop=True)

    def _get_existing_data(self, file_path):
        """기존 데이터 로드"""
        if os.path.exists(file_path):
            return pd.read_csv(file_path, encoding='utf-8-sig')
        return pd.DataFrame()

    def _check_data_completeness(self, existing_df, nx, ny):
        """특정 지역의 데이터 완성도 확인"""
        if existing_df.empty:
            return False

        region_data = existing_df[
            (existing_df['nx'] == nx) &
            (existing_df['ny'] == ny) &
            (existing_df['baseTime'] == self.base_time) &
            (existing_df['baseDate'] == int(self.base_date))
            ]

        # 데이터가 충분한지 확인 (835 또는 943개의 예상 레코드)
        return len(region_data) in [835, 943]

    def collect_weather_data(self, data_type='ultra_short'):
        """기상 데이터 수집 메인 함수"""
        logger.info(f"🐻 기상 데이터 수집 시작 - {self.base_date} {self.base_time} 기준")

        file_suffix = '_ultra' if data_type == 'ultra_short' else '_short_term'
        file_path = os.path.join(self.data_dir, f"{self.now_year}_{self.now_month}{file_suffix}.csv")

        existing_df = self._get_existing_data(file_path)
        new_data_list = []

        for _, row in tqdm(self.region_df.iterrows(),
                           total=len(self.region_df),
                           desc="🌤️ 기상 데이터 처리 중"):

            nx, ny = row['격자 X'], row['격자 Y']

            # 기존 데이터가 완전한지 확인
            if not existing_df.empty and self._check_data_completeness(existing_df, nx, ny):
                continue

            # 새 데이터 수집
            raw_data = self._make_api_request(nx, ny)
            processed_data = self._process_weather_data(raw_data)

            if not processed_data.empty:
                new_data_list.append(processed_data)

        # 데이터 병합 및 저장
        if new_data_list or existing_df.empty:
            self._save_data(existing_df, new_data_list, file_path)

        logger.info(f"🐻✅ 기상 데이터 수집 완료 - {self.base_date} {self.base_time} 기준")

    def _save_data(self, existing_df, new_data_list, file_path):
        """데이터 저장"""
        logger.info("💾 데이터 저장 중...")

        if new_data_list:
            new_df = pd.concat(new_data_list, ignore_index=True)

            if not existing_df.empty:
                final_df = pd.concat([existing_df, new_df], ignore_index=True)
                final_df.drop_duplicates(inplace=True)
            else:
                final_df = new_df
        else:
            final_df = existing_df

        final_df.to_csv(file_path, index=False, encoding='utf-8-sig')


def main():
    try:
        collector = WeatherDataCollector()
        collector.collect_weather_data('ultra_short')
        # collector.collect_weather_data('short_term')  # 필요시 주석 해제
    except Exception as e:
        logger.error(f"메인 실행 중 오류 발생: {e}")
        raise


if __name__ == "__main__":
    main()