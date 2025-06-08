import os
import sys
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
SEOUL_TZ = ZoneInfo("Asia/Seoul")

SERVICE_KEY = os.getenv("API_KEY")


# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class WeatherDataCollector:
    def __init__(self, region_csv_path='지역_코드_정리.csv'):
        self.region_df = pd.read_csv(region_csv_path, encoding='utf-8-sig')
        self.now = datetime.now(SEOUL_TZ)
        self.now_year = str(self.now.year)
        self.now_month = str(self.now.month)
        self.data_dir = os.path.join('data', self.now_year)
        os.makedirs(self.data_dir, exist_ok=True)

    def _calculate_base_time_for_short_term(self):
        """단기예보용 base_time 계산 (02,05,08,11,14,17,20,23시)"""
        current_hour = self.now.hour

        # 현재 시각보다 이전인 발표 시각들 중 가장 최근 것
        for i in range(len(BASE_HOURS) - 1, -1, -1):
            if current_hour >= BASE_HOURS[i]:
                return self.now.strftime('%Y%m%d'), BASE_TIMES[i]

        # 현재 시각이 02:00보다 이르면 전날 23:00
        yesterday = self.now - timedelta(days=1)
        return yesterday.strftime('%Y%m%d'), '2300'

    def _calculate_base_time_for_ultra_short(self):
        """초단기예보용 base_time 계산 (매시간 10분 발표, 정각 base_time)"""
        current_hour = self.now.hour
        current_minute = self.now.minute

        # 현재 시간이 10분 이후면 현재 시간 사용, 아니면 이전 시간 사용
        if current_minute >= 10:
            base_hour = current_hour
            base_date = self.now.strftime('%Y%m%d')
        else:
            if current_hour == 0:
                # 00시 10분 이전이면 전날 23시 기준
                yesterday = self.now - timedelta(days=1)
                base_date = yesterday.strftime('%Y%m%d')
                base_hour = 23
            else:
                base_hour = current_hour - 1
                base_date = self.now.strftime('%Y%m%d')

        base_time = f"{base_hour:02d}00"
        return base_date, base_time

    def _make_api_request(self, nx, ny, base_date, base_time, num_rows=1000):
        """API 요청을 보내고 응답을 처리"""
        params = {
            'serviceKey': SERVICE_KEY,
            'numOfRows': str(num_rows),
            'pageNo': '1',
            'dataType': 'JSON',
            'base_date': base_date,
            'base_time': base_time,
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

    def _check_data_completeness(self, existing_df, nx, ny, longitude, latitude, base_date, base_time, data_type):
        """특정 지역의 데이터 완성도 확인"""
        if existing_df.empty:
            return False

        # 기본 필터링 조건 (nx, ny, baseTime, baseDate로만 확인)
        # longitude, latitude는 나중에 추가될 수 있으므로 필터링에서 제외
        region_data = existing_df[
            (existing_df['nx'] == nx) &
            (existing_df['ny'] == ny) &
            (existing_df['baseTime'] == base_time) &
            (existing_df['baseDate'] == int(base_date))
            ]

        # 데이터 완성도 확인
        if data_type == 'ultra_short':
            # 초단기예보는 6시간(6개 시간대) 예보
            expected_records = 6
        else:
            # 단기예보는 3일간 예보 (72시간)
            expected_records = 72

        return len(region_data) >= expected_records

    def _should_collect_data(self, data_type):
        """데이터 수집 여부 판단"""
        if data_type == 'ultra_short':
            # 초단기예보: 매시간 30분마다 수집
            return True
        else:
            # 단기예보: base 시간(02,05,08,11,14,17,20,23시)에만 수집
            current_hour = self.now.hour
            return current_hour in BASE_HOURS

    def collect_weather_data(self, data_type='ultra_short'):
        """기상 데이터 수집 메인 함수"""
        # 수집 시점 확인
        if not self._should_collect_data(data_type):
            logger.info(f"⏰ {data_type} 데이터 수집 시간이 아닙니다.")
            return

        # base_time 계산
        if data_type == 'ultra_short':
            base_date, base_time = self._calculate_base_time_for_ultra_short()
        else:
            base_date, base_time = self._calculate_base_time_for_short_term()

        logger.info(f"🐻 {data_type} 기상 데이터 수집 시작 - {base_date} {base_time} 기준")

        file_suffix = '_ultra' if data_type == 'ultra_short' else '_short_term'
        file_path = os.path.join(self.data_dir, f"{self.now_year}_{self.now_month}{file_suffix}.csv")

        existing_df = self._get_existing_data(file_path)
        new_data_list = []
        skipped_count = 0

        for _, row in tqdm(self.region_df.iterrows(),
                           total=len(self.region_df),
                           desc=f"🌤️ {data_type} 기상 데이터 처리 중"):

            nx, ny = row['격자 X'], row['격자 Y']
            longitude, latitude = row['경도(초/100)'], row['위도(초/100)']

            # 기존 데이터가 완전한지 확인
            if self._check_data_completeness(existing_df, nx, ny, longitude, latitude, base_date, base_time, data_type):
                skipped_count += 1
                continue

            # 새 데이터 수집
            raw_data = self._make_api_request(nx, ny, base_date, base_time)
            processed_data = self._process_weather_data(raw_data)

            if not processed_data.empty:
                new_data_list.append(processed_data)

        logger.info(f"📊 스킵된 지역: {skipped_count}, 새로 수집된 지역: {len(new_data_list)}")

        # 데이터 병합 및 저장
        if new_data_list:
            self._save_data(existing_df, new_data_list, file_path)
        else:
            logger.info("💡 수집할 새 데이터가 없습니다.")

        logger.info(f"🐻✅ {data_type} 기상 데이터 수집 완료 - {base_date} {base_time} 기준")

    def _add_location_to_existing_data(self, df, column_type):
        """기존 데이터에 위치 정보 추가"""
        if df.empty:
            return df

        # 복사본 생성
        df_copy = df.copy()

        # 지역 정보와 매칭해서 위치 정보 추가
        for _, region_row in self.region_df.iterrows():
            nx, ny = region_row['격자 X'], region_row['격자 Y']

            if column_type == 'longitude':
                location_value = region_row['경도(초/100)']
            else:  # latitude
                location_value = region_row['위도(초/100)']

            # 해당 nx, ny에 해당하는 행들에 위치 정보 추가
            mask = (df_copy['nx'] == nx) & (df_copy['ny'] == ny)
            df_copy.loc[mask, column_type] = location_value

        return df_copy

    def _save_data(self, existing_df, new_data_list, file_path):
        """데이터 저장 - 누락된 컬럼 자동 추가"""
        logger.info("💾 데이터 저장 중...")

        new_df = pd.concat(new_data_list, ignore_index=True)

        if not existing_df.empty:
            # 기존 데이터에 longitude, latitude 컬럼이 없으면 추가
            if 'longitude' not in existing_df.columns:
                logger.info("📍 기존 데이터에 longitude 컬럼 추가 중...")
                existing_df = self._add_location_to_existing_data(existing_df, 'longitude')

            if 'latitude' not in existing_df.columns:
                logger.info("📍 기존 데이터에 latitude 컬럼 추가 중...")
                existing_df = self._add_location_to_existing_data(existing_df, 'latitude')

            final_df = pd.concat([existing_df, new_df], ignore_index=True)
            final_df.drop_duplicates(inplace=True)
        else:
            final_df = new_df

        final_df.to_csv(file_path, index=False, encoding='utf-8-sig')
        logger.info(f"💾 저장 완료: {len(final_df)} 레코드")

def main():
    """메인 실행 함수"""
    try:
        collector = WeatherDataCollector()

        # ultra_short와 short_term 둘 다 수집
        for data_type in ['ultra_short', 'short_term']:
            collector.collect_weather_data(data_type)

    except Exception as e:
        logger.error(f"❌ 메인 실행 중 오류 발생: {e}")
        raise


if __name__ == "__main__":
    main()
