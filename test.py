import pandas as pd

def main():
    df = pd.read_excel('지역_코드.xlsx')
    # df = pd.read_csv('지역_코드_정리.csv', encoding='utf-8-sig')
    print(df.columns)
    df = df.drop(columns=['위치업데이트', '경도(시)', '경도(분)',
       '경도(초)', '위도(시)', '위도(분)', '위도(초)', '경도(초/100)', '위도(초/100)'])
    # df = df[['구분', '행정구역코드', '1단계', '2단계','3단계', '격자 X', '격자 Y']]
    df = df[df['3단계'].isna()]
    df_clean = df.dropna(subset=['2단계']).drop(columns=['3단계'])
    print(df_clean)
    df_clean.to_csv('지역_코드_정리.csv', index=False, encoding='utf-8-sig')

if __name__ == '__main__':
    main()