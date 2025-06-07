import os
import pandas as pd

df = pd.read_excel('지역_코드.xlsx')
df = df[df['3단계'].isnull()]
df = df[df['2단계'].notnull()]
df = df[['구분', '행정구역코드', '1단계', '2단계', '격자 X', '격자 Y', '경도(초/100)', '위도(초/100)']]
df.to_csv('지역_코드_정리.csv', index=False, encoding='utf-8-sig')