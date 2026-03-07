import pandas as pd

file_path = r'D:\ACUIT_test\cleaned_enron.csv'

df = pd.read_csv(file_path)

print(df.head())

print(df['Body'].iloc[0][:200] + "...")