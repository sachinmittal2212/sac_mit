# Proogram to calculate running PNL

import pandas as pd
from pandas import read_csv

# to covert text file to .csv format
df = pd.read_csv("trades.txt",delimiter=' ')
df.columns = ['qty', 'price','']
df1 = df.iloc[df[df['qty']<0].index]['qty']*df.iloc[df[df['qty']<0].index]['price']
df2 = df.iloc[df[df['qty']>0].index]['qty']*df.iloc[df[df['qty']>0].index]['price']

# to calculate initial PNL
df['daily_PNL'] = pd.concat([df1,df2])
df['running_PNL'] = df['daily_PNL'].cumsum()
df.dropna(axis='columns', how='all')
df.to_csv('trades.csv')





