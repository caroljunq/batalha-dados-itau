import pandas as pd
# from sklearn.neighbors import KNeighborsRegressor
#
# df_internacao = pd.read_csv('AutIntHos-DataSUS-2013-2018.csv',sep=";")
df_obitos = pd.read_csv('Obitos-DataSUS-2013-2016.csv',sep=";")
header = df.iloc[0]
df_obitos = df_obitos[1:]
df.columns = header

#
#
# boston = datasets.load_boston()
# x,y = boston.data, boston.target
#
# knn = KNeighborsRegressor(n_neighbors=11,p=2)
#
# knn.fit(x,y)
#
# outputs = knn.predict(x )

obito_municio = df_obitos['OBITO_MUNICIPIO']
obito_municio = set(obito_municio)
obito_municio = list(obito_municio)
indexacao_municipio = [(index, obito_municio[index]) for index in range(len(obito_municio))]
dicio = dict((el, i) for i, el in obito_municio)
df['OBITO_MUNICIPIO'] = df['OBITO_MUNICIPIO'].replace(dicio)
