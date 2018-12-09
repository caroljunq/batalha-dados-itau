df_obitos = pd.read_csv('Obitos-DataSUS-2013-2016.csv',sep=";")

#boston = datasets.load_boston()
#x,y = boston.data, boston.target
#
# knn = KNeighborsRegressor(n_neighbors=11,p=2)
#
# knn.fit(x,y)
#
# outputs = knn.predict(x )

# obito_municipio = df_obitos['OBITO_MUNICIPIO']
# obito_municipio = set(obito_municipio)
# obito_municipio = list(obito_municipio)
# indexacao_municipio = [(index, obito_municipio[index]) for index in range(len(obito_municipio))]
# dicio = dict((el, i) for i, el in indexacao_municipio)
# df_obitos['OBITO_MUNICIPIO'] = df_obitos['OBITO_MUNICIPIO'].replace(dicio)
