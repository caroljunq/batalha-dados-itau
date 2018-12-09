import pandas as pd

df_internacao = pd.read_csv('AutIntHos-DataSUS-2013-2018.csv',sep=";")

# # Normalização Cidade - Inteiro (indexacao municipio)
# cnes_municipio = df_internacao['CNES_MUNICIPIO']
# # Valore únicos
# cnes_municipio = set(cnes_municipio)
#
# cnes_municipio = list(cnes_municipio)
#
# indexacao_municipio = [(index, cnes_municipio[index]) for index in range(len(cnes_municipio))]
#
# dicio = dict((el, i) for i, el in indexacao_municipio)
#
# df_internacao['CNES_MUNICIPIO'] = df_internacao['CNES_MUNICIPIO'].replace(dicio)
#

# Correlação de variáveis de internações
# Complexidade e Valor Total
# Há entre 64% e 77% de relação entre a complexidade da doença/atendimento paciente e o custo total
print(df_internacao['COMPLEXIDADE'].corr(df_internacao['VALOR_TOTAL'],method='pearson'))
print(df_internacao['COMPLEXIDADE'].corr(df_internacao['VALOR_TOTAL'],method='kendall'))
print(df_internacao['COMPLEXIDADE'].corr(df_internacao['VALOR_TOTAL'],method='spearman'))

# Valor total e total de dias (diaria)
# Todas as correlações são negativas, portanto, não há corelação entre número de dias de internação e valor total cobrado
print(df_internacao['DIARIA'].corr(df_internacao['VALOR_TOTAL'],method='pearson'))
print(df_internacao['DIARIA'].corr(df_internacao['VALOR_TOTAL'],method='kendall'))
print(df_internacao['DIARIA'].corr(df_internacao['VALOR_TOTAL'],method='spearman'))

# # Correlação entre cidade e tempo de internação
# print(df_internacao['DIARIA'].corr(df_internacao['CNES_MUNICIPIO'],method='pearson'))
# print(df_internacao['DIARIA'].corr(df_internacao['CNES_MUNICIPIO'],method='kendall'))
# print(df_internacao['DIARIA'].corr(df_internacao['CNES_MUNICIPIO'],method='spearman'))

# # Correlação entre valor total e valor de uti
# print(df_internacao['VALOR_TOTAL'].corr(df_internacao['VALOR_UTI'],method='pearson'))
# print(df_internacao['VALOR_TOTAL'].corr(df_internacao['VALOR_UTI'],method='kendall'))
# print(df_internacao['VALOR_TOTAL'].corr(df_internacao['VALOR_UTI'],method='spearman'))

# Correlção Idade e Complexidade
print(df_internacao['VALOR_TOTAL'].corr(df_internacao['VALOR_UTI'],method='pearson'))
print(df_internacao['VALOR_TOTAL'].corr(df_internacao['VALOR_UTI'],method='kendall'))
print(df_internacao['VALOR_TOTAL'].corr(df_internacao['VALOR_UTI'],method='spearman'))
