# Correlação e cruzamento das bases de dados fornecidas com pyspark

from pyspark import SparkConf, SparkContext, SparkSQL
from pyspark.sql.types import DoubleType


conf = (SparkConf()
         .setMaster("local")
         .setAppName("batalha de dados")
         .set("spark.executor.memory", "1g"))

sc = SparkContext(conf = conf)

df_internacao = spark.read.csv("/tmp/saude/saude/HSL/AutIntHos-DataSUS-2013-2018.csv", header="true", sep=";")
nv_internacao = df_internacao.withColumn("VALOR_UTI", df_internacao["VALOR_UTI"].cast(DoubleType()))
nv_internacao.printSchema()

tb_internacao = nv_internacao.createOrReplaceTempView("internacao")


df_obitos = spark.read.csv("/tmp/saude/saude/HSL/Obitos-DataSUS-2013-2016.csv", header="true", sep=";")
df_obitos.printSchema()
tb_obitos = df_obitos.createOrReplaceTempView("obto")


maior_custo_uti_por_municipio = spark.sql(
""" select
CID_PRINCIPAL,
CNES_UF,
CNES_MUNICIPIO,
VALOR_UTI,
LEITO_TIPO,
UTI_TIPO,
UTI_QTD_DIAS,
VALOR_UTI,
COMPLEXIDADE,
DIARIA
from
internacao
order by VALOR_UTI desc""")

maior_custo_uti_por_municipio.show(30, truncate=False)

tipo_uti = maior_custo_uti_por_municipio.groupBy("UTI_TIPO").count().show(100, truncate=False)

# +---------------------------------------------------+------+
# |UTI_TIPO                                           |count |
# +---------------------------------------------------+------+
# |7                                                  |5     |
# |15                                                 |1     |
# |11                                                 |3     |
# |3                                                  |22    |
# |01                                                 |11    |
# |8                                                  |5     |
# |22                                                 |1     |
# |85                                                 |21    |
# |16                                                 |1     |
# |0                                                  |102   |
# |null                                               |717   |
# |5                                                  |7     |
# |18                                                 |3     |
# |HECI HOSPITAL EVANGELICO DE CACHOEIRO DE ITAPEMIRIM|1     |
# |75                                                 |7550  |
# |00                                                 |343123|
# |78                                                 |2     |
# |6                                                  |3     |
# |19                                                 |3     |
# |23                                                 |1     |
# |COMPLEXO HOSPITALAR SAO FRANCISCO                  |2     |
# |HOSPITAL ARISTIDES MALTEZ                          |1     |
# |81                                                 |3     |
# |79                                                 |4     |
# |24                                                 |1     |
# |9                                                  |1     |
# |1                                                  |26    |
# |20                                                 |1     |
# |10                                                 |1     |
# |4                                                  |10    |
# |12                                                 |4     |
# |13                                                 |2     |
# |21                                                 |1     |
# |14                                                 |2     |
# |74                                                 |809   |
# |HOSPITAL DE CLINICAS                               |1     |
# |76                                                 |1792  |
# |2                                                  |41    |
# +---------------------------------------------------+------+


tipo_de_leito = maior_custo_uti_por_municipio.groupBy("LEITO_TIPO").count().show(100, truncate=False)

# +-------------------------------------------------+------+
# |LEITO_TIPO                                       |count |
# +-------------------------------------------------+------+
# |J96.0 Insuf respirat aguda                       |1     |
# |Pediátricos                                      |87    |
# |C50.9 Mama NE                                    |4     |
# |R53   Mal estar fadiga                           |1     |
# |Obstétricos                                      |29    |
# |I46.9 Parada cardiaca NE                         |1     |
# |Leito Dia / Cirúrgicos                           |3512  |
# |Psiquiatria                                      |18    |
# |null                                             |901   |
# |C50.6 Porcao axilar da mama                      |1     |
# |C50.2 Quadrante super interno da mama            |1     |
# |Pneumologia Sanitária (Tisiologia)               |18    |
# |C50.8 Lesao invasiva da mama                     |1     |
# |A41.9 Septicemia NE                              |1     |
# |J12.8 Outr pneumonias virais                     |1     |
# |Crônicos                                         |4490  |
# |Leito Dia / Intercorrência Pós-Transplante       |4     |
# |Leito Dia / Geriatria                            |3     |
# |I26.9 Embolia pulmonar s/menc cor pulmonale agudo|1     |
# |1                                                |55    |
# |Clínico                                          |114910|
# |Cirúrgico                                        |230244|
# +-------------------------------------------------+------+

tipo_uti_valor = maior_custo_uti_por_municipio.groupBy("UTI_TIPO", "VALOR_UTI").count().show(20, truncate=False).asc()

# +--------+---------+-----+
# |UTI_TIPO|VALOR_UTI|count|
# +--------+---------+-----+
# |75      |41169.92 |1    |
# |76      |38655.88 |1    |
# |75      |29201.92 |1    |
# |76      |28991.91 |1    |
# |76      |28483.28 |1    |
# |75      |23936.0  |1    |
# |75      |22499.84 |1    |
# |76      |21362.46 |1    |
# |78      |21063.68 |1    |
# |75      |20584.96 |1    |
# |76      |20345.2  |1    |
# |75      |20106.24 |3    |
# |75      |19627.52 |4    |
# |75      |19388.16 |1    |
# |76      |19327.94 |1    |
# |75      |19148.8  |3    |
# |75      |18670.08 |1    |
# |76      |18310.67 |1    |
# |75      |18191.36 |1    |
# |75      |17808.37 |1    |
# +--------+---------+-----+
# only showing top 20 rows


obtos_por_classe = spark.sql("""select
OBITO_ID,
CAUSA_CID,
SEXO,
RACA_COR,
ESCOLARIDADE,
CNES_UF,
CNES_MUNICIPIO
FROM
obto """)

raca = obtos_por_classe.groupBy("RACA_COR").count().show()

# +--------+-----+
# |RACA_COR|count|
# +--------+-----+
# |       3|  692|
# |    null| 4920|
# |       5|  100|
# |       1|74571|
# |       4|36718|
# |       2| 9467|
# +--------+-----+


sexo = sexo_obtos_por_classe.groupBy("SEXO").count().show()


# +----+------+
# |SEXO| count|
# +----+------+
# |   0|    10|
# |   1|  1500|
# |   2|124958|
# +----+------+

escolaridade = obtos_por_classe.groupBy("ESCOLARIDADE").count().show(20, truncate=False).asc()

# +------------+-----+
# |ESCOLARIDADE|count|
# +------------+-----+
# |           3|23806|
# |           0|  700|
# |        null| 7842|
# |           5|15405|
# |           9|13944|
# |           1|10282|
# |           4|27470|
# |           2|27019|
# +------------+-----+


 local_valorProfissional_valor_uti = nv_internacao.groupBy("CNES" ,"VALOR_SERVICO_PROFISSIONAL", "VALOR_UTI").count().show(20, truncate=False).asc()

# +-------+--------------------------+---------+-----+
# |CNES   |VALOR_SERVICO_PROFISSIONAL|VALOR_UTI|count|
# +-------+--------------------------+---------+-----+
# |2802112|925.13                    |0.0      |67   |
# |2802112|55.65                     |0.0      |1    |
# |0003786|268.54                    |1436.16  |4    |
# |4028155|69.01                     |0.0      |1    |
# |2417189|10.88                     |0.0      |1    |
# |0003778|162.82                    |0.0      |2    |
# |2301318|248.35                    |0.0      |1    |
# |2004976|32.6                      |0.0      |3    |
# |2006197|810.94                    |5265.92  |1    |
# |2004976|140.02                    |0.0      |12   |
# |2007037|906.31                    |0.0      |3    |
# |2007037|2339.59                   |0.0      |1    |
# |2007037|1912.78                   |0.0      |1    |
# |2651394|134.44                    |0.0      |1    |
# |2480646|65.14                     |0.0      |2    |
# |2611686|2266.83                   |0.0      |78   |
# |2611686|1912.78                   |0.0      |121  |
# |2497654|143.89                    |0.0      |5    |
# |2499363|675.34                    |4308.48  |1    |
# |2564211|545.77                    |0.0      |1    |
# +-------+--------------------------+---------+-----+
# only showing top 20 rows


cid_hospital_uf_escolaridade  = df_obitos.groupBy("CAUSA_CID","CNES_UF", "ESCOLARIDADE", ).count().show(20, truncate=False)

# +---------+-------+------------+-----+
# |CAUSA_CID|CNES_UF|ESCOLARIDADE|count|
# +---------+-------+------------+-----+
# |C500     |PE     |2           |13   |
# |C509     |AC     |5           |27   |
# |C509     |AL     |3           |69   |
# |C509     |ES     |2           |310  |
# |C500     |MA     |9           |1    |
# |C505     |GO     |4           |2    |
# |C501     |PR     |4           |4    |
# |C505     |PA     |9           |2    |
# |C504     |BA     |5           |2    |
# |C503     |null   |5           |2    |
# |C509     |CE     |9           |257  |
# |C508     |BA     |5           |14   |
# |C501     |PA     |5           |2    |
# |C509     |PA     |3           |434  |
# |C501     |MT     |1           |2    |
# |C501     |PI     |1           |4    |
# |C500     |PR     |9           |2    |
# |C504     |MG     |9           |4    |
# |C509     |DF     |null        |31   |
# |C509     |PB     |null        |123  |
# +---------+-------+------------+-----+
# only showing top 20 rows



estado_obito_residencia = df_obitos.groupBy("OBITO_UF","RESIDENCIA_UF" ).count().show(80, truncate=False)

# +--------+-------------+-----+
# |OBITO_UF|RESIDENCIA_UF|count|
# +--------+-------------+-----+
# |SP      |ES           |2    |
# |AM      |PA           |2    |
# |SC      |SC           |4571 |
# |DF      |ES           |1    |
# |RS      |AL           |1    |
# |RO      |ES           |2    |
# |AP      |AP           |135  |
# |ES      |AL           |1    |
# |AM      |AM           |1215 |
# |MS      |SP           |7    |
# |AC      |AC           |215  |
# |RJ      |RS           |3    |
# |SP      |MG           |221  |
# |MA      |TO           |5    |
# |PE      |BA           |82   |
# |SC      |SP           |2    |
# |TO      |DF           |2    |
# |PE      |PA           |2    |
# |SP      |TO           |14   |
# |TO      |PA           |29   |
# |PE      |PB           |19   |
# |MS      |MS           |1456 |
# |MS      |MT           |2    |
# |ES      |MG           |20   |
# |MG      |BA           |5    |
# |DF      |TO           |2    |
# |PR      |MS           |28   |
# |SP      |MA           |7    |
# |DF      |PI           |4    |
# |MG      |MG           |11777|
# |MT      |RO           |3    |
# |MA      |MA           |1665 |
# |CE      |CE           |5014 |
# |SE      |RJ           |2    |
# |DF      |CE           |1    |
# |MT      |AL           |2    |
# |AM      |RR           |4    |
# |SP      |RS           |1    |
# |RS      |RS           |10108|
# |PR      |AM           |2    |
# |RN      |SP           |3    |
# |GO      |SP           |2    |
# |ES      |ES           |2269 |
# |CE      |PE           |8    |
# |PR      |GO           |4    |
# |RR      |RR           |110  |
# |PA      |PA           |2235 |
# |GO      |GO           |3217 |
# |SP      |SP           |32267|
# |GO      |CE           |2    |
# |SP      |BA           |28   |
# |BA      |SE           |8    |
# |MG      |RO           |1    |
# |DF      |AM           |1    |
# |MG      |SP           |10   |
# |PE      |SE           |2    |
# |SP      |PB           |4    |
# |DF      |DF           |1635 |
# |RN      |RN           |1886 |
# |TO      |TO           |460  |
# |RS      |PR           |2    |
# |CE      |BA           |2    |
# |MG      |RJ           |37   |
# |SP      |MT           |46   |
# |DF      |MT           |2    |
# |MG      |CE           |2    |
# |SE      |BA           |16   |
# |AC      |RO           |2    |
# |SE      |SP           |2    |
# |AL      |AL           |1275 |
# |SP      |PA           |24   |
# |PA      |AM           |2    |
# |PA      |AP           |18   |
# |BA      |PI           |2    |
# |AC      |AM           |4    |
# |PR      |DF           |1    |
# |SP      |DF           |11   |
# |DF      |RJ           |2    |
# |RO      |AM           |4    |
# |SC      |RS           |2    |
# +--------+-------------+-----+


estado_raca = df_obitos.groupBy("OBITO_UF","CAUSA_CID_DESC" ).count().show(30, truncate=False)

# +--------+-------------------------------------+-----+
# |OBITO_UF|CAUSA_CID_DESC                       |count|
# +--------+-------------------------------------+-----+
# |SP      |C50.3 Quadrante infer interno da mama|13   |
# |CE      |C50.8 Lesao invasiva da mama         |298  |
# |AC      |C50.1 Porcao central da mama         |2    |
# |BA      |C50.9 Mama NE                        |6474 |
# |SC      |C50.2 Quadrante super interno da mama|7    |
# |AM      |C50.0 Mamilo e areola                |10   |
# |PR      |C50.1 Porcao central da mama         |18   |
# |PB      |C50.0 Mamilo e areola                |68   |
# |RS      |C50.8 Lesao invasiva da mama         |426  |
# |MT      |C50.4 Quadrante super externo da mama|3    |
# |PE      |C50.5 Quadrante infer externo da mama|2    |
# |MG      |C50.9 Mama NE                        |11412|
# |SC      |C50.4 Quadrante super externo da mama|5    |
# |SE      |C50.4 Quadrante super externo da mama|1    |
# |RS      |C50.4 Quadrante super externo da mama|17   |
# |RJ      |C50.2 Quadrante super interno da mama|6    |
# |SP      |C50.4 Quadrante super externo da mama|7    |
# |PR      |C50.0 Mamilo e areola                |37   |
# |AL      |C50.4 Quadrante super externo da mama|4    |
# |MS      |C50.9 Mama NE                        |1437 |
# |RN      |C50.0 Mamilo e areola                |24   |
# |PB      |C50.9 Mama NE                        |1676 |
# |GO      |C50.4 Quadrante super externo da mama|4    |
# |RN      |C50.4 Quadrante super externo da mama|6    |
# |GO      |C50.1 Porcao central da mama         |1    |
# |GO      |C50.9 Mama NE                        |3214 |
# |PR      |C50.2 Quadrante super interno da mama|6    |
# |PR      |C50.5 Quadrante infer externo da mama|1    |
# |SP      |C50.8 Lesao invasiva da mama         |806  |
# |PE      |C50.0 Mamilo e areola                |87   |
# +--------+-------------------------------------+-----+
# only showing top 30 rows


raca_causa = df_obitos.groupBy("RACA_COR","CAUSA_CID_DESC" ).count().show(30, truncate=False)

# +--------+-------------------------------------+-----+
# |RACA_COR|CAUSA_CID_DESC                       |count|
# +--------+-------------------------------------+-----+
# |4       |C50.0 Mamilo e areola                |423  |
# |2       |C50.2 Quadrante super interno da mama|5    |
# |3       |C50.1 Porcao central da mama         |3    |
# |4       |C50.2 Quadrante super interno da mama|16   |
# |3       |C50.8 Lesao invasiva da mama         |12   |
# |null    |C50.9 Mama NE                        |4695 |
# |null    |C50.4 Quadrante super externo da mama|8    |
# |1       |C50.8 Lesao invasiva da mama         |2116 |
# |2       |C50.9 Mama NE                        |9074 |
# |5       |C50.8 Lesao invasiva da mama         |4    |
# |null    |C50.1 Porcao central da mama         |20   |
# |4       |C50.1 Porcao central da mama         |162  |
# |4       |C50.5 Quadrante infer externo da mama|9    |
# |null    |C50.6 Porcao axilar da mama          |2    |
# |4       |C50.3 Quadrante infer interno da mama|8    |
# |1       |C50.0 Mamilo e areola                |635  |
# |null    |C50.0 Mamilo e areola                |46   |
# |2       |C50.6 Porcao axilar da mama          |4    |
# |4       |C50.6 Porcao axilar da mama          |16   |
# |1       |C50.4 Quadrante super externo da mama|76   |
# |null    |C50.2 Quadrante super interno da mama|2    |
# |3       |C50.9 Mama NE                        |669  |
# |null    |C50.8 Lesao invasiva da mama         |146  |
# |2       |C50.3 Quadrante infer interno da mama|6    |
# |2       |C50.4 Quadrante super externo da mama|17   |
# |1       |C50.3 Quadrante infer interno da mama|26   |
# |2       |C50.5 Quadrante infer externo da mama|2    |
# |1       |C50.5 Quadrante infer externo da mama|24   |
# |5       |C50.9 Mama NE                        |92   |
# |4       |C50.8 Lesao invasiva da mama         |1128 |
# +--------+-------------------------------------+-----+
# only showing top 30 rows



nv_internacao.write.format("csv").save("/tmp/nv_internacao/" + "nv_internacao.csv")
maior_custo_uti_por_municipio.write.format("csv").save("/tmp/maior_custo_uti_por_municipio/" + "maior_custo_uti_por_municipio.csv")
tipo_uti.write.format("csv").save("/tmp/tipo_uti/" + "tipo_uti.csv")
tipo_de_leito.write.format("csv").save("/tmp/tipo_de_leito/" + "tipo_de_leito.csv")
tipo_uti_valor.write.format("csv").save("/tmp/tipo_uti_valor/" + "tipo_uti_valor.csv")
raca.write.format("csv").save("/tmp/raca/" + "raca.csv")
sexo.write.format("csv").save("/tmp/sexo/" + "sexo.csv")
escolaridade.write.format("csv").save("/tmp/escolaridade/" + "escolaridade.csv")
local_valorProfissional_valor_uti.write.format("csv").save("/tmp/local_valorProfissional_valor_uti/" + "local_valorProfissional_valor_uti.csv")
cid_hospital_uf_escolaridade.write.format("csv").save("/tmp/cid_hospital_uf_escolaridade/" + "cid_hospital_uf_escolaridade.csv")
estado_obito_residencia.write.format("csv").save("/tmp/estado_obito_residencia/" + "estado_obito_residencia.csv")
estado_raca.write.format("csv").save("/tmp/estado_raca/" + "estado_raca.csv")
raca_causa.write.format("csv").save("/tmp/raca_causa/" + "raca_causa.csv")
