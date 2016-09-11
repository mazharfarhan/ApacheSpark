from pyspark.sql.types import IntegerType

df = sqlContext.createDataFrame(data,['name', 'age'])
# create a user defined transformation that returns the double of integer value.
doubled = udf(lamda s: s*2, Integer())
df2 = df.select(df.name, doubled(df.age).alias('age'))

#Transformation to get age that is greater than 3
df3 = df2.filter(df2.age > 3)
