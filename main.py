conda install pyspark
try:
    from pyspark import SparkContext, SparkConf
    from pyspark.sql import SparkSession
except ImportError as e:
    printmd('<<<<<!!!!! Please restart your kernel after installing Apache Spark !!!!!>>>>>')
#create basic spark session
    sc = SparkContext.getOrCreate(SparkConf().setMaster("local[*]"))

spark = SparkSession \
    .builder \
    .getOrCreate()
    #get data set
    !wget https://author.skills.network/quicklabs/Data%20Science%20in%20Insurance.%20Basic%20statistical%20analysis.csv UAInsurance.csv
    #contruct dataset with schema
    from pyspark.sql.types import *
schema = StructType([
                      StructField("Year",IntegerType()),\ 
                      StructField("Quarter",StringType()),\
                      StructField("Premiums",DoubleType()),\
                      StructField("Claims ",DoubleType()),\
                      StructField("Loss",DoubleType()),\
                      ])
df = (spark.read.format("csv").options(header="true").schema(schema).load('UAInsurance.csv'))
df.createOrReplaceTempView('insurance')
#look at dataset
df.show()
#visualize data with matplotlib library
import matplotlib.pyplot as plt
import numpy as np
#collect the data from the Spark dataframe first and convert it into a Numpy array.
date=np.array(spark.sql('SELECT Quarter FROM insurance').collect())[:,0]
loss=np.array(spark.sql('SELECT Loss FROM insurance').collect())[:,0]
#plot insurance loss ratio
plt.rcParams["figure.figsize"] = (20,4)
plt.plot(date,loss)
#calculate the minimal global insurance loss ratio
def minLoss():
  def correlationLoss(): return spark.sql("SELECT min(Loss) as minloss from insurance").first().minloss
print("Mean insurance loss ratio for Ukraine in 2012-2019yy is", meanLoss()*100,'%')
#calculate the mean value of the loss ratio
def meanLoss():
    return spark.sql("SELECT mean(Loss) as minloss from insurance").first().minloss
    #maximum of the loss ratio 
    def maxLoss():
return spark.sql("SELECT max(Loss) as maxloss from insurance").first().maxloss
print("Maximal insurance loss ratio for Ukraine in 2012-2019yy is", maxLoss()*100,'%')
#standard deviation
def sdLoss():
return spark.sql("SELECT stdev_pop(Loss) as sdloss from insurance").first().sdloss
print("Maximal insurance loss ratio for Ukraine in 2012-2019yy is", maxLoss()*100,'%')
#skewness
def skewLoss():    
    return spark.sql("""
SELECT 
    (
        1/COUNT(Loss)
    ) *
    SUM (
        POWER(Loss-%s,3)/POWER(%s,3)
    )

as skloss from insurance 
                    """ %(meanLoss(),sdLoss(),)).
                    first().skloss
                    print("Skewness  of insurance loss ratio for Ukraine in 2012-2019yy is", skewLoss())
#kurtosis
def kurtosisLoss():    
        return spark.sql("""
SELECT 
    (
        1/COUNT(Loss)
    ) *
    SUM (
        POWER(Loss-%s,4)/POWER(%s,4)
    )
as kloss from insurance
                    """ %(meanLoss(),sdLoss())).first().kloss
                    print("Kurtosis of insurance loss ratio for Ukraine in 2012-2019yy is", kurtosisLoss())
#correlation between premiums and claims
def correlationLoss():
    return spark.sql("SELECT corr(premiums, claims ) as corr from insurance").first().corr 
    print("Correlation between premiums and claims of Ukrainian insurance companies in 2012-2019yy is", correlationLoss())