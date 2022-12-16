from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.conf import SparkConf
import sys
from pyspark.sql.types import DecimalType
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
import pyspark.sql.utils
from pyspark.context import SparkContext
sc = SparkContext.getOrCreate();
spark = SparkSession(sc)
df=spark.read.option("multiline","true").format("JSON").load("s3://nitesh-landingzone-batch07/constantjson/constant.json")
spark.sparkContext.addPyFile("s3://nitesh-landingzone-batch07/dependencies/delta-core_2.12-0.8.0.jar")
from delta import *
class SparkJob:
    def __init__(self,source,destination):
        self.source=source
        self.destination=destination
        
    def readRawzone(self):
        df=spark.read.option("header",True).option("inferSchema",True).parquet(self.source)
        return df
    def maskingcolumns(self,df,column1):
        for i in column1:
            df=df.withColumn("masked_"+i, sha2(concat_ws("||", df[i]), 256))
        return df        
    def transformationSource(self,df,column1):
        join_udf = udf(lambda x: str(x).replace('_',','))
        df=df.withColumn("location_source", join_udf(col(column1)))
        return df

    def transformDecimalPrecison(self,df,column1,precision):
        for i in range(1,len(column1)):
            df=df.withColumn(column1[i], df[column1[i]].cast(DecimalType(precision)))
        return df    
    def Partitionandstagingzone(self,df):
        df.withColumn("year", year(col("date"))).withColumn("day", dayofmonth(col("date"))).write.partitionBy('year','month','day').mode('Overwrite').parquet(self.destination)
        
    def lookup_dataset(self,df,cols,path):
        lookup_location = path
        pii_cols =  cols
        datasetName = 'lookup'
        df_source = df.withColumn("begin_date",f.current_date())
        df_source = df_source.withColumn("update_date",f.lit("null"))
        
        pii_cols = [i for i in pii_cols if i in df.columns]
            
        columns_needed = []
        insert_dict = {}
        for col in pii_cols:
            if col in df.columns:
                columns_needed += [col,"masked_"+col]
        source_columns_used = columns_needed + ['begin_date','update_date']
        #print(source_columns_used)

        df_source = df_source.select(*source_columns_used)

        try:
            targetTable = DeltaTable.forPath(spark,lookup_location)
            delta_df = targetTable.toDF()
        except pyspark.sql.utils.AnalysisException:
            print('Table does not exist')
            df_source = df_source.withColumn("flag_active",f.lit("true"))
            df_source.write.format("delta").mode("overwrite").save(lookup_location)
            print('Table Created Sucessfully!')
            targetTable = DeltaTable.forPath(spark,lookup_location)
            delta_df = targetTable.toDF()
            delta_df.show(100)

        for i in columns_needed:
            insert_dict[i] = "updates."+i
            
        insert_dict['begin_date'] = f.current_date()
        insert_dict['flag_active'] = "True" 
        insert_dict['update_date'] = "null"
        
        #print(insert_dict)
        
        _condition = datasetName+".flag_active == true AND "+" OR ".join(["updates."+i+" <> "+ datasetName+"."+i for i in [x for x in columns_needed if x.startswith("masked_")]])
        
        #print(_condition)
        
        column = ",".join([datasetName+"."+i for i in [x for x in pii_cols]]) 
        
        #print(column)

        updatedColumnsToInsert = df_source.alias("updates").join(targetTable.toDF().alias(datasetName), pii_cols).where(_condition) 
        
        #print(updatedColumnsToInsert)

        stagedUpdates = (
          updatedColumnsToInsert.selectExpr('NULL as mergeKey',*[f"updates.{i}" for i in df_source.columns]).union(df_source.selectExpr("concat("+','.join([x for x in pii_cols])+") as mergeKey", "*")))

        targetTable.alias(datasetName).merge(stagedUpdates.alias("updates"),"concat("+str(column)+") = mergeKey").whenMatchedUpdate(
            condition = _condition,
            set = {                  # Set current to false and endDate to source's effective date."flag_active" : "False",
            "update_date" : f.current_date()
          }
        ).whenNotMatchedInsert(
          values = insert_dict
        ).execute()

        for i in pii_cols:
            df = df.drop(i).withColumnRenamed("masked_"+i, i)

        return df

data=['active','viwership']
for i in data:
    obj=SparkJob(df.first()[0]['rawzone_path']+i+"/",df.first()[0]['staging_path']+i+"/")
    try:
        data=obj.readRawzone()
    except:
        print("bucket not found")
        continue

    data_masked=obj.maskingcolumns(data,df.first()["masking"][i])
    data_transformed=obj.transformationSource(data_masked,df.first()["transformation"][0][0][0])
    data_casted =obj.transformDecimalPrecison(data_transformed,df.first()["transformation"][i]['columns'],int(df.first()["transformation"][0]['precision']))
    obj.Partitionandstagingzone(data_casted)
    print("Data Transformed",i)
    lookup_data=obj.lookup_dataset(data_casted,df.first()['lookup-dataset']['pii-cols-'+i],df.first()['lookup-dataset']['data-location-'+i])
    obj.Partitionandstagingzone(lookup_data)
    print("Data lookup_data",i)