# Databricks notebook source
# MAGIC %md
# MAGIC # Importing modules

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window


# COMMAND ----------

# MAGIC %md
# MAGIC # Input File Paths

# COMMAND ----------

charges='dbfs:/FileStore/Charges_use.csv'
endorse='dbfs:/FileStore/Endorse_use.csv'
restrict='dbfs:/FileStore/Restrict_use.csv'
damages='dbfs:/FileStore/Damages_use.csv'
person='dbfs:/FileStore/Primary_Person_use.csv'
unit='dbfs:/FileStore/Units_use.csv'

# COMMAND ----------

# MAGIC %md
# MAGIC # Class to create dataframes

# COMMAND ----------

class Dataframe:
    '''This class takes path as
    an input and creates an object whose df attribute is a dataframe'''
    
    def __init__(self,path,name):
        '''This function initializes variable and creates dataframe'''
        self.path=path
        self.name=name
        self.df=self.createdf()
        self.distinct_records()
        
        
    def createdf(self):
        '''This function returns a dataframe from reading file from path'''
        try:
            if self.path is None:
                return
            return spark.read.format("csv").option("header","true").option("mode","permissive").option("inferSchema" , "true").load(self.path)
        
        except Exception as e:
            print(e)
    
    def distinct_records(self):
        '''This function takes distinct records of any dataframe'''
        
        self.df=self.df.distinct()
        

# COMMAND ----------

# MAGIC %md
# MAGIC # Create Dataframes using Input file paths

# COMMAND ----------

charges=Dataframe(charges,'charges')
endorse=Dataframe(endorse,'endorse')
restrict=Dataframe(restrict,'restrict')
damages=Dataframe(damages,'damages')
person=Dataframe(person,'person')
units=Dataframe(unit,'units')

# COMMAND ----------

# MAGIC %md
# MAGIC # Create Solution class for all questions

# COMMAND ----------

class Solution:
  '''This class gives answers to all questions by printing results'''
    
  def __init__(self,paths):
      '''This function is used to initialize paths which has value of path for every problem'''
      
      self.paths=paths
    
  def write(self,df,func):
    
      '''This function is used to write results in output path'''
      
      try:
        
        path= self.paths.get(func)
        
        df.write.format("parquet").mode('overwrite').save(path)
        print('Result of '+func+' successfully written at path '+path)
            
      except Exception as e:
        print(e)
    
  def analytics1(self):
    
      '''1.	Analytics 1: Find the number of crashes (accidents) in which number of persons killed are male?
      Approach: In person_df, filter for male and injury severity as killed and count distinct crash id's
      '''
      
      try:
        
        analytics1_final_df=person.df\
                                .filter((col('PRSN_GNDR_ID')=='MALE') & (col('PRSN_INJRY_SEV_ID')=='KILLED'))\
                                .agg(countDistinct(col('CRASH_ID')).alias('Count'))
        
        analytics1_final_df.show(truncate=False)
        self.write(analytics1_final_df,'analytics1')
            
      except Exception as e:
        print(e)
        
        
  def analytics2(self):
    
    '''2.	Analysis 2: How many two wheelers are booked for crashes? 
       Approach: Find VEH_BODY_STYL_ID as MOTORCYCLE and count distinct VIN as unique vehicle number  '''
    
    try:
      analytics2_final_df= units.df\
                                .filter(col('VEH_BODY_STYL_ID')=='MOTORCYCLE')\
                                .select(col('VIN')).agg(countDistinct(col('VIN')).alias('count'))
      
      analytics2_final_df.show(truncate=False)
      self.write(analytics2_final_df,'analytics2')
            
    except Exception as e:
      print(e)    
        
  def analytics3(self):
    
    '''3.	Analysis 3: Which state has highest number of accidents in which females are involved? .
       Approach: Filter all crash caused by female and group based on states. Count distinct crash and order by desc'''
    
    try:
      
      analytics3_final_df=person.df\
                                .filter(col('PRSN_GNDR_ID')=='FEMALE')\
                                .groupBy(col('DRVR_LIC_STATE_ID'))\
                                .agg(countDistinct(col('CRASH_ID')).alias("Count"))\
                                .orderBy(col('Count').desc())\
                                .limit(1).select(col('DRVR_LIC_STATE_ID').alias('State'))
      
      analytics3_final_df.show(truncate=False)
      self.write(analytics3_final_df,'analytics3')
            
    except Exception as e:
      print(e)   
            
            
  def analytics4(self):
    
    '''4.	Analysis 4: Which are the Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death
       Approach:  Group by VEH_MAKE_ID and count total injury including deaths and rank 5 to 15th '''
    
    try:
      windowspec=Window.orderBy(col('total').desc())
      analytics4_final_df=units.df.groupBy(col('VEH_MAKE_ID'))\
                                .agg((sum(col('TOT_INJRY_CNT'))+sum(col('DEATH_CNT'))).alias('total'))\
                                .withColumn('RNK',row_number().over(windowspec))\
                                .filter(col('RNK').between(5,15))
      
      analytics4_final_df.show(truncate=False)
      self.write(analytics4_final_df,'analytics4')
            
    except Exception as e:
      print(e) 
            
            
  def analytics5(self):
    
    '''5.	Analysis 5: For all the body styles involved in crashes, mention the top ethnic user group of each unique body style 
       Approach: Join units df with person df to get ethinic group and aggregate based on distinct crashID and select top most record'''
    
    try:
      windowspec=Window.partitionBy(col('VEH_BODY_STYL_ID')).orderBy(col('Count').desc())
      analytics5_final_df= units.df.join(person.df, [units.df.CRASH_ID==person.df.CRASH_ID,units.df.UNIT_NBR==person.df.UNIT_NBR],'inner')\
                                .select(col('VEH_BODY_STYL_ID'),col('PRSN_ETHNICITY_ID'),units.df.CRASH_ID)\
                                .groupBy(col('VEH_BODY_STYL_ID'),col('PRSN_ETHNICITY_ID'))\
                                .agg(countDistinct(units.df.CRASH_ID).alias('Count'))\
                                .withColumn('RNK',row_number().over(windowspec))\
                                .filter(col('RNK')==1).select(col('VEH_BODY_STYL_ID'),col('PRSN_ETHNICITY_ID'))
            
      
      analytics5_final_df.show(truncate=False)
      self.write(analytics5_final_df,'analytics5')
            
    except Exception as e:
      print(e) 
            
    
  def analytics6(self):
    
    '''6.	Analysis 6: Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code)
     Approach: Filter all cases of drinking from unitsv and join with person df to get drier zip codes'''
    
    try:
      alcohol_df=units.df.filter(col('CONTRIB_FACTR_1_ID').like('%ALCOHOL%') | 
                                       col('CONTRIB_FACTR_1_ID').like('%DRINKING%') |
                                      col('CONTRIB_FACTR_2_ID').like('%ALCOHOL%') |
                                      col('CONTRIB_FACTR_2_ID').like('%DRINKING%') |
                                      col('CONTRIB_FACTR_P1_ID').like('%ALCOHOL%') |
                                      col('CONTRIB_FACTR_P1_ID').like('%DRINKING%') ) \
                        .select(col('CRASH_ID'),col('CONTRIB_FACTR_1_ID'),col('CONTRIB_FACTR_2_ID'),col('CONTRIB_FACTR_P1_ID'))
            
      analytics6_final_df=person.df.join(alcohol_df,[alcohol_df.CRASH_ID==person.df.CRASH_ID],'inner')\
                                .filter((col('PRSN_ALC_RSLT_ID')=='Positive') & (col('DRVR_ZIP').isNotNull()))\
                                .groupBy(col('DRVR_ZIP'))\
                                .agg(count("*").alias('count'))\
                                .orderBy(col("count").desc())\
                                .limit(5).select(col('DRVR_ZIP'))
            
      
      analytics6_final_df.show(truncate=False)
      self.write(analytics6_final_df,'analytics6')
            
    except Exception as e:
      print(e) 
            
                                
  def analytics7(self): 
    
    '''7.	Analysis 7: Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance
    Approach: left join units with damages and filter on no damages and VEH_DMAG_SCL>4'''
    
    try:
      analytics7_final_df=units.df.join(damages.df,[units.df.CRASH_ID==damages.df.CRASH_ID],'left')\
                                .where("Damaged_property is null AND \
                                        split(VEH_DMAG_SCL_1_ID,' ')[1]>4 AND \
                                        split(VEH_DMAG_SCL_2_ID,' ')[1]>4 AND \
                                        FIN_RESP_TYPE_ID like '%INSURANCE%'")\
                                .select(units.df.CRASH_ID,"VEH_DMAG_SCL_1_ID","VEH_DMAG_SCL_2_ID","FIN_RESP_TYPE_ID")\
                                .agg(countDistinct(units.df.CRASH_ID).alias("Distinct_CrashID_Count"))
      
      analytics7_final_df.show(truncate=False)
      self.write(analytics7_final_df,'analytics7')
            
    except Exception as e:
      print(e) 
            
            
  def analytics8(self):
    
    '''8.	Analysis 8: Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, used top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences (to be deduced from the data)'''
    
    try:
      top_25_offense=units.df.groupBy(col('VEH_LIC_STATE_ID'))\
                            .agg(countDistinct("CRASH_ID").alias("total_offences"))\
                            .orderBy(col("total_offences").desc())\
                            .limit(25)\
                            .select("VEH_LIC_STATE_ID")\
                            .rdd\
                            .map(lambda x:x[0])\
                            .collect()
            
      top_10_colour= units.df.filter(col('VEH_COLOR_ID')!='NA')\
                            .groupBy(col('VEH_COLOR_ID'))\
                            .agg(count(col("CRASH_ID")).alias("total_colors_being_used"))\
                            .orderBy(col("total_colors_being_used").desc())\
                            .limit(10)\
                            .select("VEH_COLOR_ID")\
                            .rdd\
                            .map(lambda x:x[0])\
                            .collect()
            
            
      charges_with_speed=charges.df.where("lower(Charge) like '%speed%'").select("CRASH_ID","CHARGE")
            
      Units_with_filtered_conditions=units.df\
                                        .filter(col("VEH_COLOR_ID").isin(*top_10_colour) & col("VEH_LIC_STATE_ID").isin(*top_25_offense))\
                                        .select("CRASH_ID","VEH_MAKE_ID","VEH_COLOR_ID","VEH_LIC_STATE_ID")
            
      U=Units_with_filtered_conditions.alias("U")
      C=charges_with_speed.alias("C")
      analytics8_final_df=U.join(C,[U.CRASH_ID==C.CRASH_ID],"INNER")\
                                .groupBy("VEH_MAKE_ID")\
                                .agg(count("*").alias("Count_for_each_Veh_Makers"))\
                                .orderBy(col("Count_for_each_Veh_Makers").desc())\
                                .limit(5)
            
      
      analytics8_final_df.show(truncate=False)
      self.write(analytics8_final_df,'analytics8')
            
    except Exception as e:
      print(e) 
            
            

# COMMAND ----------

# MAGIC %md
# MAGIC # Output Paths

# COMMAND ----------

analytics1_output_path='dbfs:/FileStore/output/analytics1.csv'
analytics2_output_path='dbfs:/FileStore/output/analytics2.csv'
analytics3_output_path='dbfs:/FileStore/output/analytics3.csv'
analytics4_output_path='dbfs:/FileStore/output/analytics4.csv'
analytics5_output_path='dbfs:/FileStore/output/analytics5.csv'
analytics6_output_path='dbfs:/FileStore/output/analytics6.csv'
analytics7_output_path='dbfs:/FileStore/output/analytics7.csv'
analytics8_output_path='dbfs:/FileStore/output/analytics8.csv'

paths={'analytics1':analytics1_output_path,
      'analytics2':analytics2_output_path,
      'analytics3':analytics3_output_path,
      'analytics4':analytics4_output_path,
      'analytics5':analytics5_output_path,
      'analytics6':analytics6_output_path,
      'analytics7':analytics7_output_path,
      'analytics8':analytics8_output_path
      }

# COMMAND ----------

# MAGIC %md
# MAGIC # Main function to execute all solutions

# COMMAND ----------

def main():
  '''Execute all methods of solution class'''
  Test= Solution(paths)
  print('Result of analytics1 :')
  Test.analytics1()
  print('Result of analytics2 :')
  Test.analytics2()
  print('Result of analytics3 :')
  Test.analytics3()
  print('Result of analytics4 :')
  Test.analytics4()
  print('Result of analytics5 :')
  Test.analytics5()
  print('Result of analytics6 :')
  Test.analytics6()
  print('Result of analytics7 :')
  Test.analytics7()
  print('Result of analytics8 :')
  Test.analytics8()
  
  
  

# COMMAND ----------

# MAGIC %md
# MAGIC # Execute all

# COMMAND ----------

if __name__=='__main__':
  main()

# COMMAND ----------


