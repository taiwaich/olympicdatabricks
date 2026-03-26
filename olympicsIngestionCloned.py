# Databricks notebook source
dbutils.fs.rm('/Volumes/workspace/mydata/olympics/parquet/summerolympians', True)
dbutils.fs.rm('/Volumes/workspace/mydata/olympics/parquet/olympics', True)
dbutils.fs.rm('/Volumes/workspace/mydata/olympics/parquet/noc', True)

# COMMAND ----------

from pyspark.sql.functions import *

file_type = "csv"
#csv options
inferSchema = "true"
firstRowIsHeader = "true"
delimiter = ","
filestore="/Volumes/workspace/mydata/olympics/"
filestore1="/Volumes/workspace/mydata/olympics/summer-games"

# COMMAND ----------

def processDF(fileLocation):
  return(spark.read.csv(fileLocation, inferSchema=inferSchema, header=firstRowIsHeader, sep=delimiter))

# COMMAND ----------

aDF1=processDF(filestore1+"/1.csv")
aDF2=processDF(filestore1+"/2.csv")
aDF3=processDF(filestore1+"/3.csv")
aDF4=processDF(filestore1+"/4.csv")
aDF5=processDF(filestore1+"/5.csv")
aDF6=processDF(filestore1+"/6.csv")
aDF7=processDF(filestore1+"/7.csv")
aDF8=processDF(filestore1+"/8.csv")
aDF10=processDF(filestore1+"/10.csv")
aDF12=processDF(filestore1+"/12.csv")
aDF14=processDF(filestore1+"/14.csv")
aDF16=processDF(filestore1+"/16.csv")
aDF18=processDF(filestore1+"/18.csv")
aDF20=processDF(filestore1+"/20.csv")
aDF23=processDF(filestore1+"/23.csv")
aDF25=processDF(filestore1+"/25.csv")
aDF27=processDF(filestore1+"/27.csv")
aDF29=processDF(filestore1+"/29.csv")
aDF31=processDF(filestore1+"/31.csv")
aDF33=processDF(filestore1+"/33.csv")
aDF35=processDF(filestore1+"/35.csv")
aDF37=processDF(filestore1+"/37.csv")
aDF39=processDF(filestore1+"/39.csv")
aDF42=processDF(filestore1+"/42.csv")
aDF44=processDF(filestore1+"/44.csv")
aDF46=processDF(filestore1+"/46.csv")
aDF48=processDF(filestore1+"/48.csv")
aDF50=processDF(filestore1+"/50.csv")
aDF52=processDF(filestore1+"/52.csv")

# COMMAND ----------

AthleteDF=aDF1.union(aDF2).union(aDF3).union(aDF4).union(aDF5).union(aDF6).union(aDF7)

# COMMAND ----------

display(AthleteDF)

# COMMAND ----------

AthleteDF1=AthleteDF.union(aDF8).union(aDF10).union(aDF12).union(aDF14).union(aDF16)

# COMMAND ----------

display(AthleteDF1)

# COMMAND ----------

AthleteDF2=AthleteDF1.union(aDF18).union(aDF20).union(aDF23).union(aDF25).union(aDF27).union(aDF29).union(aDF31).union(aDF33).union(aDF35).union(aDF37).union(aDF39).union(aDF42).union(aDF44).union(aDF46).union(aDF48).union(aDF50).union(aDF52)

# COMMAND ----------

AthleteDF2 = AthleteDF2.drop("age")

# COMMAND ----------

display(AthleteDF2)

# COMMAND ----------

#AthleteDF2.write.save("/FileStore/parquet/summergamesathletes")

# COMMAND ----------

citiesDF=processDF(filestore+"cities.csv")
display(citiesDF)

# COMMAND ----------

eventsDF=processDF(filestore+"events.csv")
display(eventsDF)

# COMMAND ----------

gamesDF=processDF(filestore+"games.csv")
display(gamesDF)
nocDF=processDF(filestore+"noc-regions.csv")
display(nocDF)
sportsDF=processDF(filestore+"sports.csv")
display(sportsDF)
teamsDF=processDF(filestore+"teams.csv")
display(teamsDF)

# COMMAND ----------

olympicsDF = gamesDF.join(citiesDF, gamesDF['city_id']==citiesDF['city_id']).select(col("Year"), col("Season"),col("City"),col("Country"))
display(olympicsDF)

# COMMAND ----------

display(olympicsDF)

# COMMAND ----------

olympicsDF.write.mode("overwrite").parquet(filestore+"parquet/olympics")

# COMMAND ----------

athleteDF=processDF(filestore+"athletes-data.csv")
display(athleteDF)

# COMMAND ----------

olympianDF=athleteDF.join(AthleteDF2, athleteDF['id']==AthleteDF2['athlete_id'])\
                    .join(gamesDF, gamesDF['game_id'] == AthleteDF2['game_id'])\
                    .join(citiesDF, citiesDF['city_id'] == gamesDF['city_id'])\
                    .join(sportsDF, sportsDF['sport_id'] == AthleteDF2['sport_id'])\
                    .join(teamsDF, teamsDF['team_id'] == AthleteDF2['team_id'])\
                    .join(eventsDF, eventsDF['event_id']==AthleteDF2['event_id'])

# COMMAND ----------

display(olympianDF)

# COMMAND ----------

olympicAthletes=olympianDF.select(col("name"), col("medal"),col("city"),col("noc"),col("year"),col("sport"),col("event"),col("gender"),col("height"),col("weight"))

# COMMAND ----------

display(olympicAthletes)

# COMMAND ----------

olympicAthletes=(olympicAthletes.withColumn('noc', regexp_replace('noc', 'URS', 'RUS')).withColumn('noc', regexp_replace('noc', 'GDR', 'DEU')).withColumn('noc', regexp_replace('noc', 'FRG', 'DEU')).withColumn('noc', regexp_replace('noc', 'TCH', 'CZE')).withColumn('noc', regexp_replace('noc', 'NED', 'NLD')).withColumn('noc', regexp_replace('noc', 'SUI', 'CHE')).withColumn('noc', regexp_replace('noc', 'EUN', 'RUS')).withColumn('noc', regexp_replace('noc', 'GER', 'DEU')).withColumn('noc', regexp_replace('noc', 'BUL', 'BGR')).withColumn('noc', regexp_replace('noc', 'CRO', 'HRV')).withColumn('noc', regexp_replace('noc', 'SLO', 'SVN')).withColumn('noc', regexp_replace('noc', 'LAT', 'LVA')).withColumn('noc', regexp_replace('noc', 'GRE', 'GRC')).withColumn('noc', regexp_replace('noc', 'CHI', 'CHL')).withColumn('noc', regexp_replace('noc', 'DEN', 'DNK')).withColumn('noc', regexp_replace('noc', 'ISV', 'VIR')).withColumn('noc', regexp_replace('noc', 'SCG', 'SRB')).withColumn('noc', regexp_replace('noc', 'MON', 'MCO')).withColumn('noc', regexp_replace('noc', 'TPE', 'TWN')).withColumn('noc', regexp_replace('noc', 'RSA', 'ZAF')).withColumn('noc', regexp_replace('noc', 'MGL', 'MNG')).withColumn('noc', regexp_replace('noc', 'LIB', 'LBN')).withColumn('noc', regexp_replace('noc', 'NEP', 'NPL')).withColumn('noc', regexp_replace('noc', 'IRI', 'IRN')).withColumn('noc', regexp_replace('noc', 'CRC', 'CRI')).withColumn('noc', regexp_replace('noc', 'FIJ', 'FJI')).withColumn('noc', regexp_replace('noc', 'BER', 'BMU')).withColumn('noc', regexp_replace('noc', 'ALG', 'DZA')).withColumn('noc', regexp_replace('noc', 'MAD', 'MDG')).withColumn('noc', regexp_replace('noc', 'POR', 'PRT')).withColumn('noc', regexp_replace('noc', 'CAY', 'CYM')).withColumn('noc', regexp_replace('noc', 'TOG', 'TGO')).withColumn('noc', regexp_replace('noc', 'ZIM', 'ZWE')).withColumn('noc', regexp_replace('noc', 'IVB', 'VGB')).withColumn('noc', regexp_replace('noc', 'TGA', 'TON')).withColumn('noc', regexp_replace('noc', 'PAR', 'PRY')).withColumn('noc', regexp_replace('noc', 'PHI', 'PHL')).withColumn('noc', regexp_replace('noc', 'PUR', 'PRI')).withColumn('noc', regexp_replace('noc', 'URU', 'URY')).withColumn('noc', regexp_replace('noc', 'ASA', 'ASM')).withColumn('noc', regexp_replace('noc', 'AHO', 'CUW')).withColumn('noc', regexp_replace('noc', 'YUG', 'SRB')).withColumn('noc', regexp_replace('noc', 'HON', 'HND')).withColumn('noc', regexp_replace('noc', 'GUA', 'GTM')).withColumn('noc', regexp_replace('noc', 'BAH', 'BHS')).withColumn('noc', regexp_replace('noc', 'UAE', 'ARE')).withColumn('noc', regexp_replace('noc', 'NGR', 'NGA')).withColumn('noc', regexp_replace('noc', 'INA', 'IDN')).withColumn('noc', regexp_replace('noc', 'VIE', 'VNM')).withColumn('noc', regexp_replace('noc', 'GRN', 'GRD')).withColumn('noc', regexp_replace('noc', 'MAS', 'MYS')).withColumn('noc', regexp_replace('noc', 'KSA', 'SAU')).withColumn('noc', regexp_replace('noc', 'KUW', 'KWT')).withColumn('noc', regexp_replace('noc', 'ANG', 'AGO')).withColumn('noc', regexp_replace('noc', 'MRI', 'MUS')).withColumn('noc', regexp_replace('noc', 'MRI', 'MUS')).withColumn('noc', regexp_replace('noc', 'ESA', 'SLV')).withColumn('noc', regexp_replace('noc', 'SAM', 'WSM')).withColumn('noc', regexp_replace('noc', 'SRI', 'LKA')).withColumn('noc', regexp_replace('noc', 'BAR', 'BRB')).withColumn('noc', regexp_replace('noc', 'BAN', 'BGD')).withColumn('noc', regexp_replace('noc', 'SKN', 'KNA')).withColumn('noc', regexp_replace('noc', 'CGO', 'COG')).withColumn('noc', regexp_replace('noc', 'ZAM', 'ZMB')).withColumn('noc', regexp_replace('noc', 'MYA', 'MMR')).withColumn('noc', regexp_replace('noc', 'CAM', 'KHM')).withColumn('noc', regexp_replace('noc', 'SUD', 'SDN')).withColumn('noc', regexp_replace('noc', 'SEY', 'SYC')).withColumn('noc', regexp_replace('noc', 'NIG', 'NER')).withColumn('noc', regexp_replace('noc', 'TAN', 'TZA')).withColumn('noc', regexp_replace('noc', 'NCA', 'NIC')).withColumn('noc', regexp_replace('noc', 'BUR', 'BFA')).withColumn('noc', regexp_replace('noc', 'VAN', 'VUT')).withColumn('noc', regexp_replace('noc', 'PLE', 'PSE')).withColumn('noc', regexp_replace('noc', 'HAI', 'HTI')).withColumn('noc', regexp_replace('noc', 'BOT', 'BWA')).withColumn('noc', regexp_replace('noc', 'LES', 'LSO')).withColumn('noc', regexp_replace('noc', 'LBA', 'LBY')).withColumn('noc', regexp_replace('noc', 'ARU', 'ABW')).withColumn('noc', regexp_replace('noc', 'ANT', 'ATG')).withColumn('noc', regexp_replace('noc', 'GBS', 'GNB')).withColumn('noc', regexp_replace('noc', 'GUI', 'GIN')).withColumn('noc', regexp_replace('noc', 'BRU', 'BRN')).withColumn('noc', regexp_replace('noc', 'BIZ', 'BLZ')).withColumn('noc', regexp_replace('noc', 'MAW', 'MWI')).withColumn('noc', regexp_replace('noc', 'OMA', 'OMN')).withColumn('noc', regexp_replace('noc', 'VIN', 'VCT')).withColumn('noc', regexp_replace('noc', 'GEQ', 'GNQ')).withColumn('noc', regexp_replace('noc', 'BHU', 'BTN')).withColumn('noc', regexp_replace('noc', 'MTN', 'MRT')).withColumn('noc', regexp_replace('noc', 'GAM', 'GMB')).withColumn('noc', regexp_replace('noc', 'CHA', 'TCD')).withColumn('noc', regexp_replace('noc', 'IOA', 'IOA')).withColumn('noc', regexp_replace('noc', 'ROT', '')).withColumn('noc', regexp_replace('noc', 'YUG', 'SRB')))

# COMMAND ----------

nocDF=(nocDF.withColumn('noc', regexp_replace('noc', 'URS', 'RUS')).withColumn('noc', regexp_replace('noc', 'GDR', 'DEU')).withColumn('noc', regexp_replace('noc', 'FRG', 'DEU')).withColumn('noc', regexp_replace('noc', 'TCH', 'CZE')).withColumn('noc', regexp_replace('noc', 'NED', 'NLD')).withColumn('noc', regexp_replace('noc', 'SUI', 'CHE')).withColumn('noc', regexp_replace('noc', 'EUN', 'RUS')).withColumn('noc', regexp_replace('noc', 'GER', 'DEU')).withColumn('noc', regexp_replace('noc', 'BUL', 'BGR')).withColumn('noc', regexp_replace('noc', 'CRO', 'HRV')).withColumn('noc', regexp_replace('noc', 'SLO', 'SVN')).withColumn('noc', regexp_replace('noc', 'LAT', 'LVA')).withColumn('noc', regexp_replace('noc', 'GRE', 'GRC')).withColumn('noc', regexp_replace('noc', 'CHI', 'CHL')).withColumn('noc', regexp_replace('noc', 'DEN', 'DNK')).withColumn('noc', regexp_replace('noc', 'ISV', 'VIR')).withColumn('noc', regexp_replace('noc', 'SCG', 'SRB')).withColumn('noc', regexp_replace('noc', 'MON', 'MCO')).withColumn('noc', regexp_replace('noc', 'TPE', 'TWN')).withColumn('noc', regexp_replace('noc', 'RSA', 'ZAF')).withColumn('noc', regexp_replace('noc', 'MGL', 'MNG')).withColumn('noc', regexp_replace('noc', 'LIB', 'LBN')).withColumn('noc', regexp_replace('noc', 'NEP', 'NPL')).withColumn('noc', regexp_replace('noc', 'IRI', 'IRN')).withColumn('noc', regexp_replace('noc', 'CRC', 'CRI')).withColumn('noc', regexp_replace('noc', 'FIJ', 'FJI')).withColumn('noc', regexp_replace('noc', 'BER', 'BMU')).withColumn('noc', regexp_replace('noc', 'ALG', 'DZA')).withColumn('noc', regexp_replace('noc', 'MAD', 'MDG')).withColumn('noc', regexp_replace('noc', 'POR', 'PRT')).withColumn('noc', regexp_replace('noc', 'CAY', 'CYM')).withColumn('noc', regexp_replace('noc', 'TOG', 'TGO')).withColumn('noc', regexp_replace('noc', 'ZIM', 'ZWE')).withColumn('noc', regexp_replace('noc', 'IVB', 'VGB')).withColumn('noc', regexp_replace('noc', 'TGA', 'TON')).withColumn('noc', regexp_replace('noc', 'PAR', 'PRY')).withColumn('noc', regexp_replace('noc', 'PHI', 'PHL')).withColumn('noc', regexp_replace('noc', 'PUR', 'PRI')).withColumn('noc', regexp_replace('noc', 'URU', 'URY')).withColumn('noc', regexp_replace('noc', 'ASA', 'ASM')).withColumn('noc', regexp_replace('noc', 'AHO', 'CUW')).withColumn('noc', regexp_replace('noc', 'YUG', 'SRB')).withColumn('noc', regexp_replace('noc', 'HON', 'HND')).withColumn('noc', regexp_replace('noc', 'GUA', 'GTM')).withColumn('noc', regexp_replace('noc', 'BAH', 'BHS')).withColumn('noc', regexp_replace('noc', 'UAE', 'ARE')).withColumn('noc', regexp_replace('noc', 'NGR', 'NGA')).withColumn('noc', regexp_replace('noc', 'INA', 'IDN')).withColumn('noc', regexp_replace('noc', 'VIE', 'VNM')).withColumn('noc', regexp_replace('noc', 'GRN', 'GRD')).withColumn('noc', regexp_replace('noc', 'MAS', 'MYS')).withColumn('noc', regexp_replace('noc', 'KSA', 'SAU')).withColumn('noc', regexp_replace('noc', 'KUW', 'KWT')).withColumn('noc', regexp_replace('noc', 'ANG', 'AGO')).withColumn('noc', regexp_replace('noc', 'MRI', 'MUS')).withColumn('noc', regexp_replace('noc', 'MRI', 'MUS')).withColumn('noc', regexp_replace('noc', 'ESA', 'SLV')).withColumn('noc', regexp_replace('noc', 'SAM', 'WSM')).withColumn('noc', regexp_replace('noc', 'SRI', 'LKA')).withColumn('noc', regexp_replace('noc', 'BAR', 'BRB')).withColumn('noc', regexp_replace('noc', 'BAN', 'BGD')).withColumn('noc', regexp_replace('noc', 'SKN', 'KNA')).withColumn('noc', regexp_replace('noc', 'CGO', 'COG')).withColumn('noc', regexp_replace('noc', 'ZAM', 'ZMB')).withColumn('noc', regexp_replace('noc', 'MYA', 'MMR')).withColumn('noc', regexp_replace('noc', 'CAM', 'KHM')).withColumn('noc', regexp_replace('noc', 'SUD', 'SDN')).withColumn('noc', regexp_replace('noc', 'SEY', 'SYC')).withColumn('noc', regexp_replace('noc', 'NIG', 'NER')).withColumn('noc', regexp_replace('noc', 'TAN', 'TZA')).withColumn('noc', regexp_replace('noc', 'NCA', 'NIC')).withColumn('noc', regexp_replace('noc', 'BUR', 'BFA')).withColumn('noc', regexp_replace('noc', 'VAN', 'VUT')).withColumn('noc', regexp_replace('noc', 'PLE', 'PSE')).withColumn('noc', regexp_replace('noc', 'HAI', 'HTI')).withColumn('noc', regexp_replace('noc', 'BOT', 'BWA')).withColumn('noc', regexp_replace('noc', 'LES', 'LSO')).withColumn('noc', regexp_replace('noc', 'LBA', 'LBY')).withColumn('noc', regexp_replace('noc', 'ARU', 'ABW')).withColumn('noc', regexp_replace('noc', 'ANT', 'ATG')).withColumn('noc', regexp_replace('noc', 'GBS', 'GNB')).withColumn('noc', regexp_replace('noc', 'GUI', 'GIN')).withColumn('noc', regexp_replace('noc', 'BRU', 'BRN')).withColumn('noc', regexp_replace('noc', 'BIZ', 'BLZ')).withColumn('noc', regexp_replace('noc', 'MAW', 'MWI')).withColumn('noc', regexp_replace('noc', 'OMA', 'OMN')).withColumn('noc', regexp_replace('noc', 'VIN', 'VCT')).withColumn('noc', regexp_replace('noc', 'GEQ', 'GNQ')).withColumn('noc', regexp_replace('noc', 'BHU', 'BTN')).withColumn('noc', regexp_replace('noc', 'MTN', 'MRT')).withColumn('noc', regexp_replace('noc', 'GAM', 'GMB')).withColumn('noc', regexp_replace('noc', 'CHA', 'TCD')).withColumn('noc', regexp_replace('noc', 'IOA', 'IOA')).withColumn('noc', regexp_replace('noc', 'ROT', '')).withColumn('noc', regexp_replace('noc', 'YUG', 'SRB')))

# COMMAND ----------

display(nocDF.orderBy("noc"))

# COMMAND ----------

nocDF.write.mode("overwrite").parquet(filestore+"/parquet/noc")

# COMMAND ----------

olympicAthletes.write.mode("overwrite").parquet(filestore+"/parquet/summerolympians")

# COMMAND ----------

##olympicAthletes.write.format('parquet').bucketBy(10, 'noc').mode("overwrite").saveAsTable('olympic_bucketed_table')

# COMMAND ----------

#olympicAthletes.write.format('parquet').bucketBy(10, 'year').mode("overwrite").saveAsTable('olympic_year_bucketed_table')

# COMMAND ----------

#olympicAthletes.cache()

# COMMAND ----------

display(olympicAthletes)

# COMMAND ----------

#aDF1.unpersist()

# COMMAND ----------

#display(aDF1)

# COMMAND ----------

newolyDF= spark.read.parquet("/Volumes/workspace/mydata/olympics/parquet/olympics")


# COMMAND ----------

display(newolyDF)

# COMMAND ----------

newathleteDF = spark.read.parquet("/Volumes/workspace/mydata/olympics/parquet/summerolympians")

# COMMAND ----------

display(newathleteDF) 
