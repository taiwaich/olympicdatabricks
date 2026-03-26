# Databricks notebook source
filestore="/Volumes/workspace/mydata/olympics/"

#olympicsDF= spark.read.parquet("/Volumes/workspace/mydata/olympics/parquet/olympics")



# COMMAND ----------

olympicsDF= spark.read.parquet("/Volumes/workspace/mydata/olympics/parquet/olympics")


# COMMAND ----------

display(olympicsDF)

# COMMAND ----------

#newolyDF= spark.read.parquet("/Volumes/workspace/mydata/olympics/parquet/olympics")
athleteDF = spark.read.parquet("/Volumes/workspace/mydata/olympics/parquet/summerolympians")

# COMMAND ----------

nocDF = spark.read.parquet("/Volumes/workspace/mydata/olympics/parquet/noc")

# COMMAND ----------

#athleteDF = spark.read.parquet("/Volumes/workspace/mydata/olympics/summerolympians")

# COMMAND ----------

display(athleteDF)

# COMMAND ----------

#add the data from the 2020 games in Tokyo and 2024 Paris
from pyspark.sql.functions import *

file_type = "csv"
#csv options
inferSchema = "true"
firstRowIsHeader = "true"
delimiter = ","
filestore="/Volumes/workspace/mydata/olympics/"
filestore1="/Volumes/workspace/mydata/olympics/summer-games"

def processDF(fileLocation):
  return(spark.read.csv(fileLocation, inferSchema=inferSchema, header=firstRowIsHeader, sep=delimiter))



a2020DF=processDF(filestore1+"/2020-athletes-data.csv")
a2024DF=processDF(filestore1+"/2024-olympics-data.csv")


# COMMAND ----------

athleteDF.printSchema()

# COMMAND ----------

from pyspark.sql.types import IntegerType, DoubleType
a2020DF = a2020DF.withColumn("height", a2020DF["height"].cast(IntegerType()))
a2020DF = a2020DF.withColumn("weight", a2020DF["weight"].cast(DoubleType()))

a2024DF = a2024DF.withColumn("height", a2024DF["height"].cast(IntegerType()))
a2024DF = a2024DF.withColumn("weight", a2024DF["weight"].cast(DoubleType()))

# COMMAND ----------

a2020DF.printSchema()

# COMMAND ----------

a2024DF.printSchema()

# COMMAND ----------

display(a2020DF)

# COMMAND ----------

display(a2024DF)

# COMMAND ----------

athleteDF=athleteDF.union(a2020DF).union(a2024DF)

# COMMAND ----------

athleteDF.printSchema()

# COMMAND ----------

display(athleteDF)


# COMMAND ----------

athleteDF

# COMMAND ----------

from pyspark.sql.functions import *
medalIs=""
olympicSeason="Summer"
values=[]
medalist=["G", "S", "B", "Any"]

dbutils.widgets.dropdown("medalType", "G", [row for row in medalist ])
values=olympicsDF.filter(col("season")=="Summer").select(concat(col('Year'), lit (" "), col('city')).alias("olympic")).orderBy("Year")


olympicIs=values

olympicIs = [row.olympic for row in values.collect()]
valueis=olympicIs[9]
dbutils.widgets.dropdown("olympicYear", valueis, olympicIs)


# COMMAND ----------

medalType=getArgument("medalType")
olympicValue=getArgument("olympicYear")
olympicYear=olympicValue.split(" ")[0]


# COMMAND ----------

medalIs="gold" if medalType == "G" else  ("silver" if medalType == "S" else "bronze" if medalType == "B" else "Any" if medalType == "Any" else"no")
print("Athletes who won " + medalIs +" medals by country in "+ olympicYear)
if medalType=="Any":
   getMedals=athleteDF.filter((col("medal")!="null") & (col("year")==olympicYear)).orderBy(col("noc"))
else:
  getMedals=athleteDF.filter((col("medal")==medalType) & (col("year")==olympicYear)).orderBy(col("noc"))

display(getMedals)

# COMMAND ----------


cityGame=""
gamesYearDF=olympicsDF.filter((col("Year")==olympicYear) & (col("season")==olympicSeason))
cityGame = gamesYearDF.select((col('city')))
newCity=cityGame.toPandas()
cityIs=newCity.iloc[0][0]


# COMMAND ----------

nocare=[]
values=[]

nocare=(nocDF.select(concat(col('noc'), lit (", "), col('region')).alias("noc"))).orderBy("noc")

values = [row.noc for row in nocare.collect()]
valueis="GBR, UK"

dbutils.widgets.dropdown("NOC",valueis , values)
nocPicked=getArgument("NOC")



# COMMAND ----------

nocIs=nocPicked.split(",")[0]
countryIs=nocPicked.split(",")[1]
display(nocIs)

# COMMAND ----------

display(athleteDF)

# COMMAND ----------

print("Number of athletes who won " +medalIs + " medals in "+ olympicYear + ", " + cityIs + " games")
if medalType=="Any":
  getMedals=athleteDF.filter((col("medal")!="null") & (col("year")==olympicYear)).orderBy(col("noc"))
else:
  getMedals=athleteDF.filter((col("medal")==medalType) & (col("year")==olympicYear)).orderBy(col("noc"))
#display(getMedals.groupBy('noc').agg(count('medal').alias('Number of Medals')).orderBy(count('number of medals').desc()))


# COMMAND ----------

display(getMedals.groupBy('noc').agg(count('medal').alias('Number of Medals')).orderBy(col('Number of Medals').desc()))


# COMMAND ----------

print("Number of Medals by Country")

print("Countries who won " + medalIs +" medals by events in "+ olympicYear + ", " + cityIs + " games")
if medalType=="Any":
  getMedals=athleteDF.filter((col("medal")!="null") & (col("year")==olympicYear)).orderBy(col("noc"))
else:  
  getMedals=athleteDF.filter((col("medal")==medalType) & (col("year")==olympicYear)).orderBy(col("noc"))
medalsAre=getMedals.groupBy('noc').agg(countDistinct('event').alias('Number of Medals'))
allMedals=medalsAre.orderBy(countDistinct('event').desc())
display(allMedals)
eventCount = getMedals.groupBy('event').agg(sum('event')).count()
print ("Total Events: " + str(eventCount))


# COMMAND ----------

print("Number of Athletes by Country")

print("Countries who had athletes in "+ olympicYear + ", " + cityIs + " games")
getAthletes=athleteDF.filter((col("year")==olympicYear)).orderBy(col("noc"))
athleteCount=athleteDF.filter((col("year")==olympicYear)).orderBy(col("noc")).count()
athletesAre=getAthletes.groupBy('noc').agg(count('event').alias("Number of Athletes"))
allAthletes=athletesAre.orderBy(count('event').desc())
display(allAthletes)
eventCount = getAthletes.groupBy('event').agg(countDistinct('event')).count()
nocs=getAthletes.groupBy('noc').agg(countDistinct('noc')).count()
print ("Total Events: " + str(eventCount))
print("Total Athletes: " + str(athleteCount))
print("Total NOC's: " + str(nocs))


# COMMAND ----------

athletes=athleteDF.select(concat(athleteDF.year, lit("-"),athleteDF.city).alias("Olympic"),athleteDF.name
              .alias("olympian"))
athletesIs=athletes.groupBy('Olympic').agg(count('olympian').alias("Athletes")).orderBy('Olympic')
display(athletesIs)


# COMMAND ----------

import matplotlib.pyplot as plt


pandasDF = athletesIs.toPandas()

bar_colors = ['tab:red', 'tab:blue']

df_aoly =  pandasDF.groupby(['Olympic'])['Athletes'].max()


ylabelis="Number of Athletes Per Olympics"
xlabelis="Olympics"

plt.figure(figsize=(24,8))
plt.ylabel(ylabelis)
plt.xlabel(xlabelis)
plt.title('Number of Athletes Per Olympics')
df_aoly.plot(kind = 'bar')



# COMMAND ----------

nocAre=athleteDF.select(concat(athleteDF.year, lit("-"),athleteDF.city).alias("Olympic"),athleteDF.noc
              .alias("noc"))
nocsIs=nocAre.groupBy('Olympic').agg(countDistinct('noc').alias("NOC's")).orderBy('Olympic')
display(nocsIs)

# COMMAND ----------

sportsAre=athleteDF.select(concat(athleteDF.year, lit("-"),athleteDF.city).alias("Olympic"),athleteDF.sport
              .alias("sports"),athleteDF.year)
sportsIs=sportsAre.groupBy('Olympic').agg(countDistinct('sports').alias("Sport's")).orderBy('Olympic')
display(sportsIs)

# COMMAND ----------

display(sportsAre.filter((col("year")==olympicYear)).distinct().orderBy(col("Olympic"),col("sports")))   

# COMMAND ----------

display(sportsAre.filter((col("year")==olympicYear)).select((col('sports'))).distinct().orderBy(col("sports")))

# COMMAND ----------

eventsAre=athleteDF.select(concat(athleteDF.year, lit("-"),athleteDF.city).alias("Olympics"),athleteDF.event
              .alias("events"), athleteDF.sport,athleteDF.year)
eventsIs=eventsAre.groupBy('Olympics').agg(count('events').alias("Events")).orderBy('Olympics')
display(eventsAre.distinct().orderBy(col("Olympics"),col("sport")))

# COMMAND ----------

getGamesEvents=eventsAre.filter((col("year")==olympicYear)).orderBy(col("noc"))
display(getGamesEvents.distinct())

# COMMAND ----------

gameslist = getGamesEvents.distinct()
gameslist = gameslist.select(col('sport'),col('events'),col('year'))
display(gameslist)

# COMMAND ----------

if medalType=="Any":
    winnersis=gameslist.join(getMedals, gameslist["events"]==getMedals["event"]).select(getMedals["sport"],col("events"), col("name"),col("noc"), col("medal"))
    whowon=winnersis.groupBy("events", "noc", "medal").agg(concat_ws(",", collect_list("name")).alias("Winners"))
    whowon=whowon.orderBy("events", when(col("medal") == "G", 1)
           .when(col("medal") == "S", 2)
           .when(col("medal") == "B", 3)
           )
else:                                                                                    
    winnersis=gameslist.join(getMedals, gameslist["events"]==getMedals["event"]).select(getMedals["sport"],col("events"), col("name"),col("noc"))
    whowon=winnersis.groupBy("sport","events", "noc").agg(concat_ws(",", collect_list("name")).alias("Winners"))
                                                                         
display(whowon)


# COMMAND ----------

print("Number of Medals by Country")

print("Countries who won " + medalIs +" medals by events in "+ olympicYear + ", " + cityIs + " games")
eventAre = whowon.groupBy('noc').agg(count('noc')).orderBy(count('noc').desc())

display(eventAre)


# COMMAND ----------

display(eventsAre)

# COMMAND ----------

print("Events in "+ olympicYear + ", " + cityIs + " games")
display(gameslist.select(col("sport"), col("events")).orderBy(col("sport")))

# COMMAND ----------

#getGamesEvents=eventsAre.filter((col("year")==olympicYear))
#display(getGamesEvents.select(col('events')).distinct().orderBy(col("sport")))
#display(getGamesEvents.select((col('events'))).distinct().orderBy(col("events")))

# COMMAND ----------


import matplotlib.pyplot as plt
import pandas as pd

pandasDF = eventsIs.toPandas()

bar_colors = ['tab:red', 'tab:blue']

df_oly =  pandasDF.groupby(['Olympics'])['Events'].max()


ylabelis="Number of Events Per Olympics"
xlabelis="Olympics"

plt.figure(figsize=(24,8))
plt.ylabel(ylabelis)
plt.xlabel(xlabelis)
plt.title('Number of Events Per Olympics')
df_oly.plot(kind = 'bar')



# COMMAND ----------

display(medalsAre)

# COMMAND ----------

#get all medalist by country, events and olympics
display(athleteDF)


# COMMAND ----------

#breakdown of male and female athletes over time
males=athleteDF.select(concat(col('Year'), lit ("-"), col('city')).alias("olympic"), col('name'), col('gender')).filter((col("gender")=='M').alias("males")) 
females=athleteDF.select(concat(col('Year'), lit ("-"), col('city')).alias("olympic"), col('name'), col('gender')).filter((col("gender")=='F').alias("female"))
maleathelete=males.groupBy('olympic').agg(count('gender').alias("males")).orderBy("olympic")
femaleathelete=females.groupBy('olympic').agg(count('gender').alias("females")).orderBy("olympic")
breakdown=maleathelete.join(femaleathelete,maleathelete.olympic==femaleathelete.olympic, "outer" ).select(maleathelete.olympic,col("males"),col("females")).orderBy(maleathelete.olympic)
print("Breakdown of male and female athletes over Olympics")
breakdown=breakdown.na.fill(0)
breakdown=breakdown.withColumn("result", col("males") + col("females"))
percentage=breakdown.withColumn("percent male", col("males")/col("result")*100).withColumn("percent female", col("females")/col("result")*100)
percentage=percentage.na.fill(0)
mfdata=percentage.select(col("olympic"),col("males"),col("females"),round(col("percent male"),2).alias("Percent Male"), round(col("percent female"),2).alias("Percent Female"))
display(mfdata)

# COMMAND ----------

#get medal progression of country by olympics

print(medalIs+ " medal progression by " + nocPicked)

if medalType=="Any":
  nocMedals=athleteDF.select(concat(col('Year'), lit ("-"), col('city')).alias("olympic"), col('name'), col('medal'), col('event'),col('noc')).filter((col("noc")==nocIs) & (col("medal")!="null")) 
else:
  nocMedals=athleteDF.select(concat(col('Year'), lit ("-"), col('city')).alias("olympic"), col('name'), col('medal'), col('event'),col('noc')).filter((col("noc")==nocIs) & (col("medal")==medalType))

medalsAre=nocMedals.groupBy('olympic').agg(countDistinct('event').alias('Progression of Medals'))
allMedals=medalsAre.orderBy(countDistinct('event').desc())
display(allMedals.orderBy('olympic'))

# COMMAND ----------

import matplotlib.pyplot as plt

fig, ax = plt.subplots()

fruits = ['apple', 'blueberry', 'cherry', 'orange']
counts = [40, 100, 30, 55]
bar_labels = ['red', 'blue', '_red', 'orange']
bar_colors = ['tab:red', 'tab:blue', 'tab:red', 'tab:orange']

ax.bar(fruits, counts, label=bar_labels, color=bar_colors)

ax.set_ylabel('fruit supply')
ax.set_title('Fruit supply by kind and color')
ax.legend(title='Fruit color')

plt.show()

# COMMAND ----------

display(eventsIs)

# COMMAND ----------

eventsAre=athleteDF.select(concat(athleteDF.year).alias("Olympics"),athleteDF.event
              .alias("events"))
eventsIs=eventsAre.groupBy('Olympics').agg(countDistinct('events').alias("Events")).orderBy('Olympics')
display(eventsIs)

# COMMAND ----------

import matplotlib.pyplot as plt
import seaborn as sns

freq_dict = eventsIs.toPandas()
print(freq_dict)

print(eventsIs.count())
print( eventsIs.collect())

# COMMAND ----------

xs = [x for x in range(len(freq_dict['Olympics']))]
print(xs)

plt.plot( freq_dict['Olympics'], freq_dict['Events'])
plt.show()

# COMMAND ----------

ys = [0.0, 3.8, 4.5, 4.3, 2.1, 2.2, 0.0]
xs = [x for x in range(len(ys))]

plt.plot(xs, ys)
plt.show()
# Make sure to close the plt object once done
plt.close()

# COMMAND ----------

display(athleteDF)

# COMMAND ----------

maleG=(athleteDF.filter((col('noc')==nocIs) & (col("year")==olympicYear) & (col("gender")=='M'))).count()
femaleG=(athleteDF.filter((col('noc')==nocIs) & (col("year")==olympicYear) & (col("gender")=='F'))).count()
titleis='In the %s games, the%s team was made up of\n%i male and %i female athletes\n\n\n' %(olympicValue, countryIs, maleG, femaleG)
total=maleG+femaleG

fig, ax = plt.subplots()
if(total > 0):
  maleis=(maleG/total)*100
  femaleis=(femaleG/total)*100
else:
  maleis=0
  femaleis=0
gender = ['Male %.2f%%' %(maleis),'Female  %.2f%%' %(femaleis)]

counts = [maleis, femaleis]
bar_labels = ['blue', 'red']
bar_colors = ['tab:blue', 'tab:red']

ax.bar(gender, counts, label=bar_labels, color=bar_colors)

ax.set_ylabel('Gender')
ax.set_title(titleis)

plt.show()
