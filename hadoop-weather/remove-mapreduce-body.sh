#!/bin/sh

sed -i '' -e '20,36d' src/main/java/com/xebia/summerclass/hadoop/weather/mapreduce/temp/TemperaturePerMonthMapper.java
sed -i '' -e '14,30d' src/main/java/com/xebia/summerclass/hadoop/weather/mapreduce/temp/MaxTemperaturePerMonthReducer.java
sed -i '' -e '14,32d' src/main/java/com/xebia/summerclass/hadoop/weather/mapreduce/temp/AverageTemperaturePerMonthReducer.java
