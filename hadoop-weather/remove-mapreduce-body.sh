#!/bin/sh

sed -i '' -e '20,34d' src/main/java/com/xebia/summerclass/hadoop/weather/mapreduce/rain/PrecipitationPerMonthMapper.java 
sed -i '' -e '14,30d' src/main/java/com/xebia/summerclass/hadoop/weather/mapreduce/rain/TotalPrecipitationPerMonthReducer.java 
sed -i '' -e '14,32d' src/main/java/com/xebia/summerclass/hadoop/weather/mapreduce/rain/AverageDailyPrecipitationPerMonthReducer.java 
