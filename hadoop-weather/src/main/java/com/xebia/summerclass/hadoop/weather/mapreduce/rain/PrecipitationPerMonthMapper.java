package com.xebia.summerclass.hadoop.weather.mapreduce.rain;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import com.xebia.summerclass.hadoop.weather.KnmiDailyLineParser;
import com.xebia.summerclass.hadoop.weather.KnmiLineParser.KnmiLineType;

public class PrecipitationPerMonthMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    private static final Logger LOG = Logger.getLogger(PrecipitationPerMonthMapper.class);

    private KnmiDailyLineParser parser = new KnmiDailyLineParser();

    protected void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException {
        try {
            if (parser.parse(line) == KnmiLineType.DATA) {
                context.write(keyForStationAndDate(), valueForPrecipitation());
            }
        } catch (NumberFormatException e) {
            // ignore line with missing data
        } catch (Exception e) {
            LOG.warn("Exception caught during call to map(): ", e);
        }
    }

    private Text keyForStationAndDate() {
        return new Text(parser.getStation() + "," + parser.getDate().substring(0, 6));
    }

    private LongWritable valueForPrecipitation() {
        return new LongWritable(Math.max(parser.getPrecipitation(), 0));
    }
}
