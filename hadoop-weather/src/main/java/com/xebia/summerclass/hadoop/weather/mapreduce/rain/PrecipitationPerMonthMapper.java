package com.xebia.summerclass.hadoop.weather.mapreduce.rain;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class PrecipitationPerMonthMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

    private static final Logger LOG = Logger.getLogger(PrecipitationPerMonthMapper.class);

    private static String COMMENT_PREFIX = "#";
    private static int STN = 0;
    private static int YYYYMMDD = 1;
    private static int RH = 22;
    private static int EXPECTED_FIELDS = 41;

    protected void map(LongWritable offset, Text line, Context context) throws IOException, InterruptedException {
        try {
            String lineString = line.toString();
            if (lineString.startsWith(COMMENT_PREFIX)) {
                return;
            }

            String[] values = lineString.split(",\\s*");
            if (values.length != EXPECTED_FIELDS) {
                return;
            }

            long precipitation = Long.parseLong(values[RH]);
            if (precipitation < 0) {
                precipitation = 0;
            }

            Text key = new Text(values[STN].trim() + "," + values[YYYYMMDD].substring(0, 6));

            context.write(key, new LongWritable(precipitation));
        } catch (Exception e) {
            LOG.warn("Exception caught during call to map(): ", e);
        }
    }
}
