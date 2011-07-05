package com.xebia.summerclass.hadoop.skeleton.wk;

import com.xebia.summerclass.hadoop.core.CommonLogLineParser;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TotalClicksOnUrlPerDayMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

	private CommonLogLineParser commonLogLineParser = new CommonLogLineParser();

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		if (commonLogLineParser.parse(value)) {
			String url = commonLogLineParser.getRequestUrl();
			Date date = commonLogLineParser.getDate();

			String day = new SimpleDateFormat("yyyy-MM-dd").format(date);

			context.write(new Text(url + "," + day), new LongWritable(1));
		} else {
			System.err.println("Could not parse line: " + value);
		}
	}
}
