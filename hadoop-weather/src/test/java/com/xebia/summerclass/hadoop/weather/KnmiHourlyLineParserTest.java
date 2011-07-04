package com.xebia.summerclass.hadoop.weather;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

import com.xebia.summerclass.hadoop.weather.KnmiLineParser.KnmiLineType;

public class KnmiHourlyLineParserTest {

    private KnmiHourlyLineParser parser;

    @Before
    public void setUp() {
        this.parser = new KnmiHourlyLineParser();
    }

    @Test
    public void shouldParseMeasurementLine() {

        String line ="  240,20110701,   12,  310,   70,   80,  110,  174,  135,   91,    7,  229,    1,    5,10239,   81,    6,   58,   23,    0,    1,    0,    0,    0";

        KnmiLineType result = this.parser.parse(line);
        assertEquals(KnmiLineType.DATA, result);
        assertEquals("240", parser.getStation());
        assertEquals("20110701", parser.getDate());
        assertEquals("12", parser.getHour());
        assertEquals(5, parser.getPrecipitation());
        assertEquals(174, parser.getTemperature());
    }

}
