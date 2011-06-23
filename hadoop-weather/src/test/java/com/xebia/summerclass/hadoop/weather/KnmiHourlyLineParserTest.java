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
        String line = "  391,20110101,   12,  290,   30,   20,   60,   27,    7,   27,    0,   13,    8,    4,     ,     ,     ,  100,     ,     ,     ,     ,     ,";
        KnmiLineType result = this.parser.parse(line);
        assertEquals(KnmiLineType.DATA, result);
        assertEquals("391", parser.getStation());
        assertEquals("20110101", parser.getDate());
        assertEquals("12", parser.getHour());
        assertEquals(4, parser.getPrecipitation());
    }

}
