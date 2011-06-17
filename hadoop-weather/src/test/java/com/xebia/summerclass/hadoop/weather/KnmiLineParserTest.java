package com.xebia.summerclass.hadoop.weather;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

import com.xebia.summerclass.hadoop.weather.KnmiLineParser.KnmiLineType;


public class KnmiLineParserTest {

    private KnmiLineParser parser;

    @Before
    public void setUp() {
        this.parser = new KnmiLineParser();
    }

    @Test
    public void shouldParseMeasurementLine() {
        String line = "  240,20100106,  186,   32,   34,   60,   17,   20,    8,   80,   17,  -28,  -66,   24,    3,   15,  -80,   24,   55,   70,  449,   29,   28,   13,   16,10036,10057,   23,10022,    4,    3,   16,   58,   14,    5,   95,   98,    4,   87,   13,    4";
        KnmiLineType result = this.parser.parse(line);
        assertEquals(KnmiLineType.DATA, result);
        assertEquals("240", parser.getStation());
        assertEquals("20100106", parser.getDate());
        assertEquals(28, parser.getPrecipitation());
    }

    @Test
    public void shouldParseCommentLine() {
        String line = "# THESE DATA CAN BE USED FREELY PROVIDED THAT THE FOLLOWING SOURCE IS ACKNOWLEDGED:";
        KnmiLineType result = this.parser.parse(line);
        assertEquals(KnmiLineType.COMMENT, result);
    }

}
