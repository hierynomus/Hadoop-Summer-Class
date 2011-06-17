package com.xebia.summerclass.hadoop.core;

import static org.junit.Assert.*;

import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;

public class CommonLogLineParserTest {

    private CommonLogLineParser parser;

    @Before
    public void setUp() {
        this.parser = new CommonLogLineParser();
    }

    @Test
    public void shouldParseLine() {
        String line = "168.231.41.0 - - [25/Jul/1998:22:00:01 +0000] \"GET /english/individuals/player93446.htm HTTP/1.0\" 200 7911";
        assertTrue(parser.parse(line));

        assertEquals("168.231.41.0", parser.getRemoteHost());
        assertEquals(new DateTime("1998-07-25T22:00:01Z"), new DateTime(parser.getDate()));

        assertEquals("GET /english/individuals/player93446.htm HTTP/1.0", parser.getRequest());
        assertEquals("GET", parser.getRequestMethod());
        assertEquals("/english/individuals/player93446.htm", parser.getRequestUrl());
        assertEquals("HTTP/1.0", parser.getRequestProtocol());

        assertEquals(200, parser.getStatusCode());
        assertEquals(7911, parser.getContentLength());
    }

    @Test
    public void shouldParseLineWithoutContentLength() {
        String line = "18.174.0.0 - - [25/Jul/1998:22:00:02 +0000] \"GET /images/hm_f98_top.gif HTTP/1.0\" 304 -";
        assertTrue(parser.parse(line));

        assertEquals(0, parser.getContentLength());
    }

}
