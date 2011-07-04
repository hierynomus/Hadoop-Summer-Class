package com.maxmind.geoip;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class LookupServiceTest {
    
    private LookupService ip2geo;
    
    @Before
    public void setUp() throws IOException {
        ip2geo = new LookupService(copyIp2GeoFileFromClasspathToLocalDisk(),
                LookupService.GEOIP_MEMORY_CACHE);
    }
    
    @After
    public void tearDown() {
        if (ip2geo != null) {
            ip2geo.close();
        }
    }
    
    @Test
    public void shouldGetLocationForIp() {
       Location location = ip2geo.getLocation("83.160.178.81");
       assertEquals(52.350006, location.latitude, 0.000001);
       assertEquals(4.916702, location.longitude, 0.000001);
    }
    
    private static File copyIp2GeoFileFromClasspathToLocalDisk() throws IOException {
        File localDatafile = new File(System.getProperty("java.io.tmpdir")
                + "/ip2geo/GeoLiteCity.dat");
        
        FileUtils.copyInputStreamToFile(
                LookupService.class.getResourceAsStream("/GeoLiteCity.dat"),
                localDatafile);
        
        return localDatafile;
    }
}
