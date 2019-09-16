package com.hive.udf.h3;
import com.uber.h3core.H3Core;
import com.uber.h3core.util.GeoCoord;

import java.io.IOException;
import java.util.List;

public class h3 {
    public static void main(String[] args) {
        H3Core h3 = null;
        try {
            h3 = H3Core.newInstance();
        } catch (IOException e) {
            e.printStackTrace();
        }

        double lat = 37.775938728915946;
        double lng = -122.41795063018799;
        int res = 9;

        String hexAddr = h3.geoToH3Address(lat, lng, res);
        System.out.println(hexAddr);
        List<String> hexagons =h3.kRing(hexAddr,0);
        System.out.println(hexagons);
        System.out.println(hexagons.size());

        List<String> hexagons1 =h3.kRing(hexAddr,2);
        System.out.println(hexagons1.toString());
        List<GeoCoord> geoCoords = h3.h3ToGeoBoundary(hexAddr);
        System.out.println(geoCoords);

    }
}
