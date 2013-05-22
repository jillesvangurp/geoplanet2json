package com.github.jillesvangurp.geoplanet;

import static com.github.jsonj.tools.JsonBuilder.array;
import static com.github.jsonj.tools.JsonBuilder.object;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import com.github.jsonj.JsonArray;
import com.github.jsonj.JsonObject;
import com.github.jsonj.tools.GeoJsonSupport;
import com.github.jsonj.tools.JsonParser;
import com.jillesvangurp.geo.GeoGeometry;

public class FlickShapeProcessor {
    private static final Charset UTF8 = Charset.forName("UTF-8");

    private final JsonParser parser;

    private final BufferedWriter bw;

    public FlickShapeProcessor(JsonParser parser, BufferedWriter bw) {
        this.parser = parser;
        this.bw = bw;
    }


    private void process(String file) throws IOException {

        BufferedReader br = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(file)),UTF8));
        JsonObject featureCollection = parser.parse(br).asObject();
        JsonArray features = featureCollection.getArray("features");
        System.out.println("nroffeatures:" + features.size());
        for(JsonObject feature: features.objects()) {
            try {
                String type = feature.getString("geometry","type");
                JsonArray coordinates = feature.getArray("geometry","coordinates");
                if("MultiPolygon".equalsIgnoreCase(type)) {
                    if(coordinates.size()==1) {
                        type="Polygon";
                        // only keep the outer
                        JsonArray newCoordinates = new JsonArray();
                        newCoordinates.add(coordinates.get(0).asArray().get(0).asArray());
                        coordinates=fixIfSelfIntersecting(newCoordinates);
                    } else {
                        for(int i=0; i<coordinates.size();i++) {
                            JsonArray newCoordinates = new JsonArray();
                            newCoordinates.add(coordinates.get(i).asArray().get(0).asArray());
                            coordinates.set(i, fixIfSelfIntersecting(newCoordinates));
                        }
                    }
                } else if("Polygon".equalsIgnoreCase(type)) {
                    JsonArray newCoordinates = new JsonArray();
                    newCoordinates.add(coordinates.get(0).asArray());
                    coordinates=fixIfSelfIntersecting(newCoordinates);
                } else {
                    System.err.println("unexpected type " + type);
                }

                JsonObject geometry = object()
                        .put("type", type)
                        .put("coordinates", coordinates)
                        .get();
                String[] values = { feature.getString("properties","place_type") };
                JsonObject categories = object()
                .put("flickr-shapes", array(values))
                .get();
                JsonObject json = object()
                    .put("title", feature.getString("properties","label"))
                    .put("ids", array(feature.getString("properties","woe_id"),feature.getString("properties","place_id")))
                    .put("categories", categories)
                    .put("geometry", geometry)
                    .get();

                bw.write(json.toString() + '\n');
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private JsonArray fixIfSelfIntersecting(JsonArray coordinates) {
        JsonArray outerCoordinates = coordinates.get(0).asArray();
        boolean intersects=false;
        Set<JsonArray> seen = new HashSet<>();
        for(int i=0;i<outerCoordinates.size()-1;i++) {
            JsonArray coord = outerCoordinates.get(i).asArray();
            if(seen.contains(coord)) {
                intersects=true;
                break;
            } else {
                seen.add(coord);
            }
        }
        if(intersects) {
            double[][] points = GeoJsonSupport.fromJsonJPolygon(coordinates)[0];
            double[][] replacement = GeoGeometry.polygonForPoints(points);

            JsonArray fixed = GeoJsonSupport.toJsonJLineString(replacement);
            if(!fixed.get(0).equals(fixed.get(fixed.size()-1))) {
                fixed.add(fixed.get(0));
            }

            JsonArray newCoordinates = new JsonArray();
            newCoordinates.add(fixed);
            return newCoordinates;
        } else {
            return coordinates;
        }
    }

    public static void main(String[] args) throws IOException {
        try(BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream("flickr.json.gz")), UTF8))) {
            FlickShapeProcessor processor = new FlickShapeProcessor(new JsonParser(),bw);

            for(String file:Arrays.asList(
                    "flickr_shapes_continents.geojson.gz",
                    "flickr_shapes_counties.geojson.gz",
                    "flickr_shapes_countries.geojson.gz",
                    "flickr_shapes_localities.geojson.gz",
                    "flickr_shapes_neighbourhoods.geojson.gz",
                    "flickr_shapes_regions.geojson.gz")) {
                System.out.println(file);
                processor.process("/Users/jilles/data/flickr/"+file);
            }
            System.out.println("done");
        }
    }
}
