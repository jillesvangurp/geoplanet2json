package com.github.jillesvangurp.geoplanet;

import static com.github.jsonj.tools.JsonBuilder.object;
import static com.github.jsonj.tools.JsonBuilder.primitive;
import static com.jillesvangurp.iterables.Iterables.processConcurrently;
import static com.jillesvangurp.iterables.Iterables.toIterable;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.lang.StringUtils;

import com.github.jsonj.JsonArray;
import com.github.jsonj.JsonObject;
import com.github.jsonj.JsonPrimitive;
import com.github.jsonj.tools.JsonParser;
import com.google.common.base.Splitter;
import com.jillesvangurp.iterables.ConcurrentProcessingIterable;
import com.jillesvangurp.iterables.LineIterable;
import com.jillesvangurp.iterables.Processor;

/**
 * Does an in memory join of the various geoplanet files to create enriched json representations of geoplanet places.
 *
 * Warning, this requires a bit of memory. I typically run it with 6GB of heap (java -Xmx6000M). I expect you would run out of memory with anything less than 5GB.
 */
public class GeoPlanetConverter {
    private static final Charset UTF8 = Charset.forName("UTF-8");
    public static final String adjacencies="/Users/jilles/data/geoplanet/geoplanet_adjacencies_7.10.0.tsv.gz";
    public static final String aliases="/Users/jilles/data/geoplanet/geoplanet_aliases_7.10.0.tsv.gz";
    public static final String places="/Users/jilles/data/geoplanet/geoplanet_places_7.10.0.tsv.gz";
    public static final String flickrShapes="/Users/jilles/data/output/flickr-shapes/flickr-shapes-20130511105122.json.gz";

    private void convert() {
        try {
            final Map<String, JsonObject> geoplanetPlaces = new ConcurrentHashMap<>();
            readPlaces(geoplanetPlaces);
            addAliases(geoplanetPlaces);
            addAdjacencies(geoplanetPlaces);
            addGeometry(geoplanetPlaces);
            System.out.println("serializing places to file");
            try(BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream("geoplanet.json.gz")), UTF8))) {
                for(Entry<String, JsonObject> e: geoplanetPlaces.entrySet()) {
                    bw.write(e.getValue().toString() + '\n');
                }
            }
            System.out.println("done");

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void addGeometry(final Map<String, JsonObject> geoplanetPlaces) throws IOException {
        System.out.println("adding geometries");
        final JsonParser parser = new JsonParser();
        final AtomicInteger brokenrefs = new AtomicInteger();
        try(LineIterable it=LineIterable.openGzipFile(flickrShapes)) {
            Processor<String, Boolean> processor = new Processor<String, Boolean>() {

                @Override
                public Boolean process(String input) {
                    try {
                        int sep = input.indexOf(';');
                        if(sep <= 13) { // geohash
                            input=input.substring(sep+1);
                        }

                        JsonObject object = parser.parse(input).asObject();
                        JsonArray ids = object.getArray("ids");
                        String woeid = ids.get(0).asString();
                        JsonObject place = geoplanetPlaces.get(woeid);
                        if(place != null) {
                            JsonObject geometry = object.getObject("geometry");
                            place.put("geometry", geometry);
                        } else {
                            brokenrefs.incrementAndGet();
                            System.out.println(object);
                        }
                        return true;
                    } catch (Exception e) {
                        e.printStackTrace();
                        return false;
                    }
                }
            };
            process(it,processor,"geometries");
            System.out.println("there are " + brokenrefs.get() + " flickr woeids without a match to geoplanet");
        }
    }

    private void process(Iterable<String> it, Processor<String,Boolean> processor, String what) throws IOException {
        final AtomicInteger lines=new AtomicInteger();
        try(ConcurrentProcessingIterable<String, Boolean> concurrentProcessor = processConcurrently(it, processor , 10000, 9, 10000)) {
            for(@SuppressWarnings("unused") Boolean b:concurrentProcessor) {
                lines.incrementAndGet();
                if(lines.get()%100000 == 0) {
                    System.out.println("processed " + lines + " " + what);
                }
            }
        }
        System.out.println("done adding " + what);
    }

    private void addAdjacencies(final Map<String, JsonObject> geoplanetPlaces) throws IOException {
        System.out.println("adding adjacencies");
        try(LineIterable it=LineIterable.openGzipFile(adjacencies)) {
            Iterator<String> iterator = it.iterator();
            final ArrayList<String> fields = readFields(iterator);
            Processor<String, Boolean> processor = new Processor<String, Boolean>() {

                @Override
                public Boolean process(String input) {
                    JsonObject object = object().get();
                    int i=0;
                    for(String value: Splitter.on('\t').split(input)) {
                        if(StringUtils.isNotEmpty(value)) {
                            value=deqoute(value);
                            object.put(fields.get(i), value);
                        }
                        i++;
                    }
                    String woeid1 = object.getString("Place_WOE_ID");
                    String woeid2 = object.getString("Neighbour_WOE_ID");


                    JsonObject place1 = geoplanetPlaces.get(woeid1);
                    JsonObject place2 = geoplanetPlaces.get(woeid2);
                    if(place1 != null && place2 != null) {
                        synchronized(place1) {
                            JsonArray array1 = place1.getOrCreateArray("neigbor_woeids");
                            if(!array1.contains(woeid2)) {
                                array1.add(woeid2);
                            }
                        }
                        synchronized(place2) {
                            JsonArray array2 = place2.getOrCreateArray("neigbor_woeids");
                            if(!array2.contains(woeid1)) {
                                array2.add(woeid1);
                            }
                        }
                    }

                    return true;
                }

            };

            process(toIterable(iterator), processor, "adjacencies");
        }
    }

    public static String deqoute(String value) {
        if(value.startsWith("\"") && value.endsWith("\"") ) {
            return value.substring(1, value.length()-1);
        } else {
            return value;
        }
    }

    private void addAliases(final Map<String, JsonObject> geoplanetPlaces) throws IOException {
        System.out.println("adding aliases");
        try(LineIterable it=LineIterable.openGzipFile(aliases)) {
            Iterator<String> iterator = it.iterator();
            final ArrayList<String> fields = readFields(iterator);
            Processor<String, Boolean> processor = new Processor<String, Boolean>() {

                @Override
                public Boolean process(String input) {
                    JsonObject object = object().get();
                    int i=0;
                    for(String value: Splitter.on('\t').split(input)) {
                        value=deqoute(value);
                        if(StringUtils.isNotEmpty(value)) {
                            object.put(fields.get(i), value);
                        }
                        i++;
                    }
                    String woeid = object.getString("WOE_ID");
                    String lang = object.getString("Language");
                    String name = object.getString("Name");
                    JsonObject place = geoplanetPlaces.get(woeid);
                    if(place != null) {
                        synchronized(place) {
                            JsonArray languageValues = place.getOrCreateArray("name",lang);
                            JsonPrimitive namePrimitive = primitive(name);
                            if(!languageValues.contains(namePrimitive)) {
                                languageValues.add(namePrimitive);
                            }
                        }
                    }
                    return true;
                }
            };
            process(toIterable(iterator), processor, "aliases");
        }
    }

    private void readPlaces(final Map<String, JsonObject> geoplanetPlaces) throws IOException {
        try(LineIterable it=LineIterable.openGzipFile(places)) {
            Iterator<String> iterator = it.iterator();
            final ArrayList<String> fields = readFields(iterator);
            System.out.println("reading places");
            // iterate over the rest of the lines

            Processor<String, Boolean> processor = new Processor<String, Boolean>() {

                @Override
                public Boolean process(String input) {
                    JsonObject object = object().get();
                    int i=0;
                    for(String value: Splitter.on('\t').split(input)) {
                        value=deqoute(value);
                        if(StringUtils.isNotEmpty(value)) {
                            object.put(fields.get(i), value);
                        }
                        i++;
                    }
                    geoplanetPlaces.put(object.getString(fields.get(0)), object);
                    return true;
                }
            };
            process(toIterable(iterator), processor, "places");
        }
    }

    private ArrayList<String> readFields(Iterator<String> iterator) {
        String first = iterator.next();
        final ArrayList<String> fields1=new ArrayList<>();
        for(String fieldName: Splitter.on('\t').split(first)) {
            fieldName=deqoute(fieldName);
            fields1.add(fieldName);
        }
        final ArrayList<String> fields = fields1;
        return fields;
    }

    public static void main(String[] args) {
        new GeoPlanetConverter().convert();
    }


}
