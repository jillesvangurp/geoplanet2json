package com.github.jillesvangurp.geoplanet;

import static com.github.jsonj.tools.JsonBuilder.array;
import static com.github.jsonj.tools.JsonBuilder.object;
import static com.github.jsonj.tools.JsonBuilder.primitive;
import static com.jillesvangurp.iterables.Iterables.processConcurrently;
import static com.jillesvangurp.iterables.Iterables.toIterable;

import java.io.BufferedWriter;
import java.io.FileNotFoundException;
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
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.lang.StringUtils;

import com.github.jsonj.JsonArray;
import com.github.jsonj.JsonObject;
import com.github.jsonj.JsonPrimitive;
import com.github.jsonj.tools.JsonParser;
import com.github.jsonj.tools.JsonSerializer;
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

    // misc strings used in the json
    private static final String CATEGORIES = "categories";
    private static final String COUNTRY = "country";
    private static final String GEOMETRY = "geometry";
    private static final String ID = "id";
    private static final String IDS = "ids";
    private static final String LANGUAGE = "language";
    private static final String NAME = "name";
    private static final String NEIGHBOR_IDS = "neighborIds";
    private static final String NEIGHBOR_WOEIDS = "neighbor_woeids";
    private static final String PARENT_ID = "parentId";
    private static final String PLACE_TYPE = "PlaceType";
    private static final String SOURCE = "source";
    private static final String TITLE = "title";
    private static final String WOE_ID = "WOE_ID";
    private static final String YAHOO_LANGUAGE = "Language";
    private static final String YAHOO_NAME = "Name";
    private static final String YAHOO_PARENT_ID = "Parent_ID";

    private static final JsonParser PARSER = new JsonParser();

    // misc files used in this file
    private static final String OUTPUT_FILE = "geoplanet.json.gz";
    private static final Charset UTF8 = Charset.forName("UTF-8");
    private static final String adjacencies="/Users/jilles/data/geoplanet/geoplanet_adjacencies_7.10.0.tsv.gz";
    private static final String aliases="/Users/jilles/data/geoplanet/geoplanet_aliases_7.10.0.tsv.gz";
    private static final String places="/Users/jilles/data/geoplanet/geoplanet_places_7.10.0.tsv.gz";
    private static final String flickrShapes="/Users/jilles/data/output/flickr-shapes/flickr-shapes-20130511105122.json.gz";

    private void convert() {
        try {
            final Map<String, JsonObject> geoplanetPlaces = new ConcurrentHashMap<>();
            readPlaces(geoplanetPlaces);
            addAliases(geoplanetPlaces);
            addAdjacencies(geoplanetPlaces);
            addGeometry(geoplanetPlaces);
            serialize(geoplanetPlaces);
            System.out.println("done");

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void serialize(final Map<String, JsonObject> geoplanetPlaces) throws IOException, FileNotFoundException {
        System.out.println("serializing places to file");
        try(BufferedWriter bw = gzipFileWriter(OUTPUT_FILE)) {
            for(Entry<String, JsonObject> e: geoplanetPlaces.entrySet()) {
                JsonSerializer.write(bw, e.getValue(), false);
                bw.newLine();
            }
        }
    }

    private void addGeometry(final Map<String, JsonObject> geoplanetPlaces) throws IOException {
        System.out.println("adding geometries");
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

                        JsonObject object = PARSER.parse(input).asObject();
                        JsonArray ids = object.getArray(IDS);
                        String woeid = ids.get(0).asString();
                        JsonObject place = geoplanetPlaces.get(woeid);
                        if(place != null) {
                            JsonObject geometry = object.getObject(GEOMETRY);
                            place.put(GEOMETRY, geometry);
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
                            JsonArray array1 = place1.getOrCreateArray(NEIGHBOR_WOEIDS);
                            if(!array1.contains(woeid2)) {
                                array1.add(woeid2);
                            }
                        }
                        synchronized(place2) {
                            JsonArray array2 = place2.getOrCreateArray(NEIGHBOR_WOEIDS);
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
                    String woeid = object.getString(WOE_ID);
                    String lang = object.getString(YAHOO_LANGUAGE);
                    String name = object.getString(YAHOO_NAME);
                    String nameType = object.getString("Name_Type");
                    JsonObject place = geoplanetPlaces.get(woeid);
                    if(place != null) {
                        synchronized(place) {
                            JsonArray languageValues = place.getOrCreateArray(NAME,lang);
                            JsonPrimitive namePrimitive = primitive(name);
                            if(!languageValues.contains(namePrimitive)) {
                                // make sure preferred names are at start of the list of alternatives
                                switch (nameType.toUpperCase()) {
                                case "P":
                                    languageValues.add(0, namePrimitive);
                                    break;
                                case "Q":
                                    languageValues.add(0, namePrimitive);
                                    break;
                                default:
                                    languageValues.add(namePrimitive);
                                    break;
                                }
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
        final ArrayList<String> fields=new ArrayList<>();
        for(String fieldName: Splitter.on('\t').split(first)) {
            fieldName=deqoute(fieldName);
            fields.add(fieldName);
        }
        return fields;
    }


    /**
     * Takes the output of convert and applies some cleanup; also gets the json in the preferred format for Localstream.
     * Separate class so that the garbage collector can get rid of the huge map.
     */
    static class PostProcess {

        private final Lock lock=new ReentrantLock();

        public void cleanup() {
            try(LineIterable it=LineIterable.openGzipFile(OUTPUT_FILE)) {
                try(BufferedWriter bw=gzipFileWriter("geoplanet_cleaned-"+System.currentTimeMillis()+".json.gz")) {
                    Processor<String, Boolean> processor = new Processor<String, Boolean>() {

                        @Override
                        public Boolean process(String input) {
                            try {
                                JsonObject place = PARSER.parse(input).asObject();

                                JsonObject result = object().get();

                                result.put(ID, place.get(WOE_ID));
                                result.put(SOURCE, "geoplanet");
                                JsonObject names = fixNames(place);
                                if (names != null) {
                                    result.put(NAME, names);
                                }
                                String title = place.getString(TITLE);
                                if (title != null) {
                                    result.put(TITLE, title);
                                }
                                JsonObject cats = fixCategories(place);
                                if (cats != null) {
                                    result.put(CATEGORIES, cats);
                                }
                                String parent = place.getString(YAHOO_PARENT_ID);
                                if (parent != null) {
                                    result.put(PARENT_ID, parent);
                                }
                                String country = place.getString("ISO");
                                if (country != null) {
                                    result.put(COUNTRY, country);
                                }
                                JsonArray neighbors = place.getArray(NEIGHBOR_WOEIDS);
                                if (neighbors != null) {
                                    result.put(NEIGHBOR_IDS, neighbors);
                                }
                                JsonObject geometry = place.getObject(GEOMETRY);
                                if (geometry != null) {
                                    result.put(GEOMETRY, geometry);
                                }
                                write(bw, result);
                                return true;
                            } catch (IOException e) {
                                throw new IllegalStateException(e);
                            }
                        }

                        private JsonObject fixCategories(JsonObject place) {
                            String geoPlanetCategory = place.getString(PLACE_TYPE);

                            return object().put("geoplanet", array(geoPlanetCategory)).get();
                        }

                        private JsonObject fixNames(JsonObject place) {
                            JsonObject names = place.getOrCreateObject(NAME);
                            String yahooName = place.getString(YAHOO_NAME);
                            String language = place.getString(YAHOO_LANGUAGE);
                            if(StringUtils.isNotEmpty(language)) {
                                JsonArray languageValues = names.getOrCreateArray(language);
                                if(!languageValues.contains(yahooName)) {
                                    languageValues.add(yahooName);
                                }
                                // make sure we give special status for the preferred Yahoo Name
                                place.put(TITLE, yahooName);
                                place.put(LANGUAGE, language);
                            } else {
                                if(StringUtils.isNotEmpty(yahooName)) {
                                    // fall back to english
                                    JsonArray languageValues = names.getOrCreateArray("ENG");
                                    if(!languageValues.contains(yahooName)) {
                                        languageValues.add(yahooName);
                                    }
                                    place.put(TITLE, yahooName);
                                }
                            }
                            return names;
                        }

                    };
                    process(it, processor, "for cleanup");
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        private void write(BufferedWriter bw, JsonObject object) throws IOException {
            lock.lock();
            try {
                JsonSerializer.write(bw, object, false);
                bw.newLine();
            } finally {
                lock.unlock();
            }
        }
    }

    private static BufferedWriter gzipFileWriter(String file) throws IOException, FileNotFoundException {
        return new BufferedWriter(new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(file)), UTF8),1000000);
    }

    private static void process(Iterable<String> it, Processor<String,Boolean> processor, String what) throws IOException {
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

    public static void main(String[] args) {
        new GeoPlanetConverter().convert();
        new PostProcess().cleanup();
    }
}
