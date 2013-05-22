# Introduction

This project is about taking the geoplanet data provided by Yahoo and the related flick shapes dataset and merging them into a more coherent and usable format.

Geoplanet was a great project at Yahoo where they built a hierarchical place graph. The flickr shapes project was a side project where the Flickr people tried to cluster geotagged photos and associated them with this graph. The result is a data set of a bit over 200000 polygons for continents, countries, states, cities, localities, neighborhoods, etc. 

There are a few problems with both datasets:

The problem with the geoplanet data is that it lacks coordinates and is essentially a database dump of several tables, which is annoying for processing it since you have to do a join of the files somehow.

The problem with the flickr shapes is that the data has woe_ids but lacks some of the geoplanet meta data. Additionally the flickr data set is pretty printed geojson, which is kind of annoying when parsing line by line. Finally, the flickr dataset contains polygons that are self intersecting, which is something e.g. Elastic Search (at the time of writing) is unable to deal with.

This project provides two converters that address these problems.

- FlickrShapeProcessor converts the flickr geojson into something a bit more usable that also fixes the self intersecting polygons (by turning them into simpler but less accurate convex polygons).
- GeoPlanetConverter takes the file that comes out of that as well as the three files contained in the geoplanet data set and merges those together to produce a gz file with a line of json for each place with all the places, aliases, adjacencies, and if available geojson geometries in one place.

# Data

You can download the input data here.
 - [geoplanet](http://archive.org/search.php?query=geoplanet)
 - [flick shapes 2.0](http://code.flickr.net/2011/01/08/flickr-shapefiles-public-dataset-2-0/)

# Downloading the output

If you want to skip the fun of running the code, I have a [torrent](geoplanet.json.gz.torrent) for the output. Download that or find it in the cloned repository and open it in your favorite torrent app. The Geoplanet data is creative commons with attribution. The Flickr shapes are creative commons zero waiver (i.e. public domain) licensed. Since this converter generates a derivative product from both, you should attribute *Yahoo Geoplanet* when using the data in this file.
 
# Using the converter yourself

First, apologies for the lack of tests. I tested by running the code and it seems to work. That being said, there's room for some improvement.

- First import the project in eclipse as a maven project (or whatever IDE you use). It's a maven project, so be sure to import it as such and make sure that you follow [my instructions](http://www.jillesvangurp.com/2013/02/27/maven-and-my-github-projects/) for hooking up my private maven repository, which you will need for various dependencies to some of my other projects. Alternatively, check them out manually and mvn clean install them.
- Download all the input files (see section above)
- adapt the hardcoded paths in the source code to your liking (sorry about that)
- first run the FlickrShapeProcessor
- fix the path to the file that was created in the previous step in GeoPlanetConverter; make sure the paths to the geoplanet data are also correct
- run that with -Xmx7000M as a jvm argument (gives you a heap of 7GB). If you don't have enough RAM, I'm sorry but you will run out of memory :-). Basically it creates a gigantic ConcurrentHashMap in memory.

The whole thing should be over in about 45 minutes. But your mileage may vary. If you are using a laptop, you might want to plug in since this thing will keep your CPU busy for a while.

# Technical

This project shows off some memory saving strategies I've used in especially jsonj. The reason this takes so much memory is that object references are 64 bit in Java and there are a lot of those references in the concurrent hash map that contains all the places (>5M). I'm saving memory by using jsonj which uses utf8 bytes for strings, caches dictionary keys, and uses a memory efficient map implementation for objects. Without that, you would need a lot more memory.

Finally, there's a bit of concurrency as well courtesy of my iterables-support project. Basically, the converters iterate over files concurrently with 8 threads, which assuming you have four cores or 8 hyperthreaded cores) should keep your computer busy.

# Caveats

- Geoplanet comes without coordinates; flickr shapes only cover a few hundred thousand of the woeids (out of > 5M). The rest are conveniently contained by the flickr shapes though. I may invest some time in correlating the geonames data set with the geoplanet dataset to address this.
- Some shapes in the flickr data sets have woeids that are not in the geoplanet data I used.
- The geoplanet hierarchy contains some inaccuracies. Especially at the neighborhood level there are quite a few inaccuracies.
- Geoplanet was shut down and is no longer maintained. This dataset is several years out of date. 
- The Flickr polygons are an approximation based on clustering geotagged photos. That means a lot of polygons are somewhat inaccurate and either too large or too small.
- Going forward, not all places will have a woeid since Yahoo no longer issues those and I may add more data sources that have no woeid.

# Future plans

We are building a location graph at Localstream using various sources of data, including this one. We eventually open up our dataset. However, currently our focus is on building our service. 

I am considering matching this dataset against geonames to add missing coordinates and to further enrich the data. 

# License

The license is the [MIT license](http://en.wikipedia.org/wiki/MIT_License), a.k.a. the expat license. The rationale for choosing this license is that I want to maximize your freedom to do whatever you want with the code while limiting my liability for the damage this code could potentially do. I do appreciate attribution but not enough to require it in the license (beyond the obligatory copyright notice).


