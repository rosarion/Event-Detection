# An algorithm for Event Detection
## DBSCAN Algorithm in Map/Reduce logic, implemented with Hadoop and MongoDB


## Algorithm
### Just a bit of background
The algorithm use the map/reduce logic to analyze tweets and photos in MongoDB and to create geolocated **events**.  

A *point* is a tweet or a photo, with an hashtag and a geotag.  
A *cluster* is an event, with a number of points analyzed and a number of neighboring points to analyze.

#### Hadoop
I choosed [Hadoop](http://hadoop.apache.org/) to realize Map/Reduce because it is the more widely used framework for this kind of job and it has large documentation.  
In this way, I don't care about multithreading and multiprocessing and I leave Hadoop this kind of jobs.

The algorithm is running on a server with 16 cores and 32GB of RAM.
This is my [mapred-site.xml](http://cl.ly/IrX1) if someone cares.

#### MongoDB
After a deep research in many relational and not relational database, [MongoDB](http://www.mongodb.org/) was my choice. It has a great documentation, a powerful support of geolocated query and it is very scalable.  
The Mongo-Hadoop plugin let me to integrate it in MongoDB. Its config options are in the file `mongo-dbscan.xml`.

### Map
The map function read the points in input. All points are not read and not clusterized. This is due to the input query execute in MongoDB from Hadoop.  
If a point has enought points in neighborhood, then a cluster will be emitted and the point is marked as clusterized; otherwise, do nothing.

### Reduce
The reduce function has in input an 'array' of clusters with the same key.  
Its job is to aggregate these clusters and to analyze their neighborhood to merge all points into a unique cluster.  
This method can be called much times, virtually every time the map function emits a new cluster with a key equals to another cluster.

## Scripts
I've written a couple of PHP scripts to collect datas from real-time stream of Twitter and Teleportd.  
There is another script to delete all points and events of 6 hours old.  
Take a look in `scripts` folder.

## At work
This algorithm is currently running in our server. Visit [eventdetection.marcosero.com](http://eventdetection.marcosero.com) to view my project.

![](screenshot.png)