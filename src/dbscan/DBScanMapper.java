/*
 * Copyright 2012 Department of Computer Science - University of Turin.
 * Author: Marco Sero
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dbscan;

// Mongo
import org.bson.*;
import com.mongodb.hadoop.io.*;
import com.mongodb.Mongo;
import com.mongodb.DBCollection;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.DB;
import org.bson.types.*;

// hadoop
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

// Java
import java.io.*;
import java.util.*;
import java.text.DecimalFormat;

/**
 * This class realizes the map function.
 * 
 * @author Marco Sero
 *
 */

public class DBScanMapper extends Mapper<ObjectId, BSONObject, Text, BSONWritable> {
	
	/**
	 * MongoDB's stuff
	 */
	private Mongo m;
	private DB db;
	private DBCollection collection;
	
	/**
	 * Radius, in kilometers, to search neighbors for every point.
	 */
	private static final double radius = 50; // km
	
	/**
	 * Minimum number of points that nearPoints must have to emit a new cluster.
	 */
	private static final int minPointsToCreateCluster = 30;
	
	// Connect to Mongo
	DBScanMapper() throws java.net.UnknownHostException {
		super();
		m = new Mongo();
		db = m.getDB( "p" );
		boolean auth = db.authenticate("username", "password".toCharArray());
		if(!auth)
			throw new RuntimeException("Login error");
		collection = db.getCollection("points");
	}
	
	/**
	 * The map function read the points in input. All points are not read and not clusterized
	 * for the input query execute in MongoDb from Hadoop.
	 * If a point has enought points in neighborhood, then a cluster will be emitted and the
	 * point is marked as clusterized; otherwise, do nothing.
	 *
	 * @param pointKey the key of the point in input
	 * @param pointValue the point in input, in BSON (binary JSon)
	 * @param pointContext the context in which map-reduce works
	 */
    @Override
    public void map(final ObjectId pointKey, final BSONObject pointValue, final Context pointContext)
            			throws IOException, InterruptedException {
	
		// the point is not visited and not clustered
    	// see mongo.input.query in mongo-dbscan.xml
    	
    	// mark point as visited
		pointValue.put("visited", new Boolean(true));
	
		// query for *near points
		// *near: points within the radius with same hashtag
		Object locationAndRadius [] = { pointValue.get("loc"), new Double(radius / 111.12) };
		BasicDBObject centerQuery = new BasicDBObject("$center", locationAndRadius);
		BasicDBObject withinQuery = new BasicDBObject("$within", centerQuery);
		
		BasicDBObject findNearPoints = new BasicDBObject();
		findNearPoints.put("loc", withinQuery);
		findNearPoints.put("hashtag", pointValue.get("hashtag"));
		
		DBCursor nearPoints = collection.find(findNearPoints);
		
		// if near points are enough -> create cluster
		if(nearPoints.count() >= minPointsToCreateCluster) {
			
			// mark point as clusterized
			pointValue.put("clusterized", new Boolean(true));
			
			// new cluster
			BSONWritable newCluster = new BSONWritable();
			
			// create key
			BSONObject loc = (BSONObject)pointValue.get("loc");
			int lat, lon;
			if(loc.get("lat") instanceof Double)
				lat = ((Double)loc.get("lat")).intValue();
			else
				lat = (Integer) loc.get("lat");
			if(loc.get("lon") instanceof Double)
				lon = ((Double)loc.get("lon")).intValue();
			else
				lon = (Integer) loc.get("lon");	
			// Final key
			String newClusterKey = pointValue.get("hashtag") + "_" + lat +"," + lon;
			
			// add points (the first of the cluster)
			newCluster.put("numPoints", 1);
			
			// add location
			DecimalFormat df = new DecimalFormat("###.####");
			String formattedLat, formattedLon;
			if(loc.get("lat") instanceof Double)
				formattedLat = df.format(((Double)loc.get("lat")));
			else
				formattedLat = df.format(((Integer)loc.get("lat")));
			if(loc.get("lon") instanceof Double)
				formattedLon = df.format(((Double)loc.get("lon")));
			else
				formattedLon = df.format(((Integer)loc.get("lon")));
			Map<String, Float> clusterLoc = new TreeMap<String, Float>();
			clusterLoc.put("lat", Float.parseFloat(formattedLat));
			clusterLoc.put("lon", Float.parseFloat(formattedLon));
			newCluster.put("loc", clusterLoc);
			
			// add near points (analyzed by reduce)
			Map<ObjectId, BSONObject> mapNearPoints = new HashMap<ObjectId, BSONObject>();
			while(nearPoints.hasNext()) {
				BSONObject o = nearPoints.next();
				mapNearPoints.put((ObjectId)o.get("_id"), o);
			}
			newCluster.put("neighborPoints", mapNearPoints);
			
			// add date
			newCluster.put("createdAt", new Date());
			
			// add hashtag
			newCluster.put("hashtag", pointValue.get("hashtag"));

			// emit new cluster
			//System.out.println("Emit new candidate cluster with " + mapNearPoints.size() + " points in neighborhood");
			pointContext.write( new Text(newClusterKey), newCluster );
		
		}
		
		// update point in Mongo
		// the point is now visited and *maybe clusterized
		// *maybe because could not be enought points in neighborhood
		BasicDBObject findOldPoint = new BasicDBObject("_id", pointValue.get("_id"));
		BasicDBObject newPoint = new BasicDBObject(pointValue.toMap());
		collection.findAndModify(findOldPoint, newPoint);
   
    }
    
}

