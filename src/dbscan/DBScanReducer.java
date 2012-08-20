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

// Java
import java.io.*;
import java.text.DecimalFormat;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

// Mongo
import org.bson.*;
import org.bson.types.*;
import com.mongodb.hadoop.io.*;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.DBCollection;
import com.mongodb.DB;

// Hadoop
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

// jcoord
import uk.me.jstott.jcoord.LatLng;

/**
 * This class realizes the reduce function.
 * 
 * @author Marco Sero
 *
 */
public class DBScanReducer extends Reducer<Text, BSONWritable, Text, BSONWritable> {
	
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
	
	/**
	 * In a few special occasions, during the cycles of the reduce, could happen
	 * that the new cluster location, calculated as the average of coordinates,
	 * is significantly different from the previous location.
	 * In this case, the cluster analyzed is not an event, but a common
	 * Twitter's trending topic on a national or international scale.
	 * We are only interest on event with an accurate location.
	 * The scope of this variable is to denote if the new event location is
	 * *very* different from previous one.
	 */
	private static final int MAX_DISTANCE_OFFSET_NEW_CLUSTER_LOCATION = 100; // km
	
	// Connect to Mongo
	DBScanReducer() throws java.net.UnknownHostException {
		super();
		m = new Mongo();
		db = m.getDB( "p" );
		boolean auth = db.authenticate("msero", "edserver".toCharArray());
		if(!auth) throw new RuntimeException("Login error");
		collection = db.getCollection("points");
	}

	/**
	 * The reduce function has in input an 'array' of clusters with the same key.
	 * Its job is to aggregate these clusters and to analyze their neighborhood
	 * to merge all points into a unique cluster.
	 * This method can be called much times, virtually every time the map function emits
	 * a new cluster with a key equals to another cluster.
	 *
	 * @param pKey the key of the clusters in input
	 * @param pValues the array iterable of clusters (type of these objects is BSONWritable)
	 * @param pContext the context in which map-reduce works
	 */
    @Override
    public void reduce( final Text pKey, final Iterable<BSONWritable> pValues, final Context pContext )
            throws IOException, InterruptedException{
		
    	//System.out.println("Reducing clusters with key : " + pKey +"...");
    	
		// get the iterator
		Iterator<BSONWritable> iterator = pValues.iterator();
		
		// alloc *new* cluster
		BSONWritable newCluster = new BSONWritable();
		
		int numPoints = 0;
		int k = 0;
		float avgLat = 0;
		float avgLon = 0;
		int numPointsAnalyzed = 0;;
		
		// start loop for analyze every cluster
		while(iterator.hasNext()) {
			
			BSONObject aCluster = iterator.next();
			
			// at the first to loop, initialize the *new* cluster
			if(k == 0) {
				newCluster.put("loc", aCluster.get("loc"));
				newCluster.put("createdAt", aCluster.get("createdAt"));
				newCluster.put("hashtag", aCluster.get("hashtag"));
				newCluster.put("isEvent", aCluster.get("isEvent"));
			}
			
			// add points to *new* cluster
			numPoints += (Integer)aCluster.get("numPoints");
						
			// put all neighbor points to a ConcurrentHashMap
			Map<ObjectId, BSONObject> tmp = (Map<ObjectId, BSONObject>)aCluster.get("neighborPoints");
			Map<ObjectId, BSONObject> neighborPoints = new ConcurrentHashMap<ObjectId, BSONObject>();
			neighborPoints.putAll(tmp);
			
			// start loop for neighbor points			
			int i = 0;
			for(Iterator iteratorNeighborPoints = neighborPoints.entrySet().iterator(); iteratorNeighborPoints.hasNext(); ) {
				
				Map.Entry<ObjectId, BSONObject> p = (Entry<ObjectId, BSONObject>) iteratorNeighborPoints.next();
				
				// needs to re-query MongoDB because the point now could be visited
				// by, for example, a map thread concurrent to this reduce thread
				BSONObject point = collection.findOne(new BasicDBObject("_id", p.getValue().get("_id")));
				boolean pointModified = false;
				
				if(point != null) {
					if((Boolean)point.get("visited") == false) {
							
						// mark as visited
						point.put("visited", true);
						pointModified = true;
											
						// find near points
						BasicDBObject findNearPoints = new BasicDBObject();
						findNearPoints.put("loc", new BasicDBObject("$within", new BasicDBObject("$center", new Object[]{ point.get("loc"), new Double(radius / 111.12) })));
						findNearPoints.put("hashtag", point.get("hashtag"));
						DBCursor nearPoints = collection.find(findNearPoints);
						
						if(nearPoints.size() >= minPointsToCreateCluster) {
							// increase performance by adding only points unvisited OR unclusterized
							// two query BUT much less points to loop
							findNearPoints.put("$or", new BasicDBObject[] { new BasicDBObject("visited", false), new BasicDBObject("clusterized", false) });
							nearPoints = collection.find(findNearPoints);
							
							toMap(neighborPoints, nearPoints.toArray());
						}
						
						// refer to null to free a bit of memory
						findNearPoints = null;
						nearPoints = null;
				
					} // end if visited == false
					
					if((Boolean)point.get("clusterized") == false) {
						// add the point to cluster
						point.put("clusterized", true);
						pointModified = true;
						numPoints++;
					}
					
					// update new point in MongoDB
					if(pointModified)
						collection.findAndModify(new BasicDBObject("_id", point.get("_id")), new BasicDBObject(point.toMap()));
					
					// update average location
					if(((BasicBSONObject)point.get("loc")).get("lat") instanceof Double)
						avgLat += ((Double) ((BasicBSONObject) point.get("loc")).get("lat")).floatValue();
					else
						avgLat += ((Integer) ((BasicBSONObject) point.get("loc")).get("lat")).floatValue();
					if(((BasicBSONObject)point.get("loc")).get("lon") instanceof Double)
						avgLon += ((Double) ((BasicBSONObject) point.get("loc")).get("lon")).floatValue();
					else
						avgLon += ((Integer) ((BasicBSONObject) point.get("loc")).get("lon")).floatValue();
					
					point = null;
					i++;
					numPointsAnalyzed++;
				}
			} // end loop for neighbor points
			k++;
			
			aCluster = null;
			neighborPoints = null;
			System.gc();
			
		} // end loop for clusters

		
		if(numPointsAnalyzed > 0) {
			// update average location of new cluster with the weighted average
			// of points analyzed
			avgLat = avgLat / (float)numPointsAnalyzed;
			avgLon = avgLon / (float)numPointsAnalyzed;
	
			// if the location of analyzed points is significantly different from
			// the old cluster location, then that cluster is not an event!
			BSONObject loc = (BSONObject)newCluster.get("loc");
			LatLng oldLatLon = new LatLng((Double)loc.get("lat"), (Double)loc.get("lon"));
			LatLng newLatLon = new LatLng(avgLat, avgLon);
			double distance = oldLatLon.distance(newLatLon);

			if(distance < MAX_DISTANCE_OFFSET_NEW_CLUSTER_LOCATION)
				// mark as event
				newCluster.put("isEvent", true);
			else
				// mark as no-event
				newCluster.put("isEvent", false);
				
			// update new position (only if is valid)
			if(avgLat >= -90.0f && avgLat <= 90.0f && avgLon >= -180.0f && avgLon <= 180.0f) {
				DecimalFormat df = new DecimalFormat("##.######");
				Map<String, Float> newLoc = new TreeMap<String, Float>();
				newLoc.put("lat", Float.parseFloat(df.format(avgLat)));
				newLoc.put("lon", Float.parseFloat(df.format(avgLon)));
				newCluster.put("loc", newLoc);
			}
			
		}
		
		// update new cluster object
		newCluster.put("numPoints", numPoints);
		newCluster.put("neighborPoints", new HashMap<ObjectId, BSONObject>());
		
		// write to context if and only if the new cluster has enought points
		if(numPoints > 30)
			pContext.write( pKey, newCluster );
		
		newCluster = null;
		
		// IN CASE OF MEMORY PROBLEMS: force garbage collector
		// it could not be elegant and is often not recommended,
		// but it works
		System.gc();
		
	}
    
    /**
     * Convert a list of DBObject into a Map
     * @param dest the destionation Map
     * @param list the input List
     */
    public static final void toMap(Map dest, List<DBObject> list) { 

    	// Iterate through each element in source and put it in dest. 
    	int j = 0;
    	for (int i = 0; i < list.size(); ++i) { 
    		if(!dest.containsKey(list.get(i).get("_id"))) {
    			dest.put(list.get(i).get("_id").toString(), list.get(i));
    			j++;
    		}
    	}
    } 

}

