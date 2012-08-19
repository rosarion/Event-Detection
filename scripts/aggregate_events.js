db.events.find().forEach(function(event) {
	
//	print("Reducing event with hashtag " + event.hashtag + " and position: " + tojson(event.loc));
    
	// find near events within 50km with the same hashtag
	var events = db.runCommand( { geoNear : "events" , near : [ event.loc.lat , event.loc.lon ], query : { hashtag : event.hashtag }, maxDistance : (50 / 111.12) } );
	events = events["results"];
	
//	print("Found " + events.length + " with the same hashtag");
		
	// merge points and remove
	for(var i = 0; i < events.length; i++) {
		var e = events[0]["obj"];
		if(e._id != event._id) {
			event.numPoints += e.numPoints;
			db.events.remove( { _id : e._id } );
//			print("Removed event with hashtag " + event.hashtag + " and position: " + tojson(event.loc));
		}
	}
	
//	print("Aggregated " + events.length + " events\n\n");
		
	// update The Event
	db.events.insert(event);
	
});
