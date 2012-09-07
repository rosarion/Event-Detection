<?php

require_once("functions.php");

while(true) {
	
	try {

		// Making a Connection
		$connection = new Mongo('mongodb://user:password@localhost/p'); // connects to localhost:27017

		// select a database
		$db = $connection->p;
		// The database does not need to be created in advance, you can create new databases by selecting them

		// select a collection (analogous to a relational database's table)
		$collection = $db->points;

		$fp = fopen("http://api.teleportd.com/stream?user_key=API_KEY","r");

		while($data = fgets($fp)) {
			$json = json_decode($data, true);

			if($json != null && array_key_exists('loc', $json) && !empty($json['hashtag'])) {
			
				$num_hashtags = count($json['hashtag']);
				for($i = 0; $i < $num_hashtags; $i++) {
					$ht = strtolower($json['hashtag'][$i]);
					if(!blacklist($ht)) {
						$obj = array( "loc" => $json["loc"], "hashtag" => $ht, "date" => new MongoDate(time()), "photoId" => $json["sha"], "visited" => false, "clusterized" => false );
						$collection->insert($obj);
					//	echo ".";
					}
				}
			}	
		}
		fclose($fp);

		// Sleep for a while
		sleep(10);
		
	} catch(Exception $err) {
		echo 'Error: ' .$err->getMessage();
	}
}

send_mail("Teleportd Script");
