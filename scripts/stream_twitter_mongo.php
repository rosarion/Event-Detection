<?php

require_once("functions.php");

$username = "Event_Detection";

function warning_handler($errno, $errstr) {
	global $username;
	if($username == "Event_Detection")
		$username = "lifeofadev";
	else
		$username = "Event_Detection";
}

set_error_handler("warning_handler", E_WARNING);


while(true) {
	
	try {

		// Making a Connection
		$connection = new Mongo('mongodb://user:password@localhost'); // connects to localhost:27017

		// select a database
		$db = $connection->p;
		// The database does not need to be created in advance, you can create new databases by selecting them

		// select a collection (analogous to a relational database's table)
		$collection = $db->points;
	

		$fp = fopen("https://$username:password@stream.twitter.com/1/statuses/filter.json?locations=-180,-90,180,90","r");

		while($data = fgets($fp)) {
		
			$json = json_decode($data, true);
		
			if(array_key_exists("created_at", $json) && !empty($json['entities']['hashtags'])) {
				if(array_key_exists("geo", $json) && !empty($json['geo'])) {
					$json['loc']['lat'] = $json['geo']['coordinates'][0];
					$json['loc']['lon'] = $json['geo']['coordinates'][1];		
			
					$num_hashtags = count($json['entities']['hashtags']);
					for($i = 0; $i < $num_hashtags; $i++) {
						$ht = strtolower($json['entities']['hashtags'][$i]['text']);
						if(!blacklist($ht)) {
							$obj = array( "loc" => $json["loc"], "hashtag" => $ht, "date" => new MongoDate(time()) , "tweetId" => $json['id'], "visited" => false, "clusterized" => false );
							$collection->insert($obj);
						//	echo ".";
						}
					}
				}
			}
	
	
		}
	//	fclose($fp);

		// Sleep for a while
		sleep(10);
		
	} catch(Exception $err) {
		echo 'Error: ' .$err->getMessage();
	}
}

send_mail("Twitter Script");

?>
