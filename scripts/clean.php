<?php

require_once("functions.php");

while (true) {
	try {
		// Making a Connection
		$connection = new Mongo('mongodb://msero:edserver@localhost'); // connects to localhost:27017

		$p = $connection->p;
		$e = $connection->e;
		$points = $p->points;
		$events = $e->events;

		while(true) {
			
			$end = new MongoDate(time() - 21600);		// 6 hours ago // (6 * 60 * 60)
			
			$points->remove(array("date" => array('$lt' => $end)));
			$events->remove(array("createdAt" => array('$lt' => $end)));
			// lt = less than
			
			sleep(30);
		}
	} catch(Exception $err) {
		echo 'Error: ' .$err->getMessage();
	}
}

send_mail("Clean Script");

?>
