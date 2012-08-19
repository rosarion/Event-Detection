<?php

function send_mail($script) {

	$to      = 'marco@marcosero.com';
	$subject = $script." error";
	$message = "There was a problem with this script";
	$headers = 'From: server@event-detection.com' . "\r\n" .
	    'Reply-To: server@event-detection.com' . "\r\n" .
	    'X-Mailer: PHP/' . phpversion();

	mail($to, $subject, $message, $headers);

}

$bl = array(
	"igers",
	"instantfollow",
	"ff",
	"followfriday",
	"100thingsaboutme",
	"instagram",
	"instamood",
	"instaday",
	"instagood",
	"teamautofollow",
	"teamfollowback",
	"followback",
	"tweegram",
	"instagrammer",
	"iphonesia",
	"iphoneography",
	"iphoneonly",
	"instadaily",
	"iphone",
	"nowplaying",
	"cute",
	"love",
	"summer",
	"me",
	"photooftheday",
	"tweegram",
	"job",
	"jobs",
	"tweetmyjobs",
	"follow",
	"followme",
	"1000aday",
	"500aday",
	"autofollowback",
	"39",
	"food",
	"foodporn",
	"mustfollow",
	"hipstamatic",
	"p2000"
);

function blacklist($str) {
	
	global $bl;
	return in_array($str, $bl);
	
}


?>
