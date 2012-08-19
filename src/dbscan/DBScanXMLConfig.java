package dbscan;

import com.mongodb.hadoop.util.*;

// Hadoop
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;

/**
 * The dbscan-hadoop xml config object.
 */
public class DBScanXMLConfig extends MongoTool {

    static{
        // Load the XML config defined in hadoop-local.xml
   		// Configuration.addDefaultResource ( "~/dbscan-hadoop/resources/hadoop-local.xml" );//( "src/examples/hadoop-local.xml" );
    	// Configuration.addDefaultResource ( "~/dbscan-hadoop/resources/mongo-defaults.xml" );//( "src/examples/mongo-defaults.xml" );
	}

    public static void main( final String[] pArgs ) throws Exception{
        System.exit( ToolRunner.run( new DBScanXMLConfig(), pArgs ) );
    }
}

