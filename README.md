SimpleMapStore
==============

Simple persistent HashMap implementation for Java. Backed by embedded BerkeleyDB

This project is on its initial stage. This Github version is not tested yet.

You will need to download the BerleleyDB Java library form:
 * From their Maven repository: download.oracle.com/maven/com/sleepycat/je/5.0.34/je-5.0.34.jar
 * Or from their official page: http://www.oracle.com/technetwork/products/berkeleydb/downloads/index.html

Map operates on strings or Objects, these are implemented on separate methods:
 * String store:
  * put(String, String) - Store any string
  * get(String) - Fetch string from db or null if not found.
  * removeString(String) - Remove string entry from db. Returns true if matching key was found.
 * Object store:
  * putObject(String, Object) - Store any Object
  * getObject(String) - Fetch Object from db or null if not found.
  * removeObject(String) - Remove Object entry from db. Returns true if matching key was found.

Example usage:

```Java
// First make sure source files are in the project path.

import com.simplemapstore.BPersistentMap;  
..  
BPersistentMap dbMap = new BPersistentMap();  // If no map exists here, then new is created on working directory
map.put("myKey","myData");  // Data is synchronously written to disk  
String data = map.get("myKey");  // Fetch data from the db
System.out.println(data);  
map.removeString("myKey");  // Synchronously removes the data from the persistent db
```

