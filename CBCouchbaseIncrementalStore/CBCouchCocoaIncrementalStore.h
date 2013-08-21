//
//  CBCouchCocoaIncrementalStore.h
//  Christian Beer
//
//  Created by Christian Beer on 01.06.13.
//
//

#import "CBCouchbaseAbstractIncrementalStore.h"

/** NSIncrementalStore implementation for CouchDB or TouchDB database. If the store URL is 
 * a file URL, a TouchDB database is opened, otherwise the CouchDB database is connected. 
 * A database URL must constist of a server / file and a database name. Examples:
 *  - file://temp/database.touchdb/database
 *  - http://localhost:5984/database
 * */
@interface CBCouchCocoaIncrementalStore : CBCouchbaseAbstractIncrementalStore

@property (nonatomic, assign) BOOL keepPropertiesWhenDeleted;

@end
