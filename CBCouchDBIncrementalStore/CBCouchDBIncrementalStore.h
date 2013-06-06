//
//  CBCouchDBIncrementalStore.h
//  Christian Beer
//
//  Created by Christian Beer on 01.06.13.
//
//

#import <CoreData/CoreData.h>


extern NSString * const kCBTDBIncrementalStoreErrorDomain;
extern NSString * const kCBTDBTypeKey;
extern NSString * const kCBTDBDesignName;
extern NSString * const kCBTDBObjectHasBeenChangedInStoreNotification;
extern NSString * const kCBTDBFetchEntityByPropertyViewNameFormat;

@class CouchServer;
@class CouchDatabase;


@interface CBCouchDBIncrementalStore : NSIncrementalStore

@property (nonatomic, strong, readonly) CouchServer   *server;
@property (nonatomic, strong, readonly) CouchDatabase *database;


+ (NSString *)type;

+ (void) updateManagedObjectModel:(NSManagedObjectModel*)managedObjectModel;

- (NSArray*) replicateWithURL:(NSURL*)replicationURL exclusively:(BOOL)exclusively;
- (NSArray*) replications;

- (void) defineFetchViewForEntity:(NSString*)entityName
                       byProperty:(NSString*)propertyName;

@end


@interface NSManagedObjectID (CBCouchDBIncrementalStore)

- (NSString*) couchDBIDRepresentation;

@end
