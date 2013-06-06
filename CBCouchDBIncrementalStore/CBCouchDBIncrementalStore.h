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

// redefinition of blocks so we don't need to import the header (which would result in 
typedef void (^TDMapEmitBlock_)(id key, id value);
typedef void (^TDMapBlock_)(NSDictionary* doc, TDMapEmitBlock_ emit);


@interface CBCouchDBIncrementalStore : NSIncrementalStore

@property (nonatomic, strong, readonly) CouchServer   *server;
@property (nonatomic, strong, readonly) CouchDatabase *database;


+ (NSString *)type;

+ (void) updateManagedObjectModel:(NSManagedObjectModel*)managedObjectModel;

- (NSArray*) replicateWithURL:(NSURL*)replicationURL exclusively:(BOOL)exclusively;
- (NSArray*) replications;

- (void) defineFetchViewForEntity:(NSString*)entityName
                       byProperty:(NSString*)propertyName
                         mapBlock:(TDMapBlock_)mapBlock
                          version:(NSString*)version;

@end


@interface NSManagedObjectID (CBCouchDBIncrementalStore)

- (NSString*) couchDBIDRepresentation;

@end
