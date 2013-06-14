//
//  CBCouchbaseAbstractIncrementalStore.h
//  CBCouchDBIncrementalStore
//
//  Created by Christian Beer on 07.06.13.
//  Copyright (c) 2013 Christian Beer. All rights reserved.
//

#import <CoreData/CoreData.h>

extern NSString * const kCBISIncrementalStoreErrorDomain;
extern NSString * const kCBISTypeKey;
extern NSString * const kCBISCurrentRevisionAttributeName;
extern NSString * const kCBISManagedObjectIDPrefix;
extern NSString * const kCBISDesignName;
extern NSString * const kCBISMetadataDocumentID;
extern NSString * const kCBISObjectHasBeenChangedInStoreNotification;
extern NSString * const kCBISAllByTypeViewName;
extern NSString * const kCBISIDByTypeViewName;
extern NSString * const kCBISFetchEntityByPropertyViewNameFormat;

@class CouchServer;
@class CouchDatabase;


BOOL CBCDBIsNull(id value);
NSString *CBCDBToManyViewNameForRelationship(NSRelationshipDescription *relationship);
NSString *CBResultTypeName(NSFetchRequestResultType resultType);

NSDate *CBCBDateFromISOString(NSString *iso);
NSString *CBCBISOStringFromDate(NSDate *date);


@interface CBCouchbaseAbstractIncrementalStore : NSIncrementalStore

+ (NSString *)type;

+ (void) updateManagedObjectModel:(NSManagedObjectModel*)managedObjectModel;

- (NSArray*) replicateWithURL:(NSURL*)replicationURL exclusively:(BOOL)exclusively;
- (NSArray*) replications;
- (void) stopReplication;

- (void) defineFetchViewForEntity:(NSString*)entityName
                       byProperty:(NSString*)propertyName;

@end


@interface CBCouchbaseAbstractIncrementalStore (Private)

- (id) _couchDBRepresentationOfManagedObject:(NSManagedObject*)object;
- (id) _couchDBRepresentationOfManagedObject:(NSManagedObject*)object withCouchDBID:(BOOL)withCouchDBID;
- (NSDictionary*) _coreDataPropertiesOfDocumentWithID:(NSString*)documentID properties:(NSDictionary*)properties withEntity:(NSEntityDescription*)entity inContext:(NSManagedObjectContext*)context;

- (NSManagedObjectID *)_newObjectIDForEntity:(NSEntityDescription *)entity managedObjectContext:(NSManagedObjectContext*)context
                                     couchID:(NSString*)couchID couchType:(NSString*)couchType;

- (void) _setCacheResults:(NSArray*)array forEntity:(NSString*)entityName predicate:(NSPredicate*)predicate;
- (NSArray*) _cachedQueryResultsForEntity:(NSString*)entityName predicate:(NSPredicate*)predicate;
- (void) _purgeCacheForEntityName:(NSString*)type;

- (BOOL) _evaluatePredicate:(NSPredicate*)predicate withEntity:(NSEntityDescription*)entity properties:(NSDictionary*)properties;

- (NSString*) _createViewNameForFetchingFromEntity:(NSString*)entityName
                                        byProperty:(NSString*)propertyName;
- (NSString*) _viewNameForFetchingFromEntity:(NSString*)entityName
                                  byProperty:(NSString*)propertyName;
- (BOOL) _hasViewForFetchingFromEntity:(NSString*)entityName
                            byProperty:(NSString*)propertyName;
- (void) _setViewName:(NSString*)viewName forFetchingProperty:(NSString*)propertyName fromEntity:(NSString*)entity;

@end


@interface NSManagedObjectID (CBCouchbaseAbstractIncrementalStore)

- (NSString*) couchDBIDRepresentation;

@end
