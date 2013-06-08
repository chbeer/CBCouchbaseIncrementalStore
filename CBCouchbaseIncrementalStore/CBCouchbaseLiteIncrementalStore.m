//
//  CBCouchbaseLiteIncrementalStore.m
//  CBCouchDBIncrementalStore
//
//  Created by Christian Beer on 07.06.13.
//  Copyright (c) 2013 Christian Beer. All rights reserved.
//

#import "CBCouchbaseLiteIncrementalStore.h"

#import <CouchbaseLite/CouchbaseLite.h>


@interface CBCouchbaseLiteIncrementalStore ()

@property (nonatomic, strong) CBLDatabase *database;

@end


@implementation CBCouchbaseLiteIncrementalStore
{
    NSMutableArray *_coalescedChanges;
}

+ (NSString *)type
{
    return @"CBCouchDBIncrementalStore";
}

- (id)initWithPersistentStoreCoordinator:(NSPersistentStoreCoordinator *)root configurationName:(NSString *)name
                                     URL:(NSURL *)url options:(NSDictionary *)options
{
    self = [super initWithPersistentStoreCoordinator:root configurationName:name URL:url options:options];
    
    _coalescedChanges = [[NSMutableArray alloc] init];
    
    return self;
}

#pragma mark - NSIncrementalStore

-(BOOL)loadMetadata:(NSError **)error;
{
    if (![super loadMetadata:error]) {
        return NO;
    }
    
    NSString *databaseName = [self.URL lastPathComponent];
    NSURL *databaseURL = [self.URL URLByDeletingLastPathComponent];
    
    NSAssert([databaseURL isFileURL], @"CouchbaseLite only supports file database URLs");

    CBLManager *manager = [[CBLManager alloc] initWithDirectory:[databaseURL path] options:0 error:error];
    if (!manager) {
        return NO;
    }
    
    NSLog(@"[info] opening Couchbase-Lite database named: %@", databaseName);
    self.database = [manager createDatabaseNamed:databaseName error:error];
    if (!self.database) {
        return NO;
    }
    
    [self initializeViews];
    
//    self.database.tracksChanges = YES;
//    [self.database onChange:^(CBLDocument *doc, BOOL external){
//        if (external) {
//            [self couchDocumentChanged:doc];
//        }
//    }];
    
    CBLDocument *doc = [self.database documentWithID:@"cbtdb_metadata"];
    
    BOOL success = NO;
    
    NSDictionary *metaData = doc.properties;
    if (![metaData objectForKey:NSStoreUUIDKey]) {
        
        metaData = @{
                     NSStoreUUIDKey: [[NSProcessInfo processInfo] globallyUniqueString],
                     NSStoreTypeKey: [[self class] type]
                     };
        [self setMetadata:metaData];
        
        CBLDocument *doc = [self.database documentWithID:@"cbtdb_metadata"];
        success = [doc putProperties:metaData error:error] != nil;
        
    } else {
        
        [self setMetadata:doc.properties];
        
        success = YES;
        
    }
    
    return success;
}

- (id)executeRequest:(NSPersistentStoreRequest *)request withContext:(NSManagedObjectContext*)context error:(NSError **)error;
{
    if (request.requestType == NSSaveRequestType) {
        
        NSSaveChangesRequest *save = (NSSaveChangesRequest*)request;
        
#ifdef DEBUG_DETAILS
        NSLog(@"[tdis] save request: ---------------- \n"
              "[tdis]   inserted:%@\n"
              "[tdis]   updated:%@\n"
              "[tdis]   deleted:%@\n"
              "[tdis]   locked:%@\n"
              "[tids]---------------- ", [save insertedObjects], [save updatedObjects], [save deletedObjects], [save lockedObjects]);
#endif
        
        
        // NOTE: currently we are waiting for each operation. That's not really what we want.
        // maybe we should wait using +[RESTOperation wait:set] but that didn't work on my test.

        NSMutableSet *changedEntities = [NSMutableSet setWithCapacity:[save insertedObjects].count];

        // Objects that were inserted...
        for (NSManagedObject *object in [save insertedObjects]) {
            NSDictionary *contents = [self _couchDBRepresentationOfManagedObject:object withCouchDBID:YES];
            
            CBLDocument *doc = [self.database documentWithID:[object.objectID couchDBIDRepresentation]];
            NSError *localError;
            if ([doc putProperties:contents error:&localError]) {
                [changedEntities addObject:object.entity.name];
                
                [object willChangeValueForKey:@"cbtdbRev"];
                [object setPrimitiveValue:doc.currentRevisionID forKey:@"cbtdbRev"];
                [object didChangeValueForKey:@"cbtdbRev"];
                
                [object willChangeValueForKey:@"objectID"];
                [context obtainPermanentIDsForObjects:@[object] error:nil];
                [object didChangeValueForKey:@"objectID"];
                
                [context refreshObject:object mergeChanges:YES];
            } else {
                if (error) *error = [NSError errorWithDomain:kCBISIncrementalStoreErrorDomain
                                                        code:2 userInfo:@{
                            NSLocalizedFailureReasonErrorKey: @"Error putting inserted objects",
                                        NSUnderlyingErrorKey:localError
                                     }];
            }
        }
        
        // clear cache for entities to get changes
        for (NSString *entityName in changedEntities) {
            [self _purgeCacheForEntityName:entityName];
        }
        
        
        // Objects that were updated...
        for (NSManagedObject *object in [save updatedObjects]) {
            NSDictionary *contents = [self _couchDBRepresentationOfManagedObject:object withCouchDBID:YES];
            
            CBLDocument *doc = [self.database documentWithID:[object.objectID couchDBIDRepresentation]];
            NSError *localError;
            if ([doc putProperties:contents error:&localError]) {
                [changedEntities addObject:object.entity.name];
                
                [object willChangeValueForKey:@"cbtdbRev"];
                [object setPrimitiveValue:doc.currentRevisionID forKey:@"cbtdbRev"];
                [object didChangeValueForKey:@"cbtdbRev"];
                
                [context refreshObject:object mergeChanges:YES];
            } else {
                if (error) *error = [NSError errorWithDomain:kCBISIncrementalStoreErrorDomain
                                                        code:2 userInfo:@{
                            NSLocalizedFailureReasonErrorKey: @"Error putting updated object",
                                        NSUnderlyingErrorKey:localError
                                     }];
            }
        }
                
        
        // Objects that were deleted from the calling context...
        for (NSManagedObject *object in [save deletedObjects]) {
            CBLDocument *doc = [self.database documentWithID:[object.objectID couchDBIDRepresentation]];
            NSError *localError;
            if (![doc deleteDocument:&localError]) {
                if (error) *error = [NSError errorWithDomain:kCBISIncrementalStoreErrorDomain
                                                        code:2 userInfo:@{
                            NSLocalizedFailureReasonErrorKey: @"Error deleting object",
                                        NSUnderlyingErrorKey:localError
                                     }];
            }
        }
        
        return @[];
        
        
    } else if (request.requestType == NSFetchRequestType) {
        
        NSFetchRequest *fetch = (NSFetchRequest*)request;
        
        NSFetchRequestResultType resultType = fetch.resultType;
        
        id result = nil;
        
        NSEntityDescription *entity = fetch.entity;
        NSString *entityName = entity.name;
        
        CFAbsoluteTime start = CFAbsoluteTimeGetCurrent();
        
        // Docs: "note that it is not necessary to populate the managed object with attribute or relationship values at this point"
        // -> you'll need them for predicates, though ;)
        
        switch (resultType) {
            case NSManagedObjectResultType:
            case NSManagedObjectIDResultType: {
                result = [self queryObjectsOfEntity:entity byFetchRequest:fetch inContext:context];
                if (fetch.sortDescriptors) {
                    result = [result sortedArrayUsingDescriptors:fetch.sortDescriptors];
                }
                if (resultType == NSManagedObjectIDResultType) {
                    NSMutableArray *objectIDs = [NSMutableArray arrayWithCapacity:[result count]];
                    for (NSManagedObject *obj in result) {
                        [objectIDs addObject:[obj objectID]];
                    }
                    result = objectIDs;
                }
            }
                break;
                
            case NSDictionaryResultType: {
                CBLView *view = [self.database existingViewNamed:@"cbtdb_all_by_type"];
                CBLQuery* query = [view query];
                query.keys = @[ entityName ];
                query.prefetch = YES;
                
                NSMutableArray *array = [NSMutableArray arrayWithCapacity:query.rows.count];
                for (CBLQueryRow *row in query.rows) {
                    NSDictionary *properties = row.documentProperties;
                    
                    if (!fetch.predicate || [fetch.predicate evaluateWithObject:properties]) {
                        
                        if (fetch.propertiesToFetch) {
                            [array addObject:[properties dictionaryWithValuesForKeys:fetch.propertiesToFetch]];
                        } else {
                            [array addObject:properties];
                        }
                    }
                }
                result = array;
            }
                break;
                
            case NSCountResultType: {
                NSUInteger count = 0;
                if (fetch.predicate) {
                    NSArray *array = [self queryObjectsOfEntity:entity byFetchRequest:fetch inContext:context];
                    count = array.count;
                } else {
                    CBLView *view = [self.database existingViewNamed:@"cbtdb_all_by_type"];
                    CBLQuery* query = [view query];
                    query.keys = @[ entityName ];
                    query.prefetch = NO;
                    
                    count = query.rows.count;
                }
                
                result = @[@(count)];
            }
                break;
            default:
                break;
        }
        
        CFAbsoluteTime end = CFAbsoluteTimeGetCurrent();
#ifndef PROFILE
        if (end - start > 1) {
#endif
            NSLog(@"[tdis] fetch request ---------------- \n"
                  "[tdis]   entity-name:%@\n"
                  "[tdis]   resultType:%@\n"
                  "[tdis]   fetchPredicate: %@\n"
                  "[tdis] --> took %f seconds\n"
                  "[tids]---------------- ",
                  entityName, CBResultTypeName(resultType), fetch.predicate, end - start);
#ifndef PROFILE
        }
#endif
        
        
        return result;
    } else {
        if (error) *error = [NSError errorWithDomain:kCBISIncrementalStoreErrorDomain
                                                code:3 userInfo:@{
                    NSLocalizedFailureReasonErrorKey: @"Unsupported requestType",
                             }];
        return nil;
    }
}

- (NSIncrementalStoreNode *)newValuesForObjectWithID:(NSManagedObjectID*)objectID withContext:(NSManagedObjectContext*)context error:(NSError**)error;
{
    CBLDocument* doc = [self.database documentWithID:[objectID couchDBIDRepresentation]];
    
    NSEntityDescription *entity = objectID.entity;
    if (![entity.name isEqual:[doc propertyForKey:kCBISTypeKey]]) {
        entity = [NSEntityDescription entityForName:[doc propertyForKey:kCBISTypeKey]
                             inManagedObjectContext:context];
    }
    
    NSDictionary *values = [self _coreDataPropertiesWithDocumentProperties:doc.properties withEntity:entity inContext:context];
    NSIncrementalStoreNode *node = [[NSIncrementalStoreNode alloc] initWithObjectID:objectID
                                                                         withValues:values
                                                                            version:1];
    
    return node;
}

- (id)newValueForRelationship:(NSRelationshipDescription*)relationship forObjectWithID:(NSManagedObjectID*)objectID withContext:(NSManagedObjectContext *)context error:(NSError **)error;
{
    if ([relationship isToMany]) {
        CBLView *view = [self.database existingViewNamed:CBCDBToManyViewNameForRelationship(relationship)];
        CBLQuery* query = [view query];
        
        query.keys = @[ [objectID couchDBIDRepresentation] ];
        
        NSMutableArray *result = [NSMutableArray arrayWithCapacity:query.rows.count];
        for (CBLQueryRow* row in query.rows) {
            [result addObject:[self _newObjectIDForEntity:relationship.destinationEntity
                                     managedObjectContext:context couchID:[row.value objectForKey:@"_id"]
                                                couchType:[row.value objectForKey:kCBISTypeKey]]];
        }
        
        return result;
    } else {
        CBLDocument* doc = [self.database documentWithID:[objectID couchDBIDRepresentation]];
        NSString *destinationID = [doc propertyForKey:relationship.name];
        if (destinationID) {
            return [self newObjectIDForEntity:relationship.destinationEntity referenceObject:destinationID];
        } else {
            return [NSNull null];
        }
    }
}

#pragma mark - Views

- (void) initializeViews
{
    NSMutableDictionary *subentitiesToSuperentities = [NSMutableDictionary dictionary];
    
    NSArray *entites = self.persistentStoreCoordinator.managedObjectModel.entities;
    for (NSEntityDescription *entity in entites) {
        
        NSArray *properties = [entity properties];
        
        for (NSPropertyDescription *property in properties) {
            
            if ([property isKindOfClass:[NSRelationshipDescription class]]) {
                NSRelationshipDescription *rel = (NSRelationshipDescription*)property;
                
                if (rel.isToMany) {
                    
                    NSMutableArray *entityNames = nil;
                    if (rel.destinationEntity.subentities.count > 0) {
                        entityNames = [NSMutableArray arrayWithCapacity:3];
                        for (NSEntityDescription *subentity in rel.destinationEntity.subentities) {
                            [entityNames addObject:subentity.name];
                        }
                    }
                    
                    NSString *viewName = CBCDBToManyViewNameForRelationship(rel);
                    NSString *destEntityName = rel.destinationEntity.name;
                    NSString *inverseRelNameLower = [rel.inverseRelationship.name lowercaseString];
                    if (entityNames.count == 0) {
                        CBLView *view = [self.database viewNamed:viewName];
                        [view setMapBlock:^(NSDictionary *doc, CBLMapEmitBlock emit) {
                            if ([[doc objectForKey:kCBISTypeKey] isEqual:destEntityName] && [doc objectForKey:inverseRelNameLower]) {
                                emit([doc objectForKey:inverseRelNameLower], @{@"_id": [doc valueForKey:@"_id"], kCBISTypeKey: [doc objectForKey:kCBISTypeKey]});
                            }
                        }
                                  version:@"1.0"];
                        
                        [self _setViewName:viewName forFetchingProperty:inverseRelNameLower fromEntity:destEntityName];
                        
                    } else {
                        CBLView *view = [self.database viewNamed:viewName];
                        [view setMapBlock:^(NSDictionary *doc, CBLMapEmitBlock emit) {
                                           if ([entityNames containsObject:[doc objectForKey:kCBISTypeKey]] && [doc objectForKey:inverseRelNameLower]) {
                                               emit([doc objectForKey:inverseRelNameLower], @{@"_id": [doc valueForKey:@"_id"], kCBISTypeKey: [doc objectForKey:kCBISTypeKey]});
                                           }
                                       }
                                        version:@"1.0"];
                        
                        // remember view for mapping super-entity and all sub-entities
                        [self _setViewName:viewName forFetchingProperty:inverseRelNameLower fromEntity:rel.destinationEntity.name];
                        for (NSString *entityName in entityNames) {
                            [self _setViewName:viewName forFetchingProperty:inverseRelNameLower fromEntity:entityName];
                        }
                        
                    }
                    
                }
                
            }
            
        }
        
        if (entity.subentities.count > 0) {
            for (NSEntityDescription *subentity in entity.subentities) {
                [subentitiesToSuperentities setObject:entity.name forKey:subentity.name];
            }
        }
    }
    
    CBLView *view = [self.database viewNamed:@"cbtdb_all_by_type"];
    [view setMapBlock:^(NSDictionary *doc, CBLMapEmitBlock emit) {
                       NSString* type = [doc objectForKey: kCBISTypeKey];
                       if (type) emit(type, doc);
                       
                       NSString *superentity = [subentitiesToSuperentities objectForKey:type];
                       if (superentity) {
                           emit(superentity, doc);
                       }
                   }
                    version:@"1.0"];
    view = [self.database viewNamed:@"cbtdb_id_by_type"];
    [view setMapBlock:^(NSDictionary *doc, CBLMapEmitBlock emit) {
                       NSString* type = [doc objectForKey:kCBISTypeKey];
                       if (type) {
                           NSDictionary *data = @{@"_id": [doc valueForKey:@"_id"], kCBISTypeKey: [doc objectForKey:kCBISTypeKey]};
                           emit(type, data);
                           
                           NSString *superentity = [subentitiesToSuperentities objectForKey:type];
                           if (superentity) {
                               emit(superentity, data);
                           }
                       }
                       
                   }
                    version:@"1.0"];
}

- (void) defineFetchViewForEntity:(NSString*)entityName
                       byProperty:(NSString*)propertyName
{
    NSString *viewName = [self _createViewNameForFetchingFromEntity:entityName byProperty:propertyName];

    CBLView *view = [self.database viewNamed:viewName];
    [view setMapBlock:^(NSDictionary *doc, CBLMapEmitBlock emit) {
        NSString* type = [doc objectForKey:kCBISTypeKey];
        if ([type isEqual:entityName] && [doc objectForKey:propertyName]) {
            emit([doc objectForKey:propertyName], doc);
        }
    }
              version:@"1.0"];
    
    [self _setViewName:viewName forFetchingProperty:propertyName fromEntity:entityName];
}

#pragma mark - Querying

- (NSArray*) queryObjectsOfEntity:(NSEntityDescription*)entity byFetchRequest:(NSFetchRequest*)fetch inContext:(NSManagedObjectContext*)context
{
    if ([self _cachedQueryResultsForEntity:entity.name predicate:fetch.predicate]) {
        return [self _cachedQueryResultsForEntity:entity.name predicate:fetch.predicate];
    }
    
    CBLQuery* query = [self queryForFetchRequest:fetch onEntity:entity];
    if (!query) {
        CBLView *view = [self.database existingViewNamed:@"cbtdb_all_by_type"];
        query = [view query];
        query.keys = @[ entity.name ];
        query.prefetch = fetch.predicate != nil;
    }
    
    NSArray *result = [self filterObjectsOfEntity:entity fromQuery:query byFetchRequest:fetch
                                        inContext:context];
    
    return result;
}

- (NSArray*) filterObjectsOfEntity:(NSEntityDescription*)entity fromQuery:(CBLQuery*)query byFetchRequest:(NSFetchRequest*)fetch inContext:(NSManagedObjectContext*)context
{
    if ([self _cachedQueryResultsForEntity:entity.name predicate:fetch.predicate]) {
        return [self _cachedQueryResultsForEntity:entity.name predicate:fetch.predicate];
    }
    
    NSMutableArray *array = [NSMutableArray arrayWithCapacity:query.rows.count];
    for (CBLQueryRow *row in query.rows) {
        if (!fetch.predicate || [self _evaluatePredicate:fetch.predicate withEntity:entity properties:row.documentProperties]) {
            NSManagedObjectID *objectID = [self _newObjectIDForEntity:entity managedObjectContext:context
                                                              couchID:[row.value valueForKey:@"_id"]
                                                            couchType:[row.value valueForKey:kCBISTypeKey]];
            NSManagedObject *object = [context objectWithID:objectID];
            [array addObject:object];
        }
    }
    
    [self _setCacheResults:array forEntity:entity.name predicate:fetch.predicate];
    
    return array;
}

- (CBLQuery*) queryForFetchRequest:(NSFetchRequest*)fetch onEntity:(NSEntityDescription*)entity
{
    NSPredicate *predicate = fetch.predicate;
    
    if (!predicate) return nil;
    
    if ([predicate isKindOfClass:[NSCompoundPredicate class]]) {
        if (((NSCompoundPredicate*)predicate).subpredicates.count == 1 ||
            ((NSCompoundPredicate*)predicate).compoundPredicateType == NSAndPredicateType) {
            predicate = ((NSCompoundPredicate*)predicate).subpredicates[0];
        } else {
            return nil;
        }
    }
    
    if (![predicate isKindOfClass:[NSComparisonPredicate class]]) {
        return nil;
    }
    
    NSComparisonPredicate *comparisonPredicate = (NSComparisonPredicate*)predicate;
    
    if (comparisonPredicate.predicateOperatorType != NSEqualToPredicateOperatorType &&
        comparisonPredicate.predicateOperatorType != NSNotEqualToPredicateOperatorType &&
        comparisonPredicate.predicateOperatorType != NSInPredicateOperatorType) {
        return nil;
    }
    
    if (comparisonPredicate.leftExpression.expressionType != NSKeyPathExpressionType) {
        return nil;
    }
    
    if (comparisonPredicate.rightExpression.expressionType != NSConstantValueExpressionType) {
        return nil;
    }
    
    if (![self _hasViewForFetchingFromEntity:entity.name byProperty:comparisonPredicate.leftExpression.keyPath]) {
        return nil;
    }
    
    NSString *viewName = [self _viewNameForFetchingFromEntity:entity.name byProperty:comparisonPredicate.leftExpression.keyPath];
    if (!viewName) {
        return nil;
    }

    CBLView *view = [self.database existingViewNamed:viewName];
    CBLQuery *query = [view query];
    if (comparisonPredicate.predicateOperatorType == NSEqualToPredicateOperatorType) {
        id rightValue = [comparisonPredicate.rightExpression constantValue];
        if ([rightValue isKindOfClass:[NSManagedObjectID class]]) {
            rightValue = [rightValue couchDBIDRepresentation];
        } else if ([rightValue isKindOfClass:[NSManagedObject class]]) {
            rightValue = [[rightValue objectID] couchDBIDRepresentation];
        }
        query.keys = @[ rightValue ];
        
    } else if (comparisonPredicate.predicateOperatorType == NSInPredicateOperatorType) {
        id rightValue = [comparisonPredicate.rightExpression constantValue];
        if ([rightValue isKindOfClass:[NSSet class]]) {
            rightValue = [[self _replaceManagedObjectsWithCouchIDInSet:rightValue] allObjects];
        } else if ([rightValue isKindOfClass:[NSArray class]]) {
            rightValue = [self _replaceManagedObjectsWithCouchIDInArray:rightValue];
        } else if (rightValue != nil) {
            NSAssert(NO, @"Wrong value in IN predicate rhv");
        }
        query.keys = rightValue;
    }
    query.prefetch = YES;
    
    return query;
}
- (NSArray*) _replaceManagedObjectsWithCouchIDInArray:(NSArray*)array
{
    NSMutableArray *result = [NSMutableArray arrayWithCapacity:array.count];
    for (id value in array) {
        if ([value isKindOfClass:[NSManagedObject class]]) {
            [result addObject:[[value objectID] couchDBIDRepresentation]];
        } else if ([value isKindOfClass:[NSManagedObjectID class]]) {
            [result addObject:[value couchDBIDRepresentation]];
        } else {
            [result addObject:value];
        }
    }
    return result;
}
- (NSSet*) _replaceManagedObjectsWithCouchIDInSet:(NSSet*)set
{
    NSMutableSet *result = [NSMutableSet setWithCapacity:set.count];
    for (id value in set) {
        if ([value isKindOfClass:[NSManagedObject class]]) {
            [result addObject:[[value objectID] couchDBIDRepresentation]];
        } else if ([value isKindOfClass:[NSManagedObjectID class]]) {
            [result addObject:[value couchDBIDRepresentation]];
        } else {
            [result addObject:value];
        }
    }
    return result;
}

#pragma mark - Replication

- (NSArray*) replicateWithURL:(NSURL*)replicationURL exclusively:(BOOL)exclusively
{
    return [self.database replicateWithURL:replicationURL exclusively:exclusively];
}
- (NSArray*) replications
{
    return self.database.allReplications;
}

#pragma mark - Change Handling

- (void) couchDocumentChanged:(CBLDocument*)document
{
    NSString *ident = document.documentID;
    BOOL deleted = [document isDeleted];
    
    [self addCouchDBChange:@{
     @"documentID": ident,
     @"deleted": @(deleted)
     }];
}

- (void) addCouchDBChange:(NSDictionary*)change
{
    [NSThread cancelPreviousPerformRequestsWithTarget:self selector:@selector(processCouchDBChanges) object:nil];
    
    @synchronized(self) {
        [_coalescedChanges addObject:change];
    }
    
    [self performSelector:@selector(processCouchDBChanges) withObject:nil afterDelay:1.0];
}

- (void) processCouchDBChanges
{
    NSArray *changes = nil;
    @synchronized(self) {
        changes = _coalescedChanges;
        _coalescedChanges = [[NSMutableArray alloc] initWithCapacity:20];
    }
    
    NSMutableSet *changedEntitites = [NSMutableSet setWithCapacity:changes.count];
    
    NSMutableArray *deletedObjectIDs = [NSMutableArray array];
    NSMutableArray *updatedObjectIDs = [NSMutableArray array];
    
    for (NSDictionary *change in changes) {
        NSString *ident = change[@"documentID"];
        BOOL deleted = [change[@"deleted"] boolValue];
        
        NSRange range = [ident rangeOfString:@"_"];
        if (range.location == NSNotFound) return;
        
        NSString *type = [ident substringToIndex:range.location];
        if ([type isEqual:@"cbtdb"]) return;
        
        
        NSString *reference = [ident substringFromIndex:range.location + 1];
        
        [changedEntitites addObject:type];
        
        if (deleted) {
            //        NSLog(@"[info] deleted : %@ : %@", type, ident);
            
            NSEntityDescription *entity = [self.persistentStoreCoordinator.managedObjectModel.entitiesByName objectForKey:type];
            NSManagedObjectID *objectID = [self newObjectIDForEntity:entity referenceObject:reference];
            
            [deletedObjectIDs addObject:objectID];
            
            return;
        }
        
        //    if ([type hasPrefix:@"cbtdb_"] || (!deleted && ![document propertyForKey:@"cbtdb_type"])) return;
        
        //    NSLog(@"[info] changed : %@ : %@", type, ident);
        
        NSEntityDescription *entity = [self.persistentStoreCoordinator.managedObjectModel.entitiesByName objectForKey:type];
        NSManagedObjectID *objectID = [self newObjectIDForEntity:entity referenceObject:reference];
        
        
        [updatedObjectIDs addObject:objectID];
    }
    
    NSDictionary *userInfo = @{
                               NSDeletedObjectsKey: deletedObjectIDs,
                               NSUpdatedObjectsKey: updatedObjectIDs
                               };
    
    [[NSNotificationCenter defaultCenter] postNotificationName:kCBISObjectHasBeenChangedInStoreNotification
                                                        object:self userInfo:userInfo];
    
}

@end
