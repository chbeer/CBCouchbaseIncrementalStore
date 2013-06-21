//
//  CBCouchCocoaIncrementalStore.m
//  Christian Beer
//
//  Created by Christian Beer on 01.06.13.
//
//

#import "CBCouchCocoaIncrementalStore.h"

#import <CouchCocoa/CouchCocoa.h>

#import "NSString+CBISTemplate.h"

//#define PROFILE
//#define PROFILE_CSV


// "hack" for handling changes
typedef void (^OnDatabaseChangeBlock)(CouchDocument*, BOOL externalChange);
@interface CouchDatabase ()

- (void) onChange: (OnDatabaseChangeBlock)block;

@end

@interface CouchAttachment ()

- (id) initWithParent: (CouchResource*)parent
                 name: (NSString*)name
             metadata: (NSDictionary*)metadata;

@end



@interface CBCouchCocoaIncrementalStore ()

@property (nonatomic, strong, readwrite) CouchDatabase *database;

@end


@implementation CBCouchCocoaIncrementalStore
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

    CouchServer *couchServer;
    if ([databaseURL isFileURL]) {
        NSLog(@"[info] connecting to TouchDB server at path: %@", [databaseURL path]);
        CouchTouchDBServer *server = [[CouchTouchDBServer alloc] initWithServerPath:[databaseURL path]];
        if ([server error]) {
            if (error) *error = [server error];
            return NO;
        }
        couchServer = server;
    } else {
        NSLog(@"[info] connecting to CouchDB server at URL: %@", databaseURL);
        CouchServer *server = [[CouchServer alloc] initWithURL:databaseURL];
        couchServer = server;
    }
    
    NSLog(@"[info] opening database named: %@", databaseName);
    self.database = [couchServer databaseNamed:databaseName];
    if (![self.database ensureCreated:error]) {
        return NO;
    }
    
    if ([databaseURL isFileURL]) {
        [self initializeTouchDBViews];
    } else {
        [self initializeCouchDBViews];
    }
    
    self.database.tracksChanges = YES;
    [self.database onChange:^(CouchDocument *doc, BOOL external){
        if (external) {
            [self couchDocumentChanged:doc];
        }
    }];
    
    CouchDocument *doc = [self.database documentWithID:kCBISMetadataDocumentID];
    
    BOOL success = NO;
    
    NSDictionary *metaData = doc.properties;
    if (![metaData objectForKey:NSStoreUUIDKey]) {
        
        metaData = @{
                     NSStoreUUIDKey: [[NSProcessInfo processInfo] globallyUniqueString],
                     NSStoreTypeKey: [[self class] type]
                     };
        [self setMetadata:metaData];
        
        CouchDocument *doc = [self.database documentWithID:kCBISMetadataDocumentID];
        success = [[doc putProperties:metaData] wait];
        
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
        
        NSMutableArray *properties = [NSMutableArray arrayWithCapacity:[save insertedObjects].count];
        NSMutableDictionary *documentIDToObjectID = [NSMutableDictionary dictionaryWithCapacity:[save insertedObjects].count];
        
        // Objects that were inserted...
        for (NSManagedObject *object in [save insertedObjects]) {
            NSDictionary *contents = [self _couchDBRepresentationOfManagedObject:object withCouchDBID:YES];
            
            [properties addObject:contents];
            [documentIDToObjectID setObject:object.objectID forKey:[contents valueForKey:@"_id"]];
        }
        
        NSMutableSet *changedEntities = [NSMutableSet setWithCapacity:properties.count];
        
        RESTOperation *op = [self.database putChanges:properties];
        BOOL success = [op wait];
        if (success) {
            NSArray *result = op.resultObject;
            
            for (CouchDocument *doc in result) {
                NSManagedObjectID *objectID = [documentIDToObjectID objectForKey:doc.documentID];
                NSManagedObject *object = [context objectWithID:objectID];
                
                [changedEntities addObject:object.entity.name];
                
                [object willChangeValueForKey:kCBISCurrentRevisionAttributeName];
                [object setPrimitiveValue:doc.currentRevisionID forKey:kCBISCurrentRevisionAttributeName];
                [object didChangeValueForKey:kCBISCurrentRevisionAttributeName];
                
                [object willChangeValueForKey:@"objectID"];
                [context obtainPermanentIDsForObjects:@[object] error:nil];
                [object didChangeValueForKey:@"objectID"];
            }
        } else {
            if (error) *error = [NSError errorWithDomain:kCBISIncrementalStoreErrorDomain
                                                    code:2 userInfo:@{
                        NSLocalizedFailureReasonErrorKey: @"Error putting inserted objects",
                                    NSUnderlyingErrorKey:op.error
                                 }];
        }
        
        
        // clear cache for entities to get changes
        for (NSString *entityName in changedEntities) {
            [self _purgeCacheForEntityName:entityName];
        }
        
        
        // Objects that were updated...
        if ([save updatedObjects].count > 0) {
            properties = [NSMutableArray arrayWithCapacity:[save updatedObjects].count];
            documentIDToObjectID = [NSMutableDictionary dictionaryWithCapacity:[save updatedObjects].count];
            
            for (NSManagedObject *object in [save updatedObjects]) {
                NSDictionary *contents = [self _couchDBRepresentationOfManagedObject:object withCouchDBID:YES];
                
                [properties addObject:contents];
                [documentIDToObjectID setObject:object.objectID forKey:[contents valueForKey:@"_id"]];
            }
            
            op = [self.database putChanges:properties];
            success = [op wait];
            if (success) {
                NSArray *result = op.resultObject;
                
                for (CouchDocument *doc in result) {
                    NSManagedObjectID *objectID = [documentIDToObjectID objectForKey:doc.documentID];
                    NSManagedObject *object = [context objectWithID:objectID];
                    
                    [object willChangeValueForKey:kCBISCurrentRevisionAttributeName];
                    [object setPrimitiveValue:doc.currentRevisionID forKey:kCBISCurrentRevisionAttributeName];
                    [object didChangeValueForKey:kCBISCurrentRevisionAttributeName];
                    
                    [context refreshObject:object mergeChanges:NO];
                }
            } else {
                if (error) *error = [NSError errorWithDomain:kCBISIncrementalStoreErrorDomain
                                                        code:2 userInfo:@{
                            NSLocalizedFailureReasonErrorKey: @"Error putting updated objects",
                                        NSUnderlyingErrorKey:op.error
                                     }];
            }
        }
        
        
        // Objects that were deleted from the calling context...
        if ([save deletedObjects].count > 0) {
            NSMutableArray *documents = [NSMutableArray arrayWithCapacity:[save deletedObjects].count];
            
            for (NSManagedObject *object in [save deletedObjects]) {
                NSString *documentId = [object.objectID couchDBIDRepresentation];
                CouchDocument *doc = [self.database documentWithID:documentId];
                [documents addObject:doc];
            }
            
            op = [self.database deleteDocuments:documents];
            success = [op wait];
            if (!success) {
                if (error) *error = [NSError errorWithDomain:kCBISIncrementalStoreErrorDomain
                                                        code:2 userInfo:@{
                            NSLocalizedFailureReasonErrorKey: @"Error deleting objects",
                                        NSUnderlyingErrorKey:op.error
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
        NSPredicate *fetchPredicate = fetch.predicate;
        
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
                CouchDesignDocument* design = [self.database designDocumentWithName:kCBISDesignName];
                
                CouchQuery* query = [design queryViewNamed:kCBISAllByTypeViewName];
                query.keys = @[ entityName ];
                query.prefetch = YES;
                
                NSMutableArray *array = [NSMutableArray arrayWithCapacity:query.rows.count];
                for (CouchQueryRow *row in query.rows) {
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
                    CouchDesignDocument* design = [self.database designDocumentWithName:kCBISDesignName];
                    
                    CouchQuery* query = [design queryViewNamed:kCBISAllByTypeViewName];
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

#ifdef PROFILE_CSV
            NSLog(@"[tdis] fetch request;%f;%@;%@;%@;%@",
                  end - start, entityName, CBResultTypeName(resultType), fetchPredicate, fetch.sortDescriptors);
#else
            NSLog(@"[tdis] fetch request ---------------- \n"
                  "[tdis]   entity-name:%@\n"
                  "[tdis]   resultType:%@\n"
                  "[tdis]   fetchPredicate: %@\n"
                  "[tdis]   sortDescriptors: %@\n"
                  "[tdis] --> took %f seconds\n"
                  "[tids]---------------- ",
                  entityName, CBResultTypeName(resultType), fetchPredicate, fetch.sortDescriptors, end - start);
#endif
            
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
#ifdef PROFILE
    CFAbsoluteTime start = CFAbsoluteTimeGetCurrent();
#endif
    CouchDocument* doc = [self.database documentWithID:[objectID couchDBIDRepresentation]];
    
    NSEntityDescription *entity = objectID.entity;
    if (![entity.name isEqual:[doc propertyForKey:kCBISTypeKey]]) {
        entity = [NSEntityDescription entityForName:[doc propertyForKey:kCBISTypeKey]
                             inManagedObjectContext:context];
    }
    
    NSDictionary *values = [self _coreDataPropertiesOfDocumentWithID:doc.documentID properties:doc.properties withEntity:entity inContext:context];
    NSIncrementalStoreNode *node = [[NSIncrementalStoreNode alloc] initWithObjectID:objectID
                                                                         withValues:values
                                                                            version:1];
#ifdef PROFILE
    CFAbsoluteTime end = CFAbsoluteTimeGetCurrent();
#ifdef PROFILE_CSV
    NSLog(@"[tdis] newValuesForObjectWithID;%f", end - start);
#else
    NSLog(@"[tdis] ---- newValuesForObjectWithID : took %f", end - start);
#endif
    
#endif
    
    return node;
}

- (id)newValueForRelationship:(NSRelationshipDescription*)relationship forObjectWithID:(NSManagedObjectID*)objectID withContext:(NSManagedObjectContext *)context error:(NSError **)error;
{
#ifdef PROFILE
    CFAbsoluteTime start = CFAbsoluteTimeGetCurrent();
#endif

    id value = nil;
    
    if ([relationship isToMany]) {
        CouchDesignDocument* design = [self.database designDocumentWithName:kCBISDesignName];
        CouchQuery* query = [design queryViewNamed:CBCDBToManyViewNameForRelationship(relationship)];
        
        query.keys = @[ [objectID couchDBIDRepresentation] ];
        
        NSMutableArray *result = [NSMutableArray arrayWithCapacity:query.rows.count];
        for (CouchQueryRow* row in query.rows) {
            [result addObject:[self _newObjectIDForEntity:relationship.destinationEntity
                                     managedObjectContext:context
                                                  couchID:[row.value objectForKey:@"_id"]
                                                couchType:[row.value objectForKey:kCBISTypeKey]]];
        }
        
        return result;
    } else {
        CouchDocument* doc = [self.database documentWithID:[objectID couchDBIDRepresentation]];
        NSString *destinationID = [doc propertyForKey:relationship.name];
        if (destinationID) {
            return [self newObjectIDForEntity:relationship.destinationEntity referenceObject:destinationID];
        } else {
            return [NSNull null];
        }
    }
    
#ifdef PROFILE
    CFAbsoluteTime end = CFAbsoluteTimeGetCurrent();

#ifdef PROFILE_CSV
    NSLog(@"[tdis] newValueForRelationship;%f;%@", end - start, relationship.name);
#else
    NSLog(@"---- newValueForRelationship:%@ took %f", relationship.name, end - start);
#endif
    
#endif
    
    return value;
}

#pragma mark - Views

- (void) initializeCouchDBViews
{
    CouchDesignDocument* design = [self.database designDocumentWithName:kCBISDesignName];
    
    NSMutableDictionary *subentitiesToSuperentities = [NSMutableDictionary dictionary];
    
    NSArray *entites = self.persistentStoreCoordinator.managedObjectModel.entities;
    for (NSEntityDescription *entity in entites) {
        
        NSArray *properties = [entity properties];
        
        for (NSPropertyDescription *property in properties) {
            
            if ([property isKindOfClass:[NSRelationshipDescription class]]) {
                NSRelationshipDescription *rel = (NSRelationshipDescription*)property;
                
                if (rel.isToMany) {
                    
                    NSString *viewName = CBCDBToManyViewNameForRelationship(rel);
                    
                    NSString *destEntityCompare = nil;
                    if (rel.destinationEntity.subentities.count > 0) {
                        NSMutableString *compare = [NSMutableString stringWithString:@"("];
                        BOOL first = YES;
                        for (NSEntityDescription *subentity in rel.destinationEntity.subentities) {
                            if (!first) {
                                [compare appendString:@" || "];
                            }
                            
                            [compare appendFormat:@"doc.cbis_type == '%@'", subentity.name];
                            
                            first = NO;
                        }
                        [compare appendString:@")"];
                        destEntityCompare = compare;
                    } else {
                        destEntityCompare = [NSString stringWithFormat:@"doc.cbis_type == '%@'", rel.destinationEntity.name];
                    }
                    
                    NSString *inverseRelNameLower = [rel.inverseRelationship.name lowercaseString];
                    
                    NSString *map = [NSString stringWithTemplate:@"function(doc) { if (${destEntityCompare} && doc.${entityNameLower}) { emit(doc.${destRelationshipNameLower}, {'_id': doc._id, 'cbis_type': doc.cbis_type}); } };"
                                                          values:@{
                                     @"entityName": entity.name,
                                     @"destEntityName": rel.destinationEntity.name,
                                     @"entityNameLower": [entity.name lowercaseString],
                                     @"destEntityNameLower": [rel.destinationEntity.name lowercaseString],
                                     @"destEntityCompare": destEntityCompare,
                                     @"destRelationshipNameLower": inverseRelNameLower
                                     }];
                    
                    [design defineViewNamed:viewName
                                        map:map];
                    
                    
                    // remember view for mapping super-entity and all sub-entities
                    [self _setViewName:viewName forFetchingProperty:inverseRelNameLower fromEntity:rel.destinationEntity.name];
                    for (NSString *entityName in [rel.destinationEntity.subentities valueForKeyPath:@"name"]) {
                        [self _setViewName:viewName forFetchingProperty:inverseRelNameLower fromEntity:entityName];
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
    
    // to fetch all subentity-objects by superentity we need to map the superentity to the subentities, too
    NSMutableString *superentityMapping = [NSMutableString string];
    NSMutableString *superentityToIDMapping = [NSMutableString string];
    BOOL first = YES;
    for (NSString *entityName in subentitiesToSuperentities) {
        NSString *superentityName = [subentitiesToSuperentities objectForKey:entityName];
        if (!first) [superentityMapping appendString:@" else "];
        [superentityMapping appendFormat:@"if (doc.cbis_type == '%@') emit('%@', doc);", entityName, superentityName];
        
        if (!first) [superentityToIDMapping appendString:@" else "];
        [superentityToIDMapping appendFormat:@"if (doc.cbis_type == '%@') emit('%@', {'_id': doc._id, 'cbis_type': doc.cbis_type});", entityName, superentityName];
        first = NO;
    }
    
    NSString *map;
    map = [NSString stringWithFormat:@"function(doc){if (doc.cbis_type) emit(doc.cbis_type,doc); %@ };", superentityMapping];
    [design defineViewNamed:kCBISAllByTypeViewName
                        map:map];
    map = [NSString stringWithFormat:@"function(doc){if (doc.cbis_type) emit(doc.cbis_type,{'_id': doc._id, 'cbis_type': doc.cbis_type}); %@ };", superentityToIDMapping];
    [design defineViewNamed:kCBISIDByTypeViewName
                        map:map];
}

- (void) initializeTouchDBViews
{
    CouchDesignDocument* design = [self.database designDocumentWithName:kCBISDesignName];
    
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
                        [design defineViewNamed:viewName
                                       mapBlock:^(NSDictionary *doc, TDMapEmitBlock emit) {
                                           if ([[doc objectForKey:kCBISTypeKey] isEqual:destEntityName] && [doc objectForKey:inverseRelNameLower]) {
                                               emit([doc objectForKey:inverseRelNameLower], @{@"_id": [doc valueForKey:@"_id"], kCBISTypeKey: [doc objectForKey:kCBISTypeKey]});
                                           }
                                       }
                                        version:@"1.0"];

                        [self _setViewName:viewName forFetchingProperty:inverseRelNameLower fromEntity:destEntityName];
                        
                    } else {
                        [design defineViewNamed:viewName
                                       mapBlock:^(NSDictionary *doc, TDMapEmitBlock emit) {
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
    
    [design defineViewNamed:kCBISAllByTypeViewName
                   mapBlock:^(NSDictionary *doc, TDMapEmitBlock emit) {
                       NSString* type = [doc objectForKey: kCBISTypeKey];
                       if (type) emit(type, doc);
                       
                       NSString *superentity = [subentitiesToSuperentities objectForKey:type];
                       if (superentity) {
                           emit(superentity, doc);
                       }
                   }
                    version:@"1.0"];
    [design defineViewNamed:kCBISIDByTypeViewName
                   mapBlock:^(NSDictionary *doc, TDMapEmitBlock emit) {
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
    CouchDesignDocument* design = [self.database designDocumentWithName:kCBISDesignName];
    
    [design defineViewNamed:viewName
                   mapBlock:^(NSDictionary *doc, TDMapEmitBlock emit) {
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
    
    CouchQuery* query = [self queryForFetchRequest:fetch onEntity:entity];
    if (!query) {
        CouchDesignDocument* design = [self.database designDocumentWithName:kCBISDesignName];
        query = [design queryViewNamed:kCBISAllByTypeViewName];
        query.keys = @[ entity.name ];
        query.prefetch = fetch.predicate != nil;
    }
    
    NSArray *result = [self filterObjectsOfEntity:entity fromQuery:query byFetchRequest:fetch
                                        inContext:context];
    
    return result;
}

- (NSArray*) filterObjectsOfEntity:(NSEntityDescription*)entity fromQuery:(CouchQuery*)query byFetchRequest:(NSFetchRequest*)fetch inContext:(NSManagedObjectContext*)context
{
    if ([self _cachedQueryResultsForEntity:entity.name predicate:fetch.predicate]) {
        return [self _cachedQueryResultsForEntity:entity.name predicate:fetch.predicate];
    }
    
    NSMutableArray *array = [NSMutableArray arrayWithCapacity:query.rows.count];
    for (CouchQueryRow *row in query.rows) {
        if (!fetch.predicate || [self _evaluatePredicate:fetch.predicate withEntity:entity properties:row.documentProperties]) {
            NSString *documentID = [row.value valueForKey:@"_id"];
            NSString *documentType = [row.value valueForKey:kCBISTypeKey];
            NSManagedObjectID *objectID = [self _newObjectIDForEntity:entity managedObjectContext:context
                                                              couchID:documentID
                                                            couchType:documentType];
            NSManagedObject *object = [context objectWithID:objectID];
            [array addObject:object];
        }
    }
    
    [self _setCacheResults:array forEntity:entity.name predicate:fetch.predicate];
    
    return array;
}

- (CouchQuery*) queryForFetchRequest:(NSFetchRequest*)fetch onEntity:(NSEntityDescription*)entity
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
    
    CouchDesignDocument* design = [self.database designDocumentWithName:kCBISDesignName];
    
    CouchQuery *query = [design queryViewNamed:viewName];
    // we should remove the predicate used in the view to not re-evaluate it later
    if (comparisonPredicate.predicateOperatorType == NSEqualToPredicateOperatorType) {
        id rightValue = [comparisonPredicate.rightExpression constantValue];
        if ([rightValue isKindOfClass:[NSManagedObjectID class]]) {
            rightValue = [rightValue couchDBIDRepresentation];
        } else if ([rightValue isKindOfClass:[NSManagedObject class]]) {
            rightValue = [[rightValue objectID] couchDBIDRepresentation];
        }
        if (rightValue) {
            query.keys = @[ rightValue ];
        }
        
    } else if (comparisonPredicate.predicateOperatorType == NSInPredicateOperatorType) {
        id rightValue = [comparisonPredicate.rightExpression constantValue];
        if ([rightValue isKindOfClass:[NSSet class]]) {
            rightValue = [[self _replaceManagedObjectsWithCouchIDInSet:rightValue] allObjects];
        } else if ([rightValue isKindOfClass:[NSArray class]]) {
            rightValue = [self _replaceManagedObjectsWithCouchIDInArray:rightValue];
        } else if (rightValue != nil) {
            NSAssert(NO, @"Wrong value in IN predicate rhv");
        }
        if (rightValue) {
            query.keys = rightValue;
        }
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
    return self.database.replications;
}
- (void) stopReplication;
{
    [self.database replicateWithURL:nil exclusively:YES];
}

#pragma mark - Change Handling

- (void) couchDocumentChanged:(CouchDocument*)document
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
        if (range.location == NSNotFound) continue;
        
        NSString *type = [ident substringToIndex:range.location];
        if ([type isEqual:@"cbis"]) continue;
        
        NSString *reference = [ident substringFromIndex:range.location + 1];

        NSEntityDescription *entity = [self.persistentStoreCoordinator.managedObjectModel.entitiesByName objectForKey:type];
        if (!entity) continue;

        [changedEntitites addObject:type];
        
        NSManagedObjectID *objectID = [self newObjectIDForEntity:entity referenceObject:reference];
        
        if (deleted) {

            [deletedObjectIDs addObject:objectID];
            
        } else {

            [updatedObjectIDs addObject:objectID];
            
        }
    }
    
    NSDictionary *userInfo = @{
                               NSDeletedObjectsKey: deletedObjectIDs,
                               NSUpdatedObjectsKey: updatedObjectIDs
                               };
    
    [[NSNotificationCenter defaultCenter] postNotificationName:kCBISObjectHasBeenChangedInStoreNotification
                                                        object:self userInfo:userInfo];
    
}

#pragma mark - Attachments

- (NSData*) _loadDataForAttachmentWithName:(NSString*)name ofDocumentWithID:(NSString*)documentID metadata:(NSDictionary*)metadata
{
    CouchDocument *doc = [self.database documentWithID:documentID];
    CouchAttachment *att = [[CouchAttachment alloc] initWithParent:doc
                                                              name:name
                                                          metadata:metadata];
    
    return att.body;
}

@end