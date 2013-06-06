//
//  CBCouchDBIncrementalStore.m
//  Christian Beer
//
//  Created by Christian Beer on 01.06.13.
//
//

#import "CBCouchDBIncrementalStore.h"

#import <CouchCocoa/CouchCocoa.h>

#import "NSString+CBTDBTemplate.h"


//#define PROFILE


NSString * const kCBTDBIncrementalStoreErrorDomain = @"CBTDBIncrementalStoreErrorDomain";
NSString * const kCBTDBTypeKey = @"cbtdb_type";

NSString * const kCBTDBDesignName = @"cbtdbDesign";
NSString * const kCBTDBFetchEntityByPropertyViewNameFormat = @"cbtdb_fetch_%@_by_%@";

NSString * const kCBTDBObjectHasBeenChangedInStoreNotification = @"kCBTDBObjectHasBeenChangedInStoreNotification";


BOOL CBCDBIsNull(id value);
NSString *CBCDBToManyViewNameForRelationship(NSRelationshipDescription *relationship);
NSString *CBResultTypeName(NSFetchRequestResultType resultType);


// "hack" for handling changes
typedef void (^OnDatabaseChangeBlock)(CouchDocument*, BOOL externalChange);
@interface CouchDatabase ()

- (void) onChange: (OnDatabaseChangeBlock)block;

@end


@interface CBCouchDBIncrementalStore ()
{
    NSMutableDictionary *_fetchRequestResultCache;
    
    NSMutableArray *_fetchViewNames;
    
    NSMutableArray *_coalescedChanges;
}

@property (nonatomic, strong, readwrite) CouchServer   *server;
@property (nonatomic, strong, readwrite) CouchDatabase *database;

@end


@implementation CBCouchDBIncrementalStore

+ (void)initialize
{
    [NSPersistentStoreCoordinator registerStoreClass:self forStoreType:[self type]];
}

+ (NSString *)type
{
    return @"CBCouchDBIncrementalStore";
}

+ (void) updateManagedObjectModel:(NSManagedObjectModel*)managedObjectModel
{
    NSArray *entites = managedObjectModel.entities;
    for (NSEntityDescription *entity in entites) {
        
        if (entity.superentity) { // only add to super-entities, not the sub-entities
            continue;
        }

        NSMutableArray *properties = [entity.properties mutableCopy];
        
        NSAttributeDescription *revAttribute = [NSAttributeDescription new];
        revAttribute.name = @"cbtdbRev";
        revAttribute.attributeType = NSStringAttributeType;
        revAttribute.optional = YES;
        revAttribute.indexed = YES;
        
        [properties addObject:revAttribute];
        
        entity.properties = properties;
    }
}


- (id)initWithPersistentStoreCoordinator:(NSPersistentStoreCoordinator *)root configurationName:(NSString *)name
                                     URL:(NSURL *)url options:(NSDictionary *)options
{
    self = [super initWithPersistentStoreCoordinator:root configurationName:name URL:url options:options];
    
    _fetchRequestResultCache = [[NSMutableDictionary alloc] init];
    
    _fetchViewNames = [[NSMutableArray alloc] init];
    
    _coalescedChanges = [[NSMutableArray alloc] init];
    
    return self;
}

- (NSString *)type
{
    return [[self class] type];
}

-(BOOL)loadMetadata:(NSError **)error;
{
    
    NSArray *entites = self.persistentStoreCoordinator.managedObjectModel.entities;
    for (NSEntityDescription *entity in entites) {

        NSDictionary *attributesByName = [entity attributesByName];
        
        if (![attributesByName objectForKey:@"cbtdbRev"]) {
            if (error) *error = [NSError errorWithDomain:kCBTDBIncrementalStoreErrorDomain code:1
                                                userInfo:@{
                        NSLocalizedFailureReasonErrorKey: @"Database Model not compatible. You need to call +[updateManagedObjectModel:]."
                                 }];
            return NO;
        }
    }
    
    
    NSString *databaseName = [self.URL lastPathComponent];
    NSURL *databaseURL = [self.URL URLByDeletingLastPathComponent];
    
    if ([databaseURL isFileURL]) {
        NSLog(@"[info] connecting to TouchDB server at path: %@", [databaseURL path]);
        CouchTouchDBServer *server = [[CouchTouchDBServer alloc] initWithServerPath:[databaseURL path]];
        if ([server error]) {
            if (error) *error = [server error];
            return NO;
        }
        self.server = server;
    } else {
        NSLog(@"[info] connecting to CouchDB server at URL: %@", databaseURL);
        CouchServer *server = [[CouchServer alloc] initWithURL:databaseURL];
        self.server = server;
    }
    
    NSLog(@"[info] opening database named: %@", databaseName);
    self.database = [self.server databaseNamed:databaseName];
    if (![self.database ensureCreated:error]) {
        return NO;
    }
    
    if ([self.server isKindOfClass:[CouchTouchDBServer class]]) {
        [self initializeViewsOnTouchDBDatabase:self.database];
    } else {
        [self initializeViewsOnCouchDBDatabase:self.database];
    }
    
    self.database.tracksChanges = YES;
    [self.database onChange:^(CouchDocument *doc, BOOL external){
        if (external) {
            [self couchDocumentChanged:doc];
        }
    }];

    CouchDocument *doc = [self.database documentWithID:@"cbtdb_metadata"];
    
    BOOL success = NO;
    
    NSDictionary *metaData = doc.properties;
    if (![metaData objectForKey:NSStoreUUIDKey]) {
        
        metaData = @{
                     NSStoreUUIDKey: [[NSProcessInfo processInfo] globallyUniqueString],
                     NSStoreTypeKey: [[self class] type]
                     };
        [self setMetadata:metaData];
        
        CouchDocument *doc = [self.database documentWithID:@"cbtdb_metadata"];
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
        
        // Merge updated and inserted objects... updated objects have a _rev
        NSMutableSet *updatedObjects = [[save insertedObjects] mutableCopy];
        [updatedObjects unionSet:[save updatedObjects]];
        
        NSMutableArray *properties = [NSMutableArray arrayWithCapacity:updatedObjects.count];
        NSMutableDictionary *documentIDToObjectID = [NSMutableDictionary dictionaryWithCapacity:updatedObjects.count];
        
        for (NSManagedObject *object in updatedObjects) {
            NSDictionary *contents = [self couchDBRepresentationOfManagedObject:object withCouchDBID:YES];
            
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

                [object willChangeValueForKey:@"cbtdbRev"];
                [object setPrimitiveValue:doc.currentRevisionID forKey:@"cbtdbRev"];
                [object didChangeValueForKey:@"cbtdbRev"];
                
                [context refreshObject:object mergeChanges:YES];
            }            
        } else {
            if (error) *error = [NSError errorWithDomain:kCBTDBIncrementalStoreErrorDomain
                                                    code:2 userInfo:@{
                        NSLocalizedFailureReasonErrorKey: @"Error putting inserted objects",
                                    NSUnderlyingErrorKey:op.error
                                 }];
        }
        
        // clear cache for entities to get changes
        for (NSString *entityName in changedEntities) {
            [self purgeCacheForEntityName:entityName];
        }
        
        
        // Objects that were deleted from the calling context.
        [[save deletedObjects] enumerateObjectsUsingBlock:^(id obj, BOOL *stop) {
            NSManagedObject *object = obj;
            
            CouchDocument* doc = [self.database documentWithID:[object.objectID couchDBIDRepresentation]];

            if (![doc propertyForKey:@"cbtdb_type"]) {
                NSLog(@"[debug] skip deleting document: %@", object.objectID);
                return;
            }
            
            RESTOperation* op = [doc DELETE];
            BOOL success = [op wait];
            if (!success) {
                if (error) *error = [NSError errorWithDomain:kCBTDBIncrementalStoreErrorDomain
                                                        code:2 userInfo:@{
                            NSLocalizedFailureReasonErrorKey: @"Error deleting object",
                                        NSUnderlyingErrorKey:op.error
                                     }];
            }
        }];

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
                CouchDesignDocument* design = [self.database designDocumentWithName:kCBTDBDesignName];
                
                CouchQuery* query = [design queryViewNamed:@"cbtdb_all_by_type"];
                query.keys = @[ entityName ];
                query.prefetch = YES;
                
                NSMutableArray *array = [NSMutableArray arrayWithCapacity:query.rows.count];
                for (CouchQueryRow *row in query.rows) {
                    NSDictionary *properties = row.documentProperties;
                    
                    if (!fetch.predicate || [fetch.predicate evaluateWithObject:properties]) {
                        [array addObject:properties];
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
                    CouchDesignDocument* design = [self.database designDocumentWithName:kCBTDBDesignName];
                    
                    CouchQuery* query = [design queryViewNamed:@"cbtdb_all_by_type"];
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
        if (end - start > 3) {
            NSLog(@"[tdis] fetch request ---------------- \n"
                  "[tdis]   entity-name:%@\n"
                  "[tdis]   resultType:%@\n"
                  "[tdis]   fetchPredicate: %@\n"
                  "[tdis] --> took %f seconds\n"
                  "[tids]---------------- ",
                  entityName, CBResultTypeName(resultType), fetch.predicate, end - start);
        }
        
        
        return result;
    } else {
        if (error) *error = [NSError errorWithDomain:kCBTDBIncrementalStoreErrorDomain
                                                code:3 userInfo:@{
                    NSLocalizedFailureReasonErrorKey: @"Unsupported requestType",
                             }];
        return nil;
    }
}

- (NSIncrementalStoreNode *)newValuesForObjectWithID:(NSManagedObjectID*)objectID withContext:(NSManagedObjectContext*)context error:(NSError**)error;
{
    CouchDocument* doc = [self.database documentWithID:[objectID couchDBIDRepresentation]];
    
    NSEntityDescription *entity = objectID.entity;
    if (![entity.name isEqual:[doc propertyForKey:kCBTDBTypeKey]]) {
        entity = [NSEntityDescription entityForName:[doc propertyForKey:kCBTDBTypeKey]
                             inManagedObjectContext:context];
    }
    
    NSDictionary *values = [self coreDataPropertiesOfDocument:doc withEntity:entity inContext:context];
    NSIncrementalStoreNode *node = [[NSIncrementalStoreNode alloc] initWithObjectID:objectID
                                                                         withValues:values
                                                                            version:1];
    
    return node;
}

- (id)newValueForRelationship:(NSRelationshipDescription*)relationship forObjectWithID:(NSManagedObjectID*)objectID withContext:(NSManagedObjectContext *)context error:(NSError **)error;
{
    if ([relationship isToMany]) {
        CouchDesignDocument* design = [self.database designDocumentWithName:kCBTDBDesignName];
        CouchQuery* query = [design queryViewNamed:CBCDBToManyViewNameForRelationship(relationship)];
        
        query.keys = @[ [objectID couchDBIDRepresentation] ];

        NSMutableArray *result = [NSMutableArray arrayWithCapacity:query.rows.count];
        for (CouchQueryRow* row in query.rows) {
            [result addObject:[self newObjectIDForEntity:relationship.destinationEntity
                                    managedObjectContext:context couchID:[row.value objectForKey:@"_id"]
                                               couchType:[row.value objectForKey:kCBTDBTypeKey]]];
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
}

- (NSArray *)obtainPermanentIDsForObjects:(NSArray *)array error:(NSError **)error
{
    NSMutableArray *result = [NSMutableArray arrayWithCapacity:array.count];
    for (NSManagedObject *object in array) {
        // if you call -[NSManagedObjectContext obtainPermanentIDsForObjects:error:] yourself,
        // this can get called with already permanent ids which leads to mismatch between store.
        if (![object.objectID isTemporaryID]) {
            
            [result addObject:object.objectID];
            
        } else {
        
            NSString *uuid = [[NSProcessInfo processInfo] globallyUniqueString];
            NSManagedObjectID *objectID = [self newObjectIDForEntity:object.entity
                                                     referenceObject:uuid];
            [result addObject:objectID];
            
        }
    }
    return result;
}

- (NSManagedObjectID *)newObjectIDForEntity:(NSEntityDescription *)entity managedObjectContext:(NSManagedObjectContext*)context
                                    couchID:(NSString*)couchID couchType:(NSString*)couchType
{
    if (![entity.name isEqual:couchType]) {
        entity = [NSEntityDescription entityForName:couchType inManagedObjectContext:context];
    }
    
    return [self newObjectIDForEntity:entity referenceObject:couchID];
}
- (NSManagedObjectID *)newObjectIDForEntity:(NSEntityDescription *)entity referenceObject:(id)data
{
    NSString *referenceObject = data;
    
    NSRange range = [referenceObject rangeOfString:@"_"];
    if (range.location != NSNotFound) referenceObject = [referenceObject substringFromIndex:range.location + 1];
    
    if ([referenceObject hasPrefix:@"p"]) {
        referenceObject = [referenceObject substringFromIndex:1];
    }
    
    return [super newObjectIDForEntity:entity referenceObject:referenceObject];
}

#pragma mark -

- (id) couchDBRepresentationOfManagedObject:(NSManagedObject*)object
{
    return [self couchDBRepresentationOfManagedObject:object withCouchDBID:NO];
}
- (id) couchDBRepresentationOfManagedObject:(NSManagedObject*)object withCouchDBID:(BOOL)withCouchDBID
{
    NSEntityDescription *desc = object.entity;
    NSDictionary *propertyDesc = [desc propertiesByName];
    
    NSMutableDictionary *proxy = [NSMutableDictionary dictionary];
    
    [proxy setObject:desc.name
              forKey:kCBTDBTypeKey];
    
    if ([propertyDesc objectForKey:@"cbtdbRev"]) {
        id rev = [object valueForKey:@"cbtdbRev"];
        if (!CBCDBIsNull(rev)) {
            [proxy setObject:rev forKey:@"_rev"];
        }
    }
    
    if (withCouchDBID) {
        [proxy setObject:[object.objectID couchDBIDRepresentation] forKey:@"_id"];
    }
    
    for (NSString *property in propertyDesc) {
        if ([@"cbtdbRev" isEqual:property]) continue;
        
        id desc = [propertyDesc objectForKey:property];
        
        if ([desc isKindOfClass:[NSAttributeDescription class]]) {
            NSAttributeDescription *attr = desc;
            
            if ([attr isTransient]) {
                continue;
            }
            
            id value = [object valueForKey:property];
            
            if (value) {
                switch ([attr attributeType]) {
                    case NSInteger16AttributeType:
                    case NSInteger32AttributeType:
                    case NSInteger64AttributeType:
                        value = [NSNumber numberWithLong:CBCDBIsNull(value) ? 0 : [value longValue]];
                        break;
                    case NSDecimalAttributeType:
                    case NSDoubleAttributeType:
                    case NSFloatAttributeType:
                        value = [NSNumber numberWithDouble:CBCDBIsNull(value) ? 0.0 : [value doubleValue]];
                        break;
                    case NSStringAttributeType:
                        value = CBCDBIsNull(value) ? @"" : value;
                        break;
                    case NSBooleanAttributeType:
                        value = [NSNumber numberWithBool:CBCDBIsNull(value) ? NO : [value boolValue]];
                        break;
                    case NSDateAttributeType:
                        value = CBCDBIsNull(value) ? nil : [RESTBody JSONObjectWithDate: [NSDate date]];
                        break;
                        /*
                         default:
                         //NSAssert(NO, @"Unsupported attribute type");
                         //break;
                         NSLog(@"ii unsupported attribute %@, type: %@ (%d)", attribute, attr, [attr attributeType]);
                         */
                }
                
                if (value) {
                    [proxy setObject:value forKey:property];
                }
                
            }
        } else if ([desc isKindOfClass:[NSRelationshipDescription class]]) {
            NSRelationshipDescription *rel = desc;
            
            id relationshipDestination = [object valueForKey:property];
            
            if (relationshipDestination) {
                if (![rel isToMany]) {
                    NSManagedObjectID *objectID = [relationshipDestination valueForKey:@"objectID"];
                    [proxy setObject:[objectID couchDBIDRepresentation] forKey:property];
                }
            }
            
        }
    }
    
    return proxy;
}

- (NSDictionary*) coreDataPropertiesOfDocument:(CouchDocument*)document withEntity:(NSEntityDescription*)entity inContext:(NSManagedObjectContext*)context
{
    NSDictionary *properties = [document properties];
    NSMutableDictionary *result = [NSMutableDictionary dictionaryWithCapacity:properties.count];
    
    NSDictionary *propertyDesc = [entity propertiesByName];
    
    for (NSString *property in propertyDesc) {
        id desc = [propertyDesc objectForKey:property];
        
        if ([desc isKindOfClass:[NSAttributeDescription class]]) {
            NSAttributeDescription *attr = desc;
            
            if ([attr isTransient]) {
                continue;
            }
            
            id value = nil;
            if ([@"cbtdbRev" isEqual:property]) {
                value = [properties objectForKey:@"_rev"];
            } else {
                value = [properties objectForKey:property];
            }
            
            if (value) {
                switch ([attr attributeType]) {
                    case NSInteger16AttributeType:
                    case NSInteger32AttributeType:
                    case NSInteger64AttributeType:
                        value = [NSNumber numberWithLong:CBCDBIsNull(value) ? 0 : [value longValue]];
                        break;
                    case NSDecimalAttributeType:
                    case NSDoubleAttributeType:
                    case NSFloatAttributeType:
                        value = [NSNumber numberWithDouble:CBCDBIsNull(value) ? 0.0 : [value doubleValue]];
                        break;
                    case NSStringAttributeType:
                        value = CBCDBIsNull(value) ? @"" : value;
                        break;
                    case NSBooleanAttributeType:
                        value = [NSNumber numberWithBool:CBCDBIsNull(value) ? NO : [value boolValue]];
                        break;
                    case NSDateAttributeType:
                        value = CBCDBIsNull(value) ? nil : [RESTBody dateWithJSONObject:value];
                        break;
                        /*
                         default:
                         //NSAssert(NO, @"Unsupported attribute type");
                         //break;
                         NSLog(@"ii unsupported attribute %@, type: %@ (%d)", attribute, attr, [attr attributeType]);
                         */
                }
                
                if (value) {
                    [result setObject:value forKey:property];
                }
                
            }
        } else if ([desc isKindOfClass:[NSRelationshipDescription class]]) {
            NSRelationshipDescription *rel = desc;
            
            if (![rel isToMany]) { // only handle to-one relationships
                id value = [properties objectForKey:property];
                
                if (!CBCDBIsNull(value)) {
                    NSManagedObjectID *destination = [self newObjectIDForEntity:rel.destinationEntity
                                                                referenceObject:value];
                
                    [result setObject:destination forKey:property];
                }
            }
        }
    }
    
    return result;
}

#pragma mark -

- (NSArray*) queryObjectsOfEntity:(NSEntityDescription*)entity byFetchRequest:(NSFetchRequest*)fetch inContext:(NSManagedObjectContext*)context
{
    NSString *cacheKey = [NSString stringWithFormat:@"%@_%@", entity.name, fetch.predicate];
    if ([_fetchRequestResultCache objectForKey:cacheKey]) {
        return [_fetchRequestResultCache objectForKey:cacheKey];
    }
    
    CouchQuery* query = [self queryForFetchRequest:fetch onEntity:entity];
    if (!query) {
        CouchDesignDocument* design = [self.database designDocumentWithName:kCBTDBDesignName];
        query = [design queryViewNamed:@"cbtdb_all_by_type"];
        query.keys = @[ entity.name ];
        query.prefetch = fetch.predicate != nil;
    }
    
    NSArray *result = [self filterObjectsOfEntity:entity fromQuery:query byFetchRequest:fetch
                                        inContext:context];

    return result;
}

- (NSArray*) filterObjectsOfEntity:(NSEntityDescription*)entity fromQuery:(CouchQuery*)query byFetchRequest:(NSFetchRequest*)fetch inContext:(NSManagedObjectContext*)context
{
    NSString *cacheKey = [NSString stringWithFormat:@"%@_%@", entity.name, fetch.predicate];
    if ([_fetchRequestResultCache objectForKey:cacheKey]) {
        return [_fetchRequestResultCache objectForKey:cacheKey];
    }
    
    NSMutableArray *array = [NSMutableArray arrayWithCapacity:query.rows.count];
    for (CouchQueryRow *row in query.rows) {
        if (!fetch.predicate || [self evaluatePredicate:fetch.predicate withEntity:entity queryRow:row]) {
            NSManagedObjectID *objectID = [self newObjectIDForEntity:entity managedObjectContext:context
                                                             couchID:[row.value valueForKey:@"_id"]
                                                           couchType:[row.value valueForKey:kCBTDBTypeKey]];
            NSManagedObject *object = [context objectWithID:objectID];
            [array addObject:object];
        }
    }
    
    [_fetchRequestResultCache setObject:array forKey:cacheKey];
    
    return array;
}

- (CouchQuery*) queryForFetchRequest:(NSFetchRequest*)fetch onEntity:(NSEntityDescription*)entity
{
    NSPredicate *predicate = fetch.predicate;
    
    if (!predicate) return nil;
    
    if ([predicate isKindOfClass:[NSCompoundPredicate class]]) {
        if (((NSCompoundPredicate*)predicate).subpredicates.count == 1) {
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
    
    NSString *viewName = [self viewNameForFetchingFromEntity:entity.name byProperty:comparisonPredicate.leftExpression.keyPath];
//    value = [self newObjectIDForEntity:rel.destinationEntity referenceObject:value];
//    }
    
    CouchDesignDocument* design = [self.database designDocumentWithName:kCBTDBDesignName];
    
    if (![_fetchViewNames containsObject:viewName]) {
        return nil;
    }
    
    CouchQuery *query = [design queryViewNamed:viewName];
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
            rightValue = [[self replaceManagedObjectsWithCouchIDInSet:rightValue] allObjects];
        } else if ([rightValue isKindOfClass:[NSArray class]]) {
            rightValue = [self replaceManagedObjectsWithCouchIDInArray:rightValue];
        } else {
            NSAssert(NO, @"Wrong value in IN predicate rhv");
        }
        query.keys = rightValue;
    }
    query.prefetch = YES;

    NSLog(@"[tdis] -->-> %@", viewName);

    return query;
}
- (NSArray*) replaceManagedObjectsWithCouchIDInArray:(NSArray*)array
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
- (NSSet*) replaceManagedObjectsWithCouchIDInSet:(NSSet*)set
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

#pragma mark - NSPredicate

- (BOOL) evaluatePredicate:(NSPredicate*)predicate withEntity:(NSEntityDescription*)entity queryRow:(CouchQueryRow*)row
{
    if ([predicate isKindOfClass:[NSCompoundPredicate class]]) {
        NSCompoundPredicate *compoundPredicate = (NSCompoundPredicate*)predicate;
        NSCompoundPredicateType type = compoundPredicate.compoundPredicateType;
        
        if (compoundPredicate.subpredicates.count == 0) {
            switch (type) {
                case NSAndPredicateType:
                    return YES;
                    break;
                case NSOrPredicateType:
                    return NO;
                    break;
                default:
                    return NO;
                    break;
            }
        }
        
        for (NSPredicate *subpredicate in compoundPredicate.subpredicates) {
            BOOL result = [self evaluatePredicate:subpredicate withEntity:entity queryRow:row];
            
            switch (type) {
                case NSAndPredicateType:
                    if (!result) return NO;
                    break;
                case NSOrPredicateType:
                    if (result) return YES;
                    break;
                case NSNotPredicateType:
                    return !result;
                    break;
                default:
                    break;
            }
        }
        return YES;
        
    } else if ([predicate isKindOfClass:[NSComparisonPredicate class]]) {
        NSComparisonPredicate *comparisonPredicate = (NSComparisonPredicate*)predicate;
        id leftValue = [self evaluateExpression:comparisonPredicate.leftExpression withEntity:entity onQueryRow:row];
        id rightValue = [self evaluateExpression:comparisonPredicate.rightExpression withEntity:entity onQueryRow:row];
        
        BOOL result = NO;
        switch (comparisonPredicate.predicateOperatorType) {
            case NSEqualToPredicateOperatorType:
                result = [leftValue isEqual:rightValue];
                break;
            case NSNotEqualToPredicateOperatorType:
                result = ![leftValue isEqual:rightValue];
                break;
            case NSInPredicateOperatorType:
                result = [rightValue containsObject:leftValue];
                break;
            case NSBeginsWithPredicateOperatorType:
                result = [leftValue hasPrefix:rightValue];
                break;
            case NSEndsWithPredicateOperatorType:
                result = [leftValue hasSuffix:rightValue];
                break;
                
                // note: the following are yet untested:
            case NSContainsPredicateOperatorType:
                result = [leftValue containsObject:rightValue];
                break;
                
            case NSLessThanPredicateOperatorType: // compare: returns NSOrderedAscending
                result = ([leftValue compare:rightValue] == NSOrderedAscending);
                break;
            case NSLessThanOrEqualToPredicateOperatorType: {// compare: returns NSOrderedAscending || NSOrderedSame
                NSComparisonResult comp = [leftValue compare:rightValue];
                result = (comp == NSOrderedAscending || comp == NSOrderedSame);
            }
                break;
            case NSGreaterThanPredicateOperatorType: // compare: returns NSOrderedDescending
                result = ([leftValue compare:rightValue] == NSOrderedDescending);
                break;
            case NSGreaterThanOrEqualToPredicateOperatorType:{ // compare: returns NSOrderedDescending || NSOrderedSame
                NSComparisonResult comp = [leftValue compare:rightValue];
                result = (comp == NSOrderedDescending || comp == NSOrderedSame);
            }
                break;
                
            case NSBetweenPredicateOperatorType: {
                id low = [rightValue objectAtIndex:0];
                id high = [rightValue objectAtIndex:1];
                NSComparisonResult compLow = [leftValue compare:low];
                NSComparisonResult compHigh = [leftValue compare:high];
                result = (compLow == NSOrderedAscending || compLow == NSOrderedSame) && (compHigh == NSOrderedDescending || compHigh == NSOrderedSame);
            }
                break;
                
            default:
                NSAssert(NO, @"[devel] PredicateOperatorType not implemented yet: %d", (int)comparisonPredicate.predicateOperatorType);
                break;
        }
        return result;
    }
    
    return NO;
}
- (id) evaluateExpression:(NSExpression*)expression withEntity:(NSEntityDescription*)entity onQueryRow:(CouchQueryRow*)row
{
    id value = nil;
    switch (expression.expressionType) {
        case NSConstantValueExpressionType:
            value = [expression constantValue];
            break;
        case NSEvaluatedObjectExpressionType:
            value = row.documentProperties;
            break;
        case NSKeyPathExpressionType: {
            value = [row.documentProperties objectForKey:expression.keyPath];
            NSPropertyDescription *property = [entity.propertiesByName objectForKey:expression.keyPath];
            if ([property isKindOfClass:[NSRelationshipDescription class]]) {
                // if it's a relationship it should be a MOCID
                NSRelationshipDescription *rel = (NSRelationshipDescription*)property;
                value = [self newObjectIDForEntity:rel.destinationEntity referenceObject:value];
            }
        }
            break;
            
        default:
            NSAssert(NO, @"[devel] Expression Type not yet supported: %@", expression);
            break;
    }
//    NSFunctionExpressionType,
//    NSAggregateExpressionType,
//    NSSubqueryExpressionType = 13,
//    NSUnionSetExpressionType,
//    NSIntersectSetExpressionType,
//    NSMinusSetExpressionType,
//    NSBlockExpressionType = 19
    
    return value;
}

#pragma mark - CouchDB

- (void) initializeViewsOnCouchDBDatabase:(CouchDatabase*)server
{
    CouchDesignDocument* design = [self.database designDocumentWithName:kCBTDBDesignName];

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
                            
                            [compare appendFormat:@"doc.cbtdb_type == '%@'", subentity.name];
                            
                            first = NO;
                        }
                        [compare appendString:@")"];
                        destEntityCompare = compare;
                    } else {
                        destEntityCompare = [NSString stringWithFormat:@"doc.cbtdb_type == '%@'", rel.destinationEntity.name];
                    }
                    
                    NSString *map = [NSString stringWithTemplate:@"function(doc) { if (${destEntityCompare} && doc.${entityNameLower}) { emit(doc.${destRelationshipNameLower}, {'_id': doc._id, 'cbtdb_type': doc.cbtdb_type}); } };"
                                                          values:@{
                                     @"entityName": entity.name,
                                     @"destEntityName": rel.destinationEntity.name,
                                     @"entityNameLower": [entity.name lowercaseString],
                                     @"destEntityNameLower": [rel.destinationEntity.name lowercaseString],
                                     @"destEntityCompare": destEntityCompare,
                                     @"destRelationshipNameLower": rel.inverseRelationship.name
                                     }];
                    
                    [design defineViewNamed:viewName
                                        map:map];
                    
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
        [superentityMapping appendFormat:@"if (doc.cbtdb_type == '%@') emit('%@', doc);", entityName, superentityName];
        
        if (!first) [superentityToIDMapping appendString:@" else "];
        [superentityToIDMapping appendFormat:@"if (doc.cbtdb_type == '%@') emit('%@', {'_id': doc._id, 'cbtdb_type': doc.cbtdb_type});", entityName, superentityName];
        first = NO;
    }
    
    NSString *map;
    map = [NSString stringWithFormat:@"function(doc){if (doc.cbtdb_type) emit(doc.cbtdb_type,doc); %@ };", superentityMapping];
    [design defineViewNamed:@"cbtdb_all_by_type"
                        map:map];
    map = [NSString stringWithFormat:@"function(doc){if (doc.cbtdb_type) emit(doc.cbtdb_type,{'_id': doc._id, 'cbtdb_type': doc.cbtdb_type}); %@ };", superentityToIDMapping];
    [design defineViewNamed:@"cbtdb_id_by_type"
                        map:map];
}

#pragma mark - TouchDB

- (void) initializeViewsOnTouchDBDatabase:(CouchDatabase*)server
{
    CouchDesignDocument* design = [self.database designDocumentWithName:kCBTDBDesignName];

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
                                           if ([[doc objectForKey:kCBTDBTypeKey] isEqual:destEntityName] && [doc objectForKey:inverseRelNameLower]) {
                                               emit([doc objectForKey:inverseRelNameLower], @{@"_id": [doc valueForKey:@"_id"], kCBTDBTypeKey: [doc objectForKey:kCBTDBTypeKey]});
                                           }
                                       }
                                        version:@"1.0"];
                    } else {
                        [design defineViewNamed:viewName
                                       mapBlock:^(NSDictionary *doc, TDMapEmitBlock emit) {
                                           if ([entityNames containsObject:[doc objectForKey:kCBTDBTypeKey]] && [doc objectForKey:inverseRelNameLower]) {
                                               emit([doc objectForKey:inverseRelNameLower], @{@"_id": [doc valueForKey:@"_id"], kCBTDBTypeKey: [doc objectForKey:kCBTDBTypeKey]});
                                           }
                                       }
                                        version:@"1.0"];
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
    
    [design defineViewNamed:@"cbtdb_all_by_type"
                   mapBlock:^(NSDictionary *doc, TDMapEmitBlock emit) {
                       NSString* type = [doc objectForKey: kCBTDBTypeKey];
                       if (type) emit(type, doc);
                       
                       NSString *superentity = [subentitiesToSuperentities objectForKey:type];
                       if (superentity) {
                           emit(superentity, doc);
                       }
                   }
                    version:@"1.0"];
    [design defineViewNamed:@"cbtdb_id_by_type"
                   mapBlock:^(NSDictionary *doc, TDMapEmitBlock emit) {
                       NSString* type = [doc objectForKey:kCBTDBTypeKey];
                       if (type) {
                           NSDictionary *data = @{@"_id": [doc valueForKey:@"_id"], kCBTDBTypeKey: [doc objectForKey:kCBTDBTypeKey]};
                           emit(type, data);

                           NSString *superentity = [subentitiesToSuperentities objectForKey:type];
                           if (superentity) {
                               emit(superentity, data);
                           }
                       }

                   }
                    version:@"1.0"];
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

#pragma - Change Handling

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
    
    [[NSNotificationCenter defaultCenter] postNotificationName:kCBTDBObjectHasBeenChangedInStoreNotification
                                                        object:self userInfo:userInfo];

}

- (void) couchDocumentChanged:(CouchDocument*)document
{
    NSString *ident = document.documentID;
    BOOL deleted = [document isDeleted];
    
    [self addCouchDBChange:@{
     @"documentID": ident,
     @"deleted": @(deleted)
     }];
}

- (void) purgeCacheForEntityName:(NSString*)type
{
    for (NSString *key in [_fetchRequestResultCache allKeys]) {
        if ([key hasPrefix:type]) {
            [_fetchRequestResultCache removeObjectForKey:key];
        }
    }
}

#pragma mark -

- (NSString*) viewNameForFetchingFromEntity:(NSString*)entityName
                                 byProperty:(NSString*)propertyName
{
    NSString *viewName = [NSString stringWithFormat:kCBTDBFetchEntityByPropertyViewNameFormat, [entityName lowercaseString], propertyName];
    return viewName;
}
- (void) defineFetchViewForEntity:(NSString*)entityName
                       byProperty:(NSString*)propertyName
                         mapBlock:(TDMapBlock)mapBlock
                          version:(NSString*)version
{
    NSString *viewName = [self viewNameForFetchingFromEntity:entityName byProperty:propertyName];
    CouchDesignDocument* design = [self.database designDocumentWithName:kCBTDBDesignName];
    [design defineViewNamed:viewName
                   mapBlock:mapBlock
                    version:version];
    [_fetchViewNames addObject:viewName];
}

@end


@implementation NSManagedObjectID (CBCouchDBIncrementalStore)

- (NSString*) couchDBIDRepresentation;
{
    NSString *uuid = [self.URIRepresentation lastPathComponent];
    NSString *ident = [NSString stringWithFormat:@"%@_%@", self.entity.name, uuid];
    return ident;
}

@end

//// utility methods

BOOL CBCDBIsNull(id value)
{
    return value == nil || [value isKindOfClass:[NSNull class]];
}

// returns name of a view that returns objectIDs for all destination entities of a to-many relationship
NSString *CBCDBToManyViewNameForRelationship(NSRelationshipDescription *relationship)
{
    NSString *entityName = [relationship.entity.name lowercaseString];
    NSString *destinationName = [relationship.destinationEntity.name lowercaseString];
    return [NSString stringWithFormat:@"cbtdb_%@_tomany_%@", entityName, destinationName];
}

NSString *CBResultTypeName(NSFetchRequestResultType resultType)
{
    switch (resultType) {
        case NSManagedObjectResultType:
            return @"NSManagedObjectResultType";
        case NSManagedObjectIDResultType:
            return @"NSManagedObjectIDResultType";
        case NSDictionaryResultType:
            return @"NSDictionaryResultType";
        case NSCountResultType:
            return @"NSCountResultType";
        default:
            return @"Unknown";
            break;
    }
}
