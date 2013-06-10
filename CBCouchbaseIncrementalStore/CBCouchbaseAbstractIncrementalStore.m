//
//  CBCouchbaseAbstractIncrementalStore.m
//  CBCouchDBIncrementalStore
//
//  Created by Christian Beer on 07.06.13.
//  Copyright (c) 2013 Christian Beer. All rights reserved.
//

#import "CBCouchbaseAbstractIncrementalStore.h"

#import "NSString+CBISTemplate.h"


//#define PROFILE


NSString * const kCBISIncrementalStoreErrorDomain = @"CBISIncrementalStoreErrorDomain";
NSString * const kCBISTypeKey = @"cbis_type";
NSString * const kCBISCurrentRevisionAttributeName = @"cbisRev";
NSString * const kCBISManagedObjectIDPrefix = @"cb";
NSString * const kCBISDesignName = @"cbisDesign";
NSString * const kCBISMetadataDocumentID = @"cbis_metadata";
NSString * const kCBISAllByTypeViewName = @"cbis_all_by_type";
NSString * const kCBISIDByTypeViewName = @"cbis_id_by_type";
NSString * const kCBISFetchEntityByPropertyViewNameFormat = @"cbis_fetch_%@_by_%@";

NSString * const kCBISObjectHasBeenChangedInStoreNotification = @"kCBISObjectHasBeenChangedInStoreNotification";



@interface CBCouchbaseAbstractIncrementalStore ()
{
    NSMutableDictionary *_fetchRequestResultCache;
    
    NSMutableDictionary *_entityAndPropertyToFetchViewName;
}

@end


@implementation CBCouchbaseAbstractIncrementalStore

+ (void)initialize
{
    if (![[self class] isEqual:[CBCouchbaseAbstractIncrementalStore class]]) {
        [NSPersistentStoreCoordinator registerStoreClass:self
                                            forStoreType:[self type]];
    }
}

+ (NSString *)type
{
    NSAssert(NO, @"Please use one of the subclasses instead");
    return nil;
}

/** This has to be called once, before the NSManagedObjectModel is used by a NSPersistentStoreCoordinator. It adds an entity
 * for storing the current revision. */
+ (void) updateManagedObjectModel:(NSManagedObjectModel*)managedObjectModel
{
    NSArray *entites = managedObjectModel.entities;
    for (NSEntityDescription *entity in entites) {
        
        if (entity.superentity) { // only add to super-entities, not the sub-entities
            continue;
        }
        
        NSMutableArray *properties = [entity.properties mutableCopy];
        
        NSAttributeDescription *revAttribute = [NSAttributeDescription new];
        revAttribute.name = kCBISCurrentRevisionAttributeName;
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
    
    _entityAndPropertyToFetchViewName = [[NSMutableDictionary alloc] init];
    
    return self;
}

- (NSString *)type
{
    return [[self class] type];
}

#pragma mark - NSIncrementalStore

-(BOOL)loadMetadata:(NSError **)error;
{
    
    NSArray *entites = self.persistentStoreCoordinator.managedObjectModel.entities;
    for (NSEntityDescription *entity in entites) {
        
        NSDictionary *attributesByName = [entity attributesByName];
        
        if (![attributesByName objectForKey:kCBISCurrentRevisionAttributeName]) {
            if (error) *error = [NSError errorWithDomain:kCBISIncrementalStoreErrorDomain code:1
                                                userInfo:@{
                        NSLocalizedFailureReasonErrorKey: @"Database Model not compatible. You need to call +[updateManagedObjectModel:]."
                                 }];
            return NO;
        }
    }
    
    return YES;
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

- (NSManagedObjectID *)newObjectIDForEntity:(NSEntityDescription *)entity referenceObject:(id)data
{
    NSString *referenceObject = data;
    
    NSRange range = [referenceObject rangeOfString:@"_"];
    if (range.location != NSNotFound) referenceObject = [referenceObject substringFromIndex:range.location + 1];
    
    if ([referenceObject hasPrefix:@"p"]) {
        referenceObject = [referenceObject substringFromIndex:1];
    }
    
    // we need to prefix the refernceObject with a non-numeric prefix, because of a bug where
    // referenceObjects starting with a digit will only use the first digit part. As described here:
    // https://github.com/AFNetworking/AFIncrementalStore/issues/82
    referenceObject = [kCBISManagedObjectIDPrefix stringByAppendingString:referenceObject];
    NSManagedObjectID *objectID = [super newObjectIDForEntity:entity referenceObject:referenceObject];
    return objectID;
}

#pragma mark - Replication

- (NSArray*) replicateWithURL:(NSURL*)replicationURL exclusively:(BOOL)exclusively
{
    NSAssert(NO, @"Must be overwritten by subclass");
    return nil;
}
- (NSArray*) replications
{
    NSAssert(NO, @"Must be overwritten by subclass");
    return nil;
}

#pragma mark - Views

- (void) defineFetchViewForEntity:(NSString*)entityName
                       byProperty:(NSString*)propertyName
{
    NSAssert(NO, @"Must be overwritten by subclass");
}

@end

@implementation CBCouchbaseAbstractIncrementalStore (Private)

- (id) _couchDBRepresentationOfManagedObject:(NSManagedObject*)object
{
    return [self _couchDBRepresentationOfManagedObject:object withCouchDBID:NO];
}
- (id) _couchDBRepresentationOfManagedObject:(NSManagedObject*)object withCouchDBID:(BOOL)withCouchDBID
{
    NSEntityDescription *desc = object.entity;
    NSDictionary *propertyDesc = [desc propertiesByName];
    
    NSMutableDictionary *proxy = [NSMutableDictionary dictionary];
    
    [proxy setObject:desc.name
              forKey:kCBISTypeKey];
    
    if ([propertyDesc objectForKey:kCBISCurrentRevisionAttributeName]) {
        id rev = [object valueForKey:kCBISCurrentRevisionAttributeName];
        if (!CBCDBIsNull(rev)) {
            [proxy setObject:rev forKey:@"_rev"];
        }
    }
    
    if (withCouchDBID) {
        [proxy setObject:[object.objectID couchDBIDRepresentation] forKey:@"_id"];
    }
    
    for (NSString *property in propertyDesc) {
        if ([kCBISCurrentRevisionAttributeName isEqual:property]) continue;
        
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
                        value = CBCDBIsNull(value) ? nil : CBCBISOStringFromDate(value);
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

- (NSDictionary*) _coreDataPropertiesWithDocumentProperties:(NSDictionary*)properties withEntity:(NSEntityDescription*)entity inContext:(NSManagedObjectContext*)context
{
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
            if ([kCBISCurrentRevisionAttributeName isEqual:property]) {
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
                        value = CBCDBIsNull(value) ? nil : CBCBDateFromISOString(value);
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

- (NSManagedObjectID *)_newObjectIDForEntity:(NSEntityDescription *)entity managedObjectContext:(NSManagedObjectContext*)context
                                     couchID:(NSString*)couchID couchType:(NSString*)couchType
{
    if (![entity.name isEqual:couchType]) {
        entity = [NSEntityDescription entityForName:couchType inManagedObjectContext:context];
    }
    
    NSManagedObjectID *objectID = [self newObjectIDForEntity:entity referenceObject:couchID];
    return objectID;
}

#pragma - Caching

- (void) _setCacheResults:(NSArray*)array forEntity:(NSString*)entityName predicate:(NSPredicate*)predicate
{
    NSString *cacheKey = [NSString stringWithFormat:@"%@_%@", entityName, predicate];
    [_fetchRequestResultCache setObject:array forKey:cacheKey];
}

- (NSArray*) _cachedQueryResultsForEntity:(NSString*)entityName predicate:(NSPredicate*)predicate;
{
    NSString *cacheKey = [NSString stringWithFormat:@"%@_%@", entityName, predicate];
    return [_fetchRequestResultCache objectForKey:cacheKey];
}

- (void) _purgeCacheForEntityName:(NSString*)type
{
    for (NSString *key in [_fetchRequestResultCache allKeys]) {
        if ([key hasPrefix:type]) {
            [_fetchRequestResultCache removeObjectForKey:key];
        }
    }
}

#pragma mark - NSPredicate

- (BOOL) _evaluatePredicate:(NSPredicate*)predicate withEntity:(NSEntityDescription*)entity properties:(NSDictionary*)properties
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
            BOOL result = [self _evaluatePredicate:subpredicate withEntity:entity properties:properties];
            
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
        id leftValue = [self _evaluateExpression:comparisonPredicate.leftExpression withEntity:entity properties:properties];
        id rightValue = [self _evaluateExpression:comparisonPredicate.rightExpression withEntity:entity properties:properties];
        
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
- (id) _evaluateExpression:(NSExpression*)expression withEntity:(NSEntityDescription*)entity properties:(NSDictionary*)properties
{
    id value = nil;
    switch (expression.expressionType) {
        case NSConstantValueExpressionType:
            value = [expression constantValue];
            break;
        case NSEvaluatedObjectExpressionType:
            value = properties;
            break;
        case NSKeyPathExpressionType: {
            value = [properties objectForKey:expression.keyPath];
            if (!value) return nil;
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

#pragma mark - Views

- (NSString*) _createViewNameForFetchingFromEntity:(NSString*)entityName
                                  byProperty:(NSString*)propertyName
{
    NSString *viewName = [NSString stringWithFormat:kCBISFetchEntityByPropertyViewNameFormat, [entityName lowercaseString], propertyName];
    return viewName;
}

- (BOOL) _hasViewForFetchingFromEntity:(NSString*)entityName
                            byProperty:(NSString*)propertyName
{
    return [self _viewNameForFetchingFromEntity:entityName byProperty:propertyName] != nil;
}
- (NSString*) _viewNameForFetchingFromEntity:(NSString*)entityName
                                        byProperty:(NSString*)propertyName
{
    return [_entityAndPropertyToFetchViewName objectForKey:[NSString stringWithFormat:@"%@_%@", entityName, propertyName]];
}
- (void) _setViewName:(NSString*)viewName forFetchingProperty:(NSString*)propertyName fromEntity:(NSString*)entity
{
    [_entityAndPropertyToFetchViewName setObject:viewName
                                          forKey:[NSString stringWithFormat:@"%@_%@", entity, propertyName]];
}

@end


@implementation NSManagedObjectID (CBCouchDBIncrementalStore)

- (NSString*) couchDBIDRepresentation;
{
    NSString *uuid = [[self.URIRepresentation lastPathComponent] substringFromIndex:kCBISManagedObjectIDPrefix.length + 1];
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
    return [NSString stringWithFormat:@"cbis_%@_tomany_%@", entityName, destinationName];
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

NSDate *CBCBDateFromISOString(NSString *iso)
{
    struct tm tm;
    time_t t;
    
    if (iso.length == 10) {
        
        strptime([iso cStringUsingEncoding:NSUTF8StringEncoding], "%Y-%m-%d", &tm);
        tm.tm_isdst = -1;
        tm.tm_sec = 0;
        tm.tm_min = 0;
        tm.tm_hour = 0;
        t = mktime(&tm);
        
    } else {
        
        strptime([iso cStringUsingEncoding:NSUTF8StringEncoding], "%Y-%m-%dT%H:%M:%S%z", &tm);
        tm.tm_isdst = -1;
        t = mktime(&tm);
        
    }
    
    return [NSDate dateWithTimeIntervalSince1970:t + [[NSTimeZone localTimeZone] secondsFromGMT]];
}

NSString *CBCBISOStringFromDate(NSDate *date)
{
    struct tm *timeinfo;
    char buffer[80];
    
    time_t rawtime = [date timeIntervalSince1970] - [[NSTimeZone localTimeZone] secondsFromGMT];
    timeinfo = localtime(&rawtime);
    
    strftime(buffer, 80, "%Y-%m-%dT%H:%M:%SZ", timeinfo);
    
    return [NSString stringWithCString:buffer encoding:NSUTF8StringEncoding];
}
