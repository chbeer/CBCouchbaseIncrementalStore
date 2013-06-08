//
//  CBCouchDBIncrementalStore_Unit_Test.m
//  CBCouchDBIncrementalStore Unit Test
//
//  Created by Christian Beer on 02.06.13.
//  Copyright (c) 2013 Christian Beer. All rights reserved.
//

#import "CBCouchDBIncrementalStore_Unit_Test.h"

#import <CouchCocoa/CouchCocoa.h>

#import "CBCouchCocoaIncrementalStore.h"

#import "Entry.h"
#import "Subentry.h"


@interface CBCouchCocoaIncrementalStore ()

@property (nonatomic, strong, readwrite) CouchDatabase *database;

@end



@implementation CBCouchDBIncrementalStore_Unit_Test
{
    NSManagedObjectContext          *_managedObjectContext;
    CBCouchCocoaIncrementalStore    *_persistentStore;
}

- (void)setUp
{
    [super setUp];
}

- (void)tearDown
{
    // Tear-down code here.
    
    [super tearDown];
 
    _managedObjectContext = nil;
    _persistentStore = nil;
}

- (void) testCRUD
{
    NSError *error;
    
    Entry *entry = [NSEntityDescription insertNewObjectForEntityForName:@"Entry"
                                                 inManagedObjectContext:[self managedObjectContext]];
    entry.created_at = [NSDate new];
    entry.text = @"Test";
    
    BOOL success = [[self managedObjectContext] save:&error];
    STAssertTrue(success, @"Could not save context: %@", error);
    
    CouchDocument *doc = [_persistentStore.database documentWithID:[entry.objectID couchDBIDRepresentation]];
    STAssertEqualObjects(entry.text, [doc propertyForKey:@"text"], @"Text should be equal");
    
    NSDate *date1 = entry.created_at;
    NSDate *date2 = [RESTBody dateWithJSONObject:[doc propertyForKey:@"created_at"]];
    int diffInSeconds = floor([date1 timeIntervalSinceDate:date2]);
    STAssertEquals(diffInSeconds, 0, @"Created-at date should be equal (without sub-seconds)");
    STAssertEqualObjects(entry.check, [doc propertyForKey:@"check"], @"Check should be equal");

    
    entry.check = @(YES);
    
    success = [[self managedObjectContext] save:&error];
    STAssertTrue(success, @"Could not save context after update: %@", error);

    doc = [_persistentStore.database documentWithID:[entry.objectID couchDBIDRepresentation]];
    STAssertEqualObjects(entry.check, [doc propertyForKey:@"check"], @"Check should be equal");
    STAssertEqualObjects(@(YES), [doc propertyForKey:@"check"], @"Check should be YES on document");

    
    NSManagedObjectID *objectID = entry.objectID;
    
    [_managedObjectContext deleteObject:entry];
    success = [_managedObjectContext save:&error];
    STAssertTrue(success, @"Could not save context after deletion: %@", error);
    
    doc = [_persistentStore.database documentWithID:[objectID couchDBIDRepresentation]];
    STAssertTrue([doc isDeleted], @"Document not marked as deleted after deletion");
}

- (void) testCreateAndUpdate
{
    NSError *error;
    
    Entry *entry = [NSEntityDescription insertNewObjectForEntityForName:@"Entry"
                                                 inManagedObjectContext:[self managedObjectContext]];
    entry.created_at = [NSDate new];
    entry.text = @"Test";
    
    BOOL success = [[self managedObjectContext] save:&error];
    STAssertTrue(success, @"Could not save context: %@", error);
    
    
    entry.check = @(YES);
    
    success = [[self managedObjectContext] save:&error];
    STAssertTrue(success, @"Could not save context after update: %@", error);

    
//    [_managedObjectContext deleteObject:entry];
//    
//    success = [managedObjectContext save:&error];
//    STAssertTrue(success, @"Could not save context after deletion: %@", error);

    Subentry *subentry = [NSEntityDescription insertNewObjectForEntityForName:@"Subentry"
                                                       inManagedObjectContext:[self managedObjectContext]];
    
    subentry.text = @"Subentry abc";
    
    [entry addSubentriesObject:subentry];
    
    success = [[self managedObjectContext] save:&error];
    STAssertTrue(success, @"Could not save context after update 2: %@", error);
    
    NSLog(@"[info] entry: %@", entry);
    
    subentry.number = @123;
    
    success = [[self managedObjectContext] save:&error];
    STAssertTrue(success, @"Could not save context after update 3: %@", error);
    
    NSManagedObjectID *objectID = entry.objectID;
    
    _managedObjectContext = nil;
    

    entry = (Entry*)[[self managedObjectContext] objectWithID:objectID];
    
    NSLog(@"[info] -----------------------------------------");
    [entry willAccessValueForKey:nil];
    NSLog(@"[info] == entry: %@", entry);
    for (Subentry *subentry in entry.subentries) {
        [subentry willAccessValueForKey:nil];
        NSLog(@"[info] ==  - subentry: %@", subentry);
    }
    NSLog(@"[info] -----------------------------------------");
}

- (void) testFetchRequests
{
    NSError *error;

    NSFetchRequest *fetchRequest = [[NSFetchRequest alloc] initWithEntityName:@"Entry"];

    fetchRequest.resultType = NSCountResultType;
    
    NSArray *result = [[self managedObjectContext] executeFetchRequest:fetchRequest error:&error];
    STAssertEquals(result.count, (NSUInteger)1, @"Fetch request should return one result that's a number");
    STAssertTrue([result[0] intValue] > 0, @"Database should contain more than zero entries (if the testCreateAndUpdate was run)");
    
    NSUInteger count = [result[0] intValue];
    
    fetchRequest.resultType = NSDictionaryResultType;
    
    result = [[self managedObjectContext] executeFetchRequest:fetchRequest error:&error];
    STAssertEquals(result.count, count, @"Fetch request should return same result count as number fetch");
    STAssertTrue([result[0] isKindOfClass:[NSDictionary class]], @"Results are not NSDictionaries");


    fetchRequest.resultType = NSManagedObjectIDResultType;
    
    result = [[self managedObjectContext] executeFetchRequest:fetchRequest error:&error];
    STAssertEquals(result.count, count, @"Fetch request should return same result count as number fetch");
    STAssertTrue([result[0] isKindOfClass:[NSManagedObjectID class]], @"Results are not NSManagedObjectIDs");

    
    fetchRequest.resultType = NSManagedObjectResultType;
    
    result = [[self managedObjectContext] executeFetchRequest:fetchRequest error:&error];
    STAssertEquals(result.count, count, @"Fetch request should return same result count as number fetch");
    STAssertTrue([result[0] isKindOfClass:[NSManagedObject class]], @"Results are not NSManagedObjects");

    //// Predicate

    Entry *entry = [NSEntityDescription insertNewObjectForEntityForName:@"Entry"
                                                 inManagedObjectContext:[self managedObjectContext]];
    entry.created_at = [NSDate new];
    entry.text = @"Test2";
    
    BOOL success = [[self managedObjectContext] save:&error];
    STAssertTrue(success, @"Could not save context: %@", error);

    fetchRequest.predicate = [NSPredicate predicateWithFormat:@"text == 'Test2'"];
    
    fetchRequest.resultType = NSCountResultType;

    result = [[self managedObjectContext] executeFetchRequest:fetchRequest error:&error];
    STAssertEquals(result.count, (NSUInteger)1, @"Fetch request should return one result that's a number");
    STAssertTrue([result[0] intValue] > 0, @"Database should contain more than zero entries (if the testCreateAndUpdate was run)");

    count = [result[0] intValue];
    
    fetchRequest.resultType = NSDictionaryResultType;
    
    result = [[self managedObjectContext] executeFetchRequest:fetchRequest error:&error];
    STAssertTrue(result.count == count, @"Fetch request should return same result count as number fetch");
    STAssertTrue([result[0] isKindOfClass:[NSDictionary class]], @"Results are not NSDictionaries");

    fetchRequest.resultType = NSManagedObjectIDResultType;
    
    result = [[self managedObjectContext] executeFetchRequest:fetchRequest error:&error];
    STAssertTrue(result.count == count, @"Fetch request should return same result count as number fetch");
    STAssertTrue([result[0] isKindOfClass:[NSManagedObjectID class]], @"Results are not NSManagedObjectIDs");

    fetchRequest.resultType = NSManagedObjectResultType;
    
    result = [[self managedObjectContext] executeFetchRequest:fetchRequest error:&error];
    STAssertTrue(result.count == count, @"Fetch request should return same result count as number fetch");
    STAssertTrue([result[0] isKindOfClass:[NSManagedObject class]], @"Results are not NSManagedObjects");
}

#pragma mark -

- (NSManagedObjectContext*) managedObjectContext
{
    if (_managedObjectContext) return _managedObjectContext;
    
    NSError *error = nil;
    NSURL *modelURL = [[NSBundle bundleForClass:[self class]] URLForResource:@"CouchDBIncrementalStore_iOSDemo" withExtension:@"momd"];
    STAssertNotNil(modelURL, @"Could not load Core Data model");
    
    NSManagedObjectModel* model = [[NSManagedObjectModel alloc] initWithContentsOfURL:modelURL];
    
    [CBCouchCocoaIncrementalStore updateManagedObjectModel:model];
    
    NSPersistentStoreCoordinator* coordinator = [[NSPersistentStoreCoordinator alloc] initWithManagedObjectModel:model];
    STAssertNotNil(coordinator, @"Could not init persistent store coordinator: %@", error);
    
    NSString *libraryDir = [NSSearchPathForDirectoriesInDomains(NSLibraryDirectory, NSUserDomainMask, YES) lastObject];
    NSString *databasePath = [libraryDir stringByAppendingPathComponent:@"test.touchdb"];
    NSURL *databaseURL = [[NSURL fileURLWithPath:databasePath] URLByAppendingPathComponent:@"test-eins"];
//    NSURL *databaseURL = [NSURL URLWithString:@"http://127.0.0.1:5984/test-eins"];
    
    NSPersistentStore *persistenStore = [coordinator addPersistentStoreWithType:[CBCouchCocoaIncrementalStore type]
                                                                  configuration:nil URL:databaseURL
                                                                        options:nil error:&error];
    STAssertNotNil(persistenStore, @"Could not add persistent store: %@", error);
    
    NSManagedObjectContext* managedObjectContext= [[NSManagedObjectContext alloc] init];
    managedObjectContext.persistentStoreCoordinator = coordinator;
    
    _persistentStore = (CBCouchCocoaIncrementalStore*)persistenStore;
    _managedObjectContext = managedObjectContext;
    
    return _managedObjectContext;
}

@end
