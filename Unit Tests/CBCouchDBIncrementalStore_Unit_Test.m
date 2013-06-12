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
#import "File.h"

typedef void(^CBISAssertionBlock)(NSArray *result, NSFetchRequestResultType resultType);


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
    
    [self deleteDatabase];
    
    Entry *entry = [NSEntityDescription insertNewObjectForEntityForName:@"Entry"
                                                 inManagedObjectContext:[self managedObjectContext]];
    entry.created_at = [NSDate new];
    entry.text = @"Test";
    entry.check = @(YES);
    
    BOOL success = [[self managedObjectContext] save:&error];
    STAssertTrue(success, @"Could not save context: %@", error);
    

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

    entry = [NSEntityDescription insertNewObjectForEntityForName:@"Entry"
                                          inManagedObjectContext:[self managedObjectContext]];
    entry.created_at = [NSDate new];
    entry.text = @"Test2";
    
    success = [[self managedObjectContext] save:&error];
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

- (void) testAttachment
{
    NSError *error;

    [self deleteDatabase];

    File *file = [NSEntityDescription insertNewObjectForEntityForName:@"File"
                                               inManagedObjectContext:[self managedObjectContext]];
    file.filename = @"test.txt";
    
    NSData *data = [@"Test. Hello World" dataUsingEncoding:NSUTF8StringEncoding];
    file.data = data;
    
    BOOL success = [[self managedObjectContext] save:&error];
    STAssertTrue(success, @"Could not save context: %@", error);
    
    CouchDocument *doc = [_persistentStore.database documentWithID:[file.objectID couchDBIDRepresentation]];
    STAssertNotNil(doc, @"Document should not be nil");
    STAssertEqualObjects(file.filename, [doc propertyForKey:@"filename"], @"fileName should be equal");
    
    CouchAttachment *att = [[CouchAttachment alloc] initWithParent:doc relativePath:@"data"];
    STAssertNotNil(att, @"Attachmant should be created");
    
    NSData *body = att.body;
    STAssertNotNil(body, @"Body should be loaded");
    STAssertEquals(body.length, data.length, @"Data length should be equal");
    STAssertEqualObjects(body, data, @"Data should be equal");
}

- (void) testFetchWithPredicates
{
    NSError *error;

    [self deleteDatabase];

    NSDictionary *entry1 = @{
                             @"created_at": [NSDate new],
                             @"text": @"This is a test for predicates. Möhre.",
                             @"text2": @"This is text2.",
                             @"number": [NSNumber numberWithInt:10],
                             @"decimalNumber": [NSDecimalNumber decimalNumberWithString:@"123.456"],
                             @"doubleNumber": [NSNumber numberWithDouble:42.23]
                             };
    NSDictionary *entry2 = @{
                             @"created_at": [[NSDate new] dateByAddingTimeInterval:-60],
                             @"text": @"Entry number 2. touché.",
                             @"text2": @"Text 2 by Entry number 2",
                             @"number": [NSNumber numberWithInt:20],
                             @"decimalNumber": [NSDecimalNumber decimalNumberWithString:@"345.432"],
                             @"doubleNumber": [NSNumber numberWithDouble:12.45]
                             };
    NSDictionary *entry3 = @{
                             @"created_at": [[NSDate new] dateByAddingTimeInterval:60],
                             @"text": @"Entry number 3",
                             @"text2": @"Text 2 by Entry number 3",
                             @"number": [NSNumber numberWithInt:30],
                             @"decimalNumber": [NSDecimalNumber decimalNumberWithString:@"32.23"],
                             @"doubleNumber": [NSNumber numberWithDouble:98.76]
                             };
    
    [self insertEntriesWithProperties:@[entry1, entry2, entry3]];
    
    BOOL success = [[self managedObjectContext] save:&error];
    STAssertTrue(success, @"Could not save context: %@", error);

    NSFetchRequest *fetchRequest = [NSFetchRequest fetchRequestWithEntityName:@"Entry"];
    
    //// ==
    fetchRequest.predicate = [NSPredicate predicateWithFormat:@"text == %@", entry1[@"text"]];
    [self assertFetchRequest:fetchRequest assertionBlock:^(NSArray *result, NSFetchRequestResultType resultType) {
        STAssertEquals((int)result.count, 1, @"Count should be 1");
        if (result.count != 1) return;
        STAssertEqualObjects([result[0] valueForKey:@"text"], entry1[@"text"], @"Text should be equal");
    }];
    fetchRequest.predicate = [NSPredicate predicateWithFormat:@"number == %@", entry1[@"number"]];
    [self assertFetchRequest:fetchRequest assertionBlock:^(NSArray *result, NSFetchRequestResultType resultType) {
        STAssertEquals((int)result.count, 1, @"Count should be 1");
        if (result.count != 1) return;
        STAssertEqualObjects([result[0] valueForKey:@"number"], entry1[@"number"], @"Number should be equal");
    }];
    
    //// >=
    fetchRequest.predicate = [NSPredicate predicateWithFormat:@"number >= %@", entry2[@"number"]];
    [self assertFetchRequest:fetchRequest assertionBlock:^(NSArray *result, NSFetchRequestResultType resultType) {
        STAssertEquals((int)result.count, 2, @"Count should be 2");
        if (result.count != 2) return;
        NSArray *numbers = [[result valueForKey:@"number"] sortedArrayUsingSelector:@selector(compare:)];
        STAssertEqualObjects(numbers[0], entry2[@"number"], @"Number should be equal");
        STAssertEqualObjects(numbers[1], entry3[@"number"], @"Number should be equal");
    }];

    //// <=
    fetchRequest.predicate = [NSPredicate predicateWithFormat:@"number <= %@", entry2[@"number"]];
    [self assertFetchRequest:fetchRequest assertionBlock:^(NSArray *result, NSFetchRequestResultType resultType) {
        STAssertEquals((int)result.count, 2, @"Count should be 2");
        if (result.count != 2) return;
        NSArray *numbers = [[result valueForKey:@"number"] sortedArrayUsingSelector:@selector(compare:)];
        STAssertEqualObjects(numbers[0], entry1[@"number"], @"Number should be equal");
        STAssertEqualObjects(numbers[1], entry2[@"number"], @"Number should be equal");
    }];

    //// >
    fetchRequest.predicate = [NSPredicate predicateWithFormat:@"number > %@", entry2[@"number"]];
    [self assertFetchRequest:fetchRequest assertionBlock:^(NSArray *result, NSFetchRequestResultType resultType) {
        STAssertEquals((int)result.count, 1, @"Count should be 1");
        if (result.count != 1) return;
        NSArray *numbers = [[result valueForKey:@"number"] sortedArrayUsingSelector:@selector(compare:)];
        STAssertEqualObjects(numbers[0], entry3[@"number"], @"Number should be equal");
    }];
    
    //// <
    fetchRequest.predicate = [NSPredicate predicateWithFormat:@"number < %@", entry2[@"number"]];
    [self assertFetchRequest:fetchRequest assertionBlock:^(NSArray *result, NSFetchRequestResultType resultType) {
        STAssertEquals((int)result.count, 1, @"Count should be 1");
        if (result.count != 1) return;
        NSArray *numbers = [[result valueForKey:@"number"] sortedArrayUsingSelector:@selector(compare:)];
        STAssertEqualObjects(numbers[0], entry1[@"number"], @"Number should be equal");
    }];
    
    //// !=
    fetchRequest.predicate = [NSPredicate predicateWithFormat:@"number != %@", entry2[@"number"]];
    [self assertFetchRequest:fetchRequest assertionBlock:^(NSArray *result, NSFetchRequestResultType resultType) {
        STAssertEquals((int)result.count, 2, @"Count should be 2");
        if (result.count != 2) return;
        NSArray *numbers = [[result valueForKey:@"number"] sortedArrayUsingSelector:@selector(compare:)];
        STAssertEqualObjects(numbers[0], entry1[@"number"], @"Number should be equal");
        STAssertEqualObjects(numbers[1], entry3[@"number"], @"Number should be equal");
    }];

    //// BETWEEN
    fetchRequest.predicate = [NSPredicate predicateWithFormat:@"number BETWEEN %@", @[entry1[@"number"], entry2[@"number"]]];
    [self assertFetchRequest:fetchRequest assertionBlock:^(NSArray *result, NSFetchRequestResultType resultType) {
        STAssertEquals((int)result.count, 2, @"Count should be 2");
        if (result.count != 2) return;
        NSArray *numbers = [[result valueForKey:@"number"] sortedArrayUsingSelector:@selector(compare:)];
        STAssertEqualObjects(numbers[0], entry1[@"number"], @"Number should be equal");
        STAssertEqualObjects(numbers[1], entry2[@"number"], @"Number should be equal");
    }];

    //// BEGINSWITH
    fetchRequest.predicate = [NSPredicate predicateWithFormat:@"text BEGINSWITH 'Entry'"];
    [self assertFetchRequest:fetchRequest assertionBlock:^(NSArray *result, NSFetchRequestResultType resultType) {
        STAssertEquals((int)result.count, 2, @"Count should be 2");
        if (result.count != 2) return;
        STAssertEquals((int)[[result[0] valueForKey:@"text"] rangeOfString:@"Entry"].location, 0, @"Text should be found");
        STAssertEquals((int)[[result[1] valueForKey:@"text"] rangeOfString:@"Entry"].location, 0, @"Text should be found");
    }];

    //// CONTAINS
    fetchRequest.predicate = [NSPredicate predicateWithFormat:@"text CONTAINS 'test'"];
    [self assertFetchRequest:fetchRequest assertionBlock:^(NSArray *result, NSFetchRequestResultType resultType) {
        STAssertEquals((int)result.count, 1, @"Count should be 1");
        STAssertTrue([[result[0] valueForKey:@"text"] rangeOfString:@"test"].location != NSNotFound, @"Text should be found");
    }];
    fetchRequest.predicate = [NSPredicate predicateWithFormat:@"text CONTAINS[c] 'This'"];
    [self assertFetchRequest:fetchRequest assertionBlock:^(NSArray *result, NSFetchRequestResultType resultType) {
        STAssertEquals((int)result.count, 1, @"Count should be 1");
        STAssertTrue([[result[0] valueForKey:@"text"] rangeOfString:@"test"].location != NSNotFound, @"Text should be found");
    }];
    fetchRequest.predicate = [NSPredicate predicateWithFormat:@"text CONTAINS[c] 'this'"];
    [self assertFetchRequest:fetchRequest assertionBlock:^(NSArray *result, NSFetchRequestResultType resultType) {
        STAssertEquals((int)result.count, 1, @"Count should be 1");
        STAssertTrue([[result[0] valueForKey:@"text"] rangeOfString:@"test"].location != NSNotFound, @"Text should be found");
    }];
    fetchRequest.predicate = [NSPredicate predicateWithFormat:@"text CONTAINS 'this'"];
    [self assertFetchRequest:fetchRequest assertionBlock:^(NSArray *result, NSFetchRequestResultType resultType) {
        STAssertEquals((int)result.count, 0, @"Count should be 0");
    }];
    fetchRequest.predicate = [NSPredicate predicateWithFormat:@"text CONTAINS 'touche'"];
    [self assertFetchRequest:fetchRequest assertionBlock:^(NSArray *result, NSFetchRequestResultType resultType) {
        STAssertEquals((int)result.count, 0, @"Count should be 0");
    }];
    fetchRequest.predicate = [NSPredicate predicateWithFormat:@"text CONTAINS[d] 'touche'"];
    [self assertFetchRequest:fetchRequest assertionBlock:^(NSArray *result, NSFetchRequestResultType resultType) {
        STAssertEquals((int)result.count, 1, @"Count should be 1");
    }];
    
    //// ENDSWITH
    fetchRequest.predicate = [NSPredicate predicateWithFormat:@"text ENDSWITH 'touché.'"];
    [self assertFetchRequest:fetchRequest assertionBlock:^(NSArray *result, NSFetchRequestResultType resultType) {
        STAssertEquals((int)result.count, 1, @"Count should be 1");
        STAssertTrue([[result[0] valueForKey:@"text"] rangeOfString:@"touché."].location != NSNotFound, @"Text should be found");
    }];

    //// LIKE
    fetchRequest.predicate = [NSPredicate predicateWithFormat:@"text LIKE '*number ?*'"];
    [self assertFetchRequest:fetchRequest assertionBlock:^(NSArray *result, NSFetchRequestResultType resultType) {
        STAssertEquals((int)result.count, 2, @"Count should be 2");
    }];

    //// MATCH
// this test fails, although I think it should be correctly filter the second and third entries...: Need to investigate more
//    fetchRequest.predicate = [NSPredicate predicateWithFormat:@"text MATCHES %@", @"^Entry"];
//    [self assertFetchRequest:fetchRequest assertionBlock:^(NSArray *result, NSFetchRequestResultType resultType) {
//        STAssertEquals((int)result.count, 2, @"Count should be 2");
//    }];
}

#pragma mark -

- (NSURL*) databaseURL
{
    NSString *libraryDir = [NSSearchPathForDirectoriesInDomains(NSLibraryDirectory, NSUserDomainMask, YES) lastObject];
    NSString *databasePath = [libraryDir stringByAppendingPathComponent:@"test.touchdb"];
    NSURL *databaseURL = [[NSURL fileURLWithPath:databasePath] URLByAppendingPathComponent:@"test-eins"];
    //    NSURL *databaseURL = [NSURL URLWithString:@"http://127.0.0.1:5984/test-eins"];
    return databaseURL;
}

- (void) deleteDatabase
{
    _managedObjectContext = nil;
    NSError *error;
    BOOL success = [[NSFileManager defaultManager] removeItemAtURL:[[self databaseURL] URLByDeletingLastPathComponent] error:&error];
    STAssertTrue(success, @"Couldn't delete database: %@", error);
}

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
    
    NSPersistentStore *persistenStore = [coordinator addPersistentStoreWithType:[CBCouchCocoaIncrementalStore type]
                                                                  configuration:nil URL:[self databaseURL]
                                                                        options:nil error:&error];
    STAssertNotNil(persistenStore, @"Could not add persistent store: %@", error);
    
    NSManagedObjectContext* managedObjectContext= [[NSManagedObjectContext alloc] init];
    managedObjectContext.persistentStoreCoordinator = coordinator;
    
    _persistentStore = (CBCouchCocoaIncrementalStore*)persistenStore;
    _managedObjectContext = managedObjectContext;
    
    return _managedObjectContext;
}

- (void) assertFetchRequest:(NSFetchRequest*)fetchRequest assertionBlock:(CBISAssertionBlock)assertionBlock
{
    NSFetchRequestResultType resultTypes[] = {NSManagedObjectResultType, NSDictionaryResultType};
    for (int index = 0; index < 2; index++) {
        fetchRequest.resultType = resultTypes[index];
        NSError *error;
        NSArray *result = [[self managedObjectContext] executeFetchRequest:fetchRequest error:&error];
        STAssertNotNil(result, @"Could not execute fetch request: %@", error);
        assertionBlock(result, fetchRequest.resultType);
    }
}

- (Entry*) insertEntryWithProperties:(NSDictionary*)props
{
    Entry *entry = [NSEntityDescription insertNewObjectForEntityForName:@"Entry"
                                                 inManagedObjectContext:[self managedObjectContext]];
    [entry setValuesForKeysWithDictionary:props];
    return  entry;
}
- (NSArray*) insertEntriesWithProperties:(NSArray*)entityProps
{
    NSMutableArray *result = [NSMutableArray arrayWithCapacity:entityProps.count];
    for (NSDictionary *props in entityProps) {
        [result addObject:[self insertEntryWithProperties:props]];
    }
    return result;
}

@end
