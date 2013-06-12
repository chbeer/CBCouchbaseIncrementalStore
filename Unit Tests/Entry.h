//
//  Entry.h
//  CBCouchbaseIncrementalStore
//
//  Created by Christian Beer on 12.06.13.
//  Copyright (c) 2013 Christian Beer. All rights reserved.
//

#import <Foundation/Foundation.h>
#import <CoreData/CoreData.h>

@class File, Subentry;

@interface Entry : NSManagedObject

@property (nonatomic, retain) NSNumber * check;
@property (nonatomic, retain) NSDate * created_at;
@property (nonatomic, retain) NSString * text;
@property (nonatomic, retain) NSString * text2;
@property (nonatomic, retain) NSNumber * number;
@property (nonatomic, retain) NSDecimalNumber * decimalNumber;
@property (nonatomic, retain) NSNumber * doubleNumber;
@property (nonatomic, retain) NSSet *subentries;
@property (nonatomic, retain) NSSet *files;
@end

@interface Entry (CoreDataGeneratedAccessors)

- (void)addSubentriesObject:(Subentry *)value;
- (void)removeSubentriesObject:(Subentry *)value;
- (void)addSubentries:(NSSet *)values;
- (void)removeSubentries:(NSSet *)values;

- (void)addFilesObject:(File *)value;
- (void)removeFilesObject:(File *)value;
- (void)addFiles:(NSSet *)values;
- (void)removeFiles:(NSSet *)values;

@end
