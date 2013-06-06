//
//  Entry.h
//  CBCouchDBIncrementalStore
//
//  Created by Christian Beer on 03.06.13.
//  Copyright (c) 2013 Christian Beer. All rights reserved.
//

#import <Foundation/Foundation.h>
#import <CoreData/CoreData.h>

@class Subentry;

@interface Entry : NSManagedObject

@property (nonatomic, retain) NSNumber * check;
@property (nonatomic, retain) NSDate * created_at;
@property (nonatomic, retain) NSString * text;
@property (nonatomic, retain) NSSet *subentries;
@end

@interface Entry (CoreDataGeneratedAccessors)

- (void)addSubentriesObject:(Subentry *)value;
- (void)removeSubentriesObject:(Subentry *)value;
- (void)addSubentries:(NSSet *)values;
- (void)removeSubentries:(NSSet *)values;

@end
