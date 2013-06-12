//
//  File.h
//  CBCouchbaseIncrementalStore
//
//  Created by Christian Beer on 12.06.13.
//  Copyright (c) 2013 Christian Beer. All rights reserved.
//

#import <Foundation/Foundation.h>
#import <CoreData/CoreData.h>

@class Entry;

@interface File : NSManagedObject

@property (nonatomic, retain) NSString * filename;
@property (nonatomic, retain) NSData * data;
@property (nonatomic, retain) Entry *entry;

@end
