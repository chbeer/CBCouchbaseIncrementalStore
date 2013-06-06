//
//  Subentry.h
//  CBCouchDBIncrementalStore
//
//  Created by Christian Beer on 03.06.13.
//  Copyright (c) 2013 Christian Beer. All rights reserved.
//

#import <Foundation/Foundation.h>
#import <CoreData/CoreData.h>

@class Entry;

@interface Subentry : NSManagedObject

@property (nonatomic, retain) NSString * text;
@property (nonatomic, retain) NSNumber * number;
@property (nonatomic, retain) Entry *entry;

@end
