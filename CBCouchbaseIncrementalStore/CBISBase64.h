//
//  CBISBase64.h
//  CBCouchDBIncrementalStore
//
//  Created by Christian Beer on 29.03.12.
//  Copyright (c) 2012 Christian Beer. All rights reserved.
//

#import <Foundation/Foundation.h>

NSString* CBISBase64EncodedStringFromData(NSData *data);
NSData* CBISDataFromBase64EncodedString(NSString *string);

