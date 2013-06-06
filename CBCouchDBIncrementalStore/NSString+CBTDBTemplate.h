//
//  NSString+CBTDBTemplate.h
//  CBCouchDBIncrementalStore
//
//  Created by Christian Beer on 16.06.10.
//  Copyright 2010 kaufDA - Juno Internet GmbH . All rights reserved.
//

#import <Foundation/Foundation.h>


@interface NSString (CBTDBTemplate)

+ (NSString*) stringWithTemplate:(NSString*)template values:(NSDictionary*)dict;
- (NSString*) stringAsTemplateWithValues:(NSDictionary*)dict;

@end
