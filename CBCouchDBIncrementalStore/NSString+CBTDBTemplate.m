//
//  NSString+CBTDBTemplate.m
//  CBCouchDBIncrementalStore
//
//  Created by Christian Beer on 16.06.10.
//  Copyright 2010 kaufDA - Juno Internet GmbH . All rights reserved.
//

#import "NSString+CBTDBTemplate.h"

@implementation NSString (CBTDBTemplate)

+ (NSString*) stringWithTemplate:(NSString*)template values:(NSDictionary*)dict {
    NSMutableString *result = [NSMutableString stringWithString:template];

    for (NSString *key in [dict keyEnumerator]) {
        NSString *target = [NSString stringWithFormat:@"${%@}", key];
        [result replaceOccurrencesOfString:target
                                withString:[dict objectForKey:key]
                                   options:0 
                                     range:NSMakeRange(0, [result length])];
    }
    
    return result;
}

- (NSString*) stringAsTemplateWithValues:(NSDictionary*)dict {
    NSMutableString *result = [NSMutableString stringWithString:self];
    
    for (NSString *key in [dict keyEnumerator]) {
        NSString *target = [NSString stringWithFormat:@"${%@}", key];
        [result replaceOccurrencesOfString:target
                                withString:[dict objectForKey:key]
                                   options:0 
                                     range:NSMakeRange(0, [result length])];
    }
    
    return result;
}

@end
