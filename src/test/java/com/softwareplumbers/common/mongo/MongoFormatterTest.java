package com.softwareplumbers.common.mongo;


import com.mongodb.MongoClientSettings;
import com.softwareplumbers.common.abstractquery.Cube;
import static com.softwareplumbers.common.mongo.MongoFormatter.MONGO_FORMAT;
import static com.mongodb.client.model.Filters.*;

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

import org.bson.Document;
import org.bson.conversions.Bson;

public class MongoFormatterTest {
	
	public static String pretty(Bson data) {
		return data.toBsonDocument(null, MongoClientSettings.getDefaultCodecRegistry()).toString();
	}
	
	public static void assertEqualsBson(Bson a, Bson b) {
		String as = a.toBsonDocument(null, MongoClientSettings.getDefaultCodecRegistry()).toString();
		String bs = b.toBsonDocument(null, MongoClientSettings.getDefaultCodecRegistry()).toString();
		assertEquals(as,bs);
	}
	
	@Test
	public void formatsASimpleQuery() {
    	Bson query = Cube.fromJson("{ 'a': '23', 'b': [2,4]}").toExpression(MONGO_FORMAT);    	
    	assertEqualsBson(and(eq("a","23"), and(gte("b",2), lt("b",4))), query);
	}
	
	@Test
	public void formatsAnotherSimpleQuery() { 
        Bson query = Cube.fromJson("{ 'a': '23', 'b': [2,null]}").toExpression(MONGO_FORMAT);
        assertEqualsBson(query, and(eq("a","23"), gte("b",2)));
    }

    @Test public void formatsAnOrQuery() { 
    	Bson query = Cube
    		.fromJson("{ 'a': '23', 'b': [2,4]}")
    		.union("{ 'a': '24', 'b': [1,16]}")
    		.toExpression(MONGO_FORMAT);

    	assertEqualsBson( 
    		or(
    			and(
    				eq("a","23"),
    				and(gte("b",2), lt("b",4))
    			),
    			and(
    				eq("a","24"),
    				and(gte("b",1), lt("b",16))
    			)
    		),
    		query
    	);
    }

    @Test public void formatsASubquery() { 
    	Bson query = Cube
    		.fromJson("{ 'a': '23', 'b': { 'c': 4, 'd': 2}}")
    		.toExpression(MONGO_FORMAT);

    	assertEqualsBson(query,
    			and(
    				eq("a","23"),
    				and(
    					eq("b.c",4),
    					eq("b.d",2)
    				)
    			)
    		);
    }

    @Test public void formatsAQueryOnAnArrayElement() { 

    	Bson query = Cube
    		.fromJson("{ 'a': '23', 'b': { '$has' : 'bongo' } }")
    		.toExpression(MONGO_FORMAT);

    	Bson expected = and(
    					eq("a","23"),
    					elemMatch("b", Document.parse("{ $eq: 'bongo'}"))
    				);
    	
    	assertEqualsBson(expected, query);
    }
/*
   @Test public void formatsAQueryOnMultipleArraElements() { 
 

    	Bson query = Cube
    		.fromJson("{ 'a': '23', 'b': { '$has' : ['bongo', 'bingo'] } }") // TODO: was hasAll...
    		.toExpression(MONGO_FORMAT);

    	assertEqualsBson(
    			and(eq("a",23), 
    				and(
    					elemMatch("b", Document.parse("{ $eq: 'bongo'}")),
    					elemMatch("b", Document.parse("{ $eq: 'bingo'}"))
    				)
    			),
    			query);
    }
*/
    @Test public void formatsAQueryOnAnArrayOfDocuments() { 

    	Cube query = Cube.fromJson("{ 'a': '23', 'b': { '$has' : { 'drumkit': 'bongo' } } }");
    	

    	assertEqualsBson(
    			and(
    				eq("a","23"), 
    				elemMatch("b", eq("drumkit", "bongo"))
    			),
    			query.toExpression(MONGO_FORMAT)
    		); 
    }

    /*
    it ('formats example for README.md() {
        Bson query = Cube
            .fromJson("{ 'a': '23', 'b': [2,4]}")
            .or("{ 'a': '24', 'b': [1,16]}")
            .toExpression(MONGO_FORMAT);

        let result = 
            {'$or':[
                {'$and':[
                    {'a':'23'},
                    {'$and':[
                        {'b':{'$gte':2}},
                        {'b':{'$lt':4}}
                    ]}
                ]},
                {'$and':[
                    {'a':'24'},
                    {'$and':[
                        {'b':{'$gte':1}},
                        {'b':{'$lt':16}}
                    ]}
                ]}
            ]};

        assertEquals(query.toString(),result);

    }	
*/
}
