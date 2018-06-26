package com.softwareplumbers.common.mongo;

import java.math.BigDecimal;
import java.util.stream.Stream;

import org.bson.Document;
import org.bson.conversions.Bson;
import static com.mongodb.client.model.Filters.*;

import com.mongodb.MongoClientSettings;
import com.softwareplumbers.common.abstractquery.Formatter;
import com.softwareplumbers.common.abstractquery.Value;
import com.softwareplumbers.common.abstractquery.Formatter.Context;

public class MongoFormatter implements Formatter<Bson> {
	
	
	static String printDimension(Context context) {
		if (context.type == Context.Type.ROOT) return "";
		if (context.type == Context.Type.ARRAY) return "$self";
		if (context.type == Context.Type.OBJECT) return printDimension(context.parent);
		// else context.type == field
		if (context.parent.type == Context.Type.OBJECT && (
				context.parent.parent.type == Context.Type.ARRAY || context.parent.parent.type == Context.Type.ROOT))
			return context.dimension;
		if (context.parent.type == Context.Type.ROOT) 
			return context.dimension; 
		return printDimension(context.parent) + "." + context.dimension;
	}
	
	@Override
	public Bson andExpr(Context context, Value.Type type, Stream<Bson> ands) {
		return and(ands.toArray(Bson[]::new));
	}

	@Override
	public Bson operExpr(Context context, String operator, Value value) {
		Object operand = ((Value.Atomic)value).value;
		
		if (value.type == Value.Type.NUMBER) {
			BigDecimal bdo = (BigDecimal)operand;
			if (bdo.scale() == 0)
				operand = Integer.valueOf(bdo.intValueExact());
		}
		
		if (context.type == Context.Type.ARRAY) {
			if (operator.equals("=")) return new Document("$eq", operand );
			if (operator.equals("<")) return new Document("$lt", operand );
			if (operator.equals(">")) return new Document("$gt", operand );
			if (operator.equals("<=")) return new Document("$lte", operand );
			if (operator.equals(">=")) return new Document("$gte", operand );;			
		} else {
			if (operator.equals("=")) return eq(printDimension(context), operand );
			if (operator.equals("<")) return lt(printDimension(context), operand);
			if (operator.equals(">")) return gt(printDimension(context), operand);
			if (operator.equals("<=")) return lte(printDimension(context), operand);
			if (operator.equals(">=")) return gte(printDimension(context), operand);
		}
		throw new RuntimeException("Unhandled operator;" + operator);
	}

	@Override
	public Bson orExpr(Context context, Value.Type type, Stream<Bson> ors) {
		return or(ors.toArray(Bson[]::new));
	}

	@Override
	public Bson subExpr(Context context, String operator, Bson matches) {
		if (operator.equals("has")) {
			return elemMatch(printDimension(context), matches.toBsonDocument(null, MongoClientSettings.getDefaultCodecRegistry()));
		}
		throw new RuntimeException("Unhandled operator;" + operator);		
	}
	
	public static final Formatter<Bson> MONGO_FORMAT = new MongoFormatter();
	
}