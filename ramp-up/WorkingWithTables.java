package com.amazonaws.samples;

import java.util.Iterator;
import java.util.List;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.TableCollection;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.TableDescription;

public class WorkingWithTables {
	public static void main(String[] args) {
		AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().build();
		DynamoDB dynamoDB = new DynamoDB(client);
		TableDescription tableDescription = dynamoDB.getTable("ProductCatalog").describe();
		
		// Print table details
		System.out.printf("%s: %s \t ReadCapacityUnits: %d \t WriteCapacityUnits: %d\n",
			    tableDescription.getTableStatus(),
			    tableDescription.getTableName(),
			    tableDescription.getProvisionedThroughput().getReadCapacityUnits(),
			    tableDescription.getProvisionedThroughput().getWriteCapacityUnits()); 
		/*
		List<KeySchemaElement> keySchema = tableDescription.getKeySchema();
		for (KeySchemaElement k: keySchema) {
			System.out.println(keySchema.size());
			System.out.println(k.getAttributeName());
		} */
		
		// Listing Tables
		TableCollection<ListTablesResult> tables = dynamoDB.listTables();
		Iterator<Table> iterator = tables.iterator();
		
		while (iterator.hasNext()) {
			Table table = iterator.next();
			System.out.println(table.getTableName());
		}
		
		// Writing Item to Table
		Table table = dynamoDB.getTable("phil-test");
		String breed = "dachshund";
		String name = "Frank";
		String age = "11.4";
		
		try {
			System.out.println("Adding a new item..");
			PutItemOutcome outcome = table
					.putItem(new Item().withPrimaryKey("breed", breed, "name", name).withString("age", age));
			System.out.println("PutItem succeeded:\n" + outcome.getPutItemResult());
		}
		catch (Exception e) {
			System.err.println("Unable to add item: " + breed + " " + name);
			System.out.println(e.getMessage());
		}
		
	}
}
