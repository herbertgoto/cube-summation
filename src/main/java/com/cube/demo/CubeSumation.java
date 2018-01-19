package com.cube.demo;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.ThreadLocalRandom;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.UpdateItemOutcome;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;

import org.json.JSONObject;
import org.json.JSONTokener;
import org.json.JSONException;

public class CubeSumation implements RequestStreamHandler {

	@Override
	public void handleRequest(InputStream inputStream, OutputStream outputStream, Context context) throws IOException {

		JSONObject requestJson = new JSONObject(new JSONTokener(inputStream));
		JSONObject responseJson = new JSONObject();
		JSONObject responseBody = new JSONObject();

		// Headers
		JSONObject headerJson = new JSONObject();
		headerJson.put("Content-Type", "application/json");

		try {
			JSONObject params = requestJson.getJSONObject("queryStringParameters");

			if (params.getString("operation").equals("createMatrix")) {
				
				//String matrixName = "matrix" + ThreadLocalRandom.current().nextInt();
				//String matrixName = "hgt";			// Used for test purposes
				createMatrix(params.getString("matrixName"));
				populateMatrix(params.getInt("N"), params.getString("matrixName"));
				
				responseBody.put("matrixName", params.getString("matrixName"));
				responseJson.put("statusCode", "200");
			} else if (params.getString("operation").equals("addItem")) {
				updateItem(params.getInt("pos"), params.getInt("value"), params.getString("matrixName"));

				responseBody.put("result", "Item added.");
				responseJson.put("statusCode", "200");
			} else if (params.getString("operation").equals("query")) {
				
				responseBody.put("result", queryMatrix(params.getInt("pos1"), params.getInt("pos2"), params.getString("matrixName")));
				responseJson.put("statusCode", "200");
			}
		} catch (JSONException e) {
			responseBody.put("status", "error");
			responseBody.put("message", "error");
			responseJson.put("statusCode", "400");
		}
		
		// Assemble the response.
		responseJson.put("body", responseBody.toString());
		responseJson.put("headers", headerJson);
		
		OutputStreamWriter writer = new OutputStreamWriter(outputStream, "UTF-8");
		writer.write(responseJson.toString());
		writer.close();

	}

	// Creates dynamoDB data structure to represent nxnxn matrix
	private void createMatrix(String matrixName) {

		
//		AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().withEndpointConfiguration(
		//			new AwsClientBuilder.EndpointConfiguration("http://localhost:8000", "us-east-1")).build();
		
		AmazonDynamoDB client = AmazonDynamoDBClientBuilder.defaultClient();

		DynamoDB dynamoDB = new DynamoDB(client);

		try {
			System.out.println("Attempting to create matrix structure; please wait...");
			Table table = dynamoDB.createTable(matrixName, Arrays.asList(new KeySchemaElement("id", KeyType.HASH), // Partition
																													// key
					new KeySchemaElement("pos", KeyType.RANGE)), // Sort key

					Arrays.asList(new AttributeDefinition("id", ScalarAttributeType.N),
							new AttributeDefinition("pos", ScalarAttributeType.N)),
					new ProvisionedThroughput(10L, 10L));
			table.waitForActive();
			System.out.println("Success.  Matrix structure status: " + table.getDescription().getTableStatus());

		} catch (Exception e) {
			System.err.println("Unable to matrix structure: ");
			System.err.println(e.getMessage());
		}

	}

	// Populates dynamo table with 0 as value and posicion as index
	private void populateMatrix(int n, String matrixName) {

//		AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().withEndpointConfiguration(
		//			new AwsClientBuilder.EndpointConfiguration("http://localhost:8000", "us-east-1")).build();
		
		AmazonDynamoDB client = AmazonDynamoDBClientBuilder.defaultClient();

		DynamoDB dynamoDB = new DynamoDB(client);

		Table table = dynamoDB.getTable(matrixName);

		// int id = 1;

		for (int i = 1; i < n + 1; i++) {
			for (int j = 1; j < n + 1; j++) {
				for (int k = 1; k < n + 1; k++) {

					try {

						int index = Integer.valueOf(String.valueOf(i) + String.valueOf(j) + String.valueOf(k));

						table.putItem(new Item().withPrimaryKey("id", 1, "pos", index).withNumber("val", 0));
						 System.out.println("PutItem succeeded: " + index + " " + 0);
						// id++;

					} catch (Exception e) {
						System.err.println("Unable to add item!");
						System.err.println(e.getMessage());
						break;
					}

				}
			}
		}
	}

	// Upload item
	private void updateItem(int index, int val, String matrixName) {

	//	AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().withEndpointConfiguration(
	//			new AwsClientBuilder.EndpointConfiguration("http://localhost:8000", "us-east-1")).build();

		AmazonDynamoDB client = AmazonDynamoDBClientBuilder.defaultClient();
		
		DynamoDB dynamoDB = new DynamoDB(client);

		Table table = dynamoDB.getTable(matrixName);

		UpdateItemSpec updateItemSpec = new UpdateItemSpec().withPrimaryKey("id", 1, "pos", index)
				.withUpdateExpression("set val = :v").withValueMap(new ValueMap().withNumber(":v", val))
				.withReturnValues(ReturnValue.UPDATED_NEW);

		try {
			// System.out.println("Updating the item...");
			UpdateItemOutcome outcome = table.updateItem(updateItemSpec);
			// System.out.println("UpdateItem succeeded:\n" +
			// outcome.getItem().toJSONPretty());

		} catch (Exception e) {
			System.err.println("Unable to update item!");
			System.err.println(e.getMessage());
		}
	}

	// Query matrix
	private int queryMatrix(int initIndex, int endIndex, String matrixName) {

//		AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().withEndpointConfiguration(
		//			new AwsClientBuilder.EndpointConfiguration("http://localhost:8000", "us-east-1")).build();
		
		AmazonDynamoDB client = AmazonDynamoDBClientBuilder.defaultClient();
		
		DynamoDB dynamoDB = new DynamoDB(client);

		Table table = dynamoDB.getTable(matrixName);

		QuerySpec querySpec = new QuerySpec().withKeyConditionExpression("id = :v and pos between :pos1 and :pos2")
				.withValueMap(new ValueMap().withInt(":v", 1).withInt(":pos1", initIndex).withInt(":pos2", endIndex));

		ItemCollection<QueryOutcome> items = null;
		Iterator<Item> iterator = null;
		Item item = null;

		int sum = 0;

		try {

			items = table.query(querySpec);

			iterator = items.iterator();
			while (iterator.hasNext()) {
				item = iterator.next();

				sum = sum + item.getNumber("val").intValue();

			}	

		} catch (Exception e) {
			System.err.println("Unable to query items!");
			System.err.println(e.getMessage());
		}
		
		return sum;

	}

}
