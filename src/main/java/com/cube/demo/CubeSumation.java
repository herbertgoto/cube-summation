package com.cube.demo;

import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.UpdateItemOutcome;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

public class CubeSumation implements RequestHandler<Object, String> {

	@Override
	public String handleRequest(Object input, Context context) {

		AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().withEndpointConfiguration(
				new AwsClientBuilder.EndpointConfiguration("http://localhost:8000", "us-east-1")).build();

		DynamoDB dynamoDB = new DynamoDB(client);

		context.getLogger().log("Operation: " + input);

		String matrixName = "matrix" + ThreadLocalRandom.current().nextInt();

		System.out.println(matrixName);

		//if (input.equals("CreateMatrix")) {
			createMatrix(matrixName);
			populateMatrix(2, matrixName);

			Table table = dynamoDB.getTable(matrixName);
			//return "" + table.describe().getItemCount();
		//} else if(input.equals("UploadItem")) {
			updateItem(111, 4, matrixName);
			
			//System.out.println(table.getItem("pos", 111).get("val"));
			
			return "" + table.getItem("pos", 111).get("val");
		//}
		// TODO: implement your handler
		//return "success";
	}

	// Creates dynamoDB data structure to represent nxnxn matrix
	private void createMatrix(String matrixName) {

		AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().withEndpointConfiguration(
				new AwsClientBuilder.EndpointConfiguration("http://localhost:8000", "us-east-1")).build();

		DynamoDB dynamoDB = new DynamoDB(client);

		try {
			System.out.println("Attempting to create matrix structure; please wait...");
			Table table = dynamoDB.createTable(matrixName, Arrays.asList(new KeySchemaElement("pos", KeyType.HASH)), // Partition
																														// key
					Arrays.asList(new AttributeDefinition("pos", ScalarAttributeType.N)),
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

		AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().withEndpointConfiguration(
				new AwsClientBuilder.EndpointConfiguration("http://localhost:8000", "us-east-1")).build();

		DynamoDB dynamoDB = new DynamoDB(client);

		Table table = dynamoDB.getTable(matrixName);

		for (int i = 1; i < n + 1; i++) {
			for (int j = 1; j < n + 1; j++) {
				for (int k = 1; k < n + 1; k++) {

					try {

						int index = Integer.valueOf(String.valueOf(i) + String.valueOf(j) + String.valueOf(k));

						table.putItem(new Item().withPrimaryKey("pos", index).withNumber("val", 0));
						// System.out.println("PutItem succeeded: " + index + " " + 0);

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
	private void updateItem(int pos, int val, String matrixName) {

		AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().withEndpointConfiguration(
				new AwsClientBuilder.EndpointConfiguration("http://localhost:8000", "us-east-1")).build();

		DynamoDB dynamoDB = new DynamoDB(client);

		Table table = dynamoDB.getTable(matrixName);

		UpdateItemSpec updateItemSpec = new UpdateItemSpec().withPrimaryKey("pos", pos)
				.withUpdateExpression("set val = :v")
				.withValueMap(new ValueMap().withNumber(":v", val))
				.withReturnValues(ReturnValue.UPDATED_NEW);

		try {
			//System.out.println("Updating the item...");
			UpdateItemOutcome outcome = table.updateItem(updateItemSpec);
			//System.out.println("UpdateItem succeeded:\n" + outcome.getItem().toJSONPretty());

		} catch (Exception e) {
			System.err.println("Unable to update item!");
			System.err.println(e.getMessage());
		}
	}

}
