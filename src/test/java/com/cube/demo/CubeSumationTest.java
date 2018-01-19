package com.cube.demo;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import org.junit.BeforeClass;
import org.junit.Test;

import com.amazonaws.services.lambda.runtime.Context;

/**
 * A simple test harness for locally invoking your Lambda function handler.
 */
public class CubeSumationTest {

	private static InputStream matrixCreationRequest;
	private static ByteArrayOutputStream matrixCreationResponse;
	
	private static InputStream addItemRequest;
	private static ByteArrayOutputStream addItemResponse;
	
	private static InputStream queryRequest;
	private static ByteArrayOutputStream queryResponse;
	
    @BeforeClass
    public static void createInput() throws IOException {
        // Creation of input streams
    		
    	 matrixCreationRequest = new ByteArrayInputStream(
                 "{\"queryStringParameters\":{\"operation\":createMatrix,\"N\":4,\"matrixName\":hgt23}}".getBytes());
    	 matrixCreationResponse = new ByteArrayOutputStream();
    	 
    	 addItemRequest = new ByteArrayInputStream(
                 "{\"queryStringParameters\":{\"operation\":addItem,\"pos\":112,\"value\":4,\"matrixName\":hgt23}}".getBytes());
    	 addItemResponse = new ByteArrayOutputStream();
    	 
    	 queryRequest = new ByteArrayInputStream(
    			 "{\"queryStringParameters\":{\"operation\":query,\"pos1\":111,\"pos2\":333,\"matrixName\":hgt23}}".getBytes());
    	 queryResponse = new ByteArrayOutputStream();
    }

    private Context createContext() {
        TestContext ctx = new TestContext();

        // TODO: customize your context here if needed.
        ctx.setFunctionName("Your Function Name");

        return ctx;
    }

    @Test
    public void testCubeSumation() throws IOException {
        CubeSumation cube = new CubeSumation();
        Context ctx = createContext();

        cube.handleRequest(matrixCreationRequest, matrixCreationResponse, ctx);
        cube.handleRequest(addItemRequest, addItemResponse, ctx);
        cube.handleRequest(queryRequest, queryResponse, ctx);
        
        byte[] byteArray = queryResponse.toByteArray();

        assertEquals(
                "{\"headers\":{\"Content-Type\":\"application/json\"},\"body\":\"{\\\"result\\\":4}\",\"statusCode\":\"200\"}",
                new String(byteArray)
            );
    }
}
