package telran.aws;

import java.io.*;
import java.util.HashMap;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.spec.PutItemSpec;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;

import telran.aws.sensors.dto.SensorData;

public class ApiSensorHandler implements RequestStreamHandler {
static final String TABLE_NAME = "sensors";
	@Override
	public void handleRequest(InputStream input, OutputStream output, Context context)
			throws IOException {
		LambdaLogger logger = context.getLogger();
		String response = "";
		HashMap<String, Object> responseMap =  new HashMap<>();
		try {
			JSONParser parser = new JSONParser();
			
			BufferedReader reader = new BufferedReader(new InputStreamReader(input));
			JSONObject event = (JSONObject) parser.parse(reader);
			String sensorJson = (String) event.get("body");
			if(sensorJson == null) {
				throw new IllegalArgumentException("request doesn't contain body");
			}
			logger.log("sendorJson is " + sensorJson);
			SensorData sensorData = new SensorData(sensorJson);
			populateSensorData(sensorData);
			response = createResponse(responseMap, String.format("sensor %d, value %f has been saved to Database",
					sensorData.getId(), sensorData.getValue()), 200);
			
		} catch (Exception e) {
			logger.log("exception: " + e.toString());
			response = createResponse(responseMap, e.getMessage(), 400);
			
		} 
		PrintStream writer = new PrintStream(output);
		writer.println(response);
		writer.close();
		

	}

	private void populateSensorData(SensorData sensorData) {
		AmazonDynamoDB client = AmazonDynamoDBClientBuilder.defaultClient();
		DynamoDB dynamoDB = new DynamoDB(client);
		var table = dynamoDB.getTable(TABLE_NAME);
		table.putItem(new PutItemSpec().withItem(new Item()
				.withNumber("id", sensorData.getId())
				.withNumber("threshold_value", sensorData.getValue())));
		
	}

	private String createResponse(HashMap<String, Object> responseMap, String bodyText, int code) {
		responseMap.put("status", code);
		responseMap.put("body", bodyText);
		return JSONObject.toJSONString(responseMap);
	}

}
