package com.dataaggregation.service;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import org.springframework.web.client.RestTemplate;

import com.dataaggregation.config.DataAggregatorConfig;
import com.dataaggregation.utility.ErrorUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.jayway.jsonpath.JsonPath;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlobContainerPublicAccessType;
import com.microsoft.azure.storage.blob.BlobRequestOptions;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.table.CloudTable;
import com.microsoft.azure.storage.table.CloudTableClient;
import com.microsoft.azure.storage.table.TableQuery;
import com.microsoft.azure.storage.table.TableQuery.Operators;
import com.microsoft.azure.storage.table.TableQuery.QueryComparisons;
import com.microsoft.windowsazure.Configuration;
import com.microsoft.windowsazure.exception.ServiceException;
import com.microsoft.windowsazure.services.servicebus.ServiceBusConfiguration;
import com.microsoft.windowsazure.services.servicebus.ServiceBusContract;
import com.microsoft.windowsazure.services.servicebus.ServiceBusService;
import com.microsoft.windowsazure.services.servicebus.implementation.BrokerProperties;
import com.microsoft.windowsazure.services.servicebus.models.BrokeredMessage;
import com.microsoft.windowsazure.services.servicebus.models.ReceiveMessageOptions;
import com.microsoft.windowsazure.services.servicebus.models.ReceiveMode;
import com.microsoft.windowsazure.services.servicebus.models.ReceiveQueueMessageResult;

/**
 * @author ShaishavS
 * 
 *         Service class to fetch data from service bus queue, process on the
 *         data, get epic data, do transformation, store data in storage and
 *         push data in a queue
 */

@Service
@PropertySource(value = { "classpath:constant.properties" })
public class DataAggregationServiceImpl implements DataAggregationService {

	@Value("${error.serviceBus.ioError}")
	private String ioError;
	@Value("${error.serviceBus.pollingMsg}")
	private String pollingMsgError;
	@Value("${error.keyVaultError}")
	private String keyVaultError;
	@Value("${error.epicError}")
	private String epicError;
	@Value("${error.jsonError}")
	private String jsonError;
	@Value("${error.invalidStorageKey}")
	private String invalidStorageKey;
	@Value("${error.uriSyntaxError}")
	private String uriSyntaxError;
	@Value("${error.storageError}")
	private String storageError;
	@Value("${error.serviceBus.exception}")
	private String exceptionError;
	@Value("${error.serviceBus.emptyMsg}")
	private String emptyMsgError;
	@Value("${error.serviceBus.malformedModelJson}")
	private String malformedModelJsonError;
	@Value("${error.serviceBus.modelDetails}")
	private String modelDetailsError;
	@Value("${error.serviceBus.tableURI}")
	private String tableURIError;
	@Value("${error.serviceBus.accountKey}")
	private String accountKeyError;
	@Value("${error.serviceBus.tableStorage}")
	private String tableStorageError;
	@Value("${error.serviceBus.tablejsonObject}")
	private String tablejsonObjectError;
	@Value("${error.serviceBus.malformedTableJson}")
	private String malformedTableJson;
	@Value("${error.serviceBus.pushingMsg}")
	private String pushingMsgError;


	JSONArray logArray = new JSONArray();

	@Autowired
	private ErrorUtil errorUtil;
	private String isDebug = System.getenv("ISDEBUG");
	private String nameSpace = System.getenv("SERVICE_BUS_NAMESPACE");
	private String sasKeyName = System.getenv("SERVICE_BUS_SAS_KEY_NAME");
	private String sasKeyKeyVault = System.getenv("SERVICE_BUS_SAS_KEY_VALUE_KEY_VAULT");
	private String serviceBusRootUri = System.getenv("SERVICE_BUS_SERVICE_BUS_ROOT_URI");
	private String scoreRetrieverQueueName = System.getenv("SERVICE_BUS_SCORE_RETRIEVER_QUEUE_NAME");
	private String dataAggrgationQueueName = System.getenv("SERVICE_BUS_DATAAGGREGATOR_QUEUE_NAME");
	private String storageConnectionString = System.getenv("CONNECTION_STRING_KEY_VAULT");
	private String epicUrl = System.getenv("EPIC_URL");
	private String loggingURL = System.getenv("LOGGING_URL");
	private String containerName = System.getenv("CONTAINER_NAME");
	private String tableName = System.getenv("TABLE_NAME");
	private String timeZone = System.getenv("TIMEZONE");
	private String KEYVAULT_SERVICE_ENDPOINT = System.getenv("KEYVAULT_SERVICE_ENDPOINT");
	private String STORAGE_ACCOUNT_ACCESS_KEY_NAME =  System.getenv("STORAGE_ACCOUNT_ACCESS_KEY_NAME");

	

	
	@Override
	public void pollMessageFromQueue() {
		try {

			storageConnectionString = getSecretFromKeyVault(storageConnectionString);
			sasKeyKeyVault = getSecretFromKeyVault(sasKeyKeyVault);

			Configuration config = ServiceBusConfiguration.configureWithSASAuthentication(nameSpace, sasKeyName,
					sasKeyKeyVault, serviceBusRootUri);
			ServiceBusContract serviceBusContract = ServiceBusService.create(config);

			ReceiveMessageOptions receiveMessageOptions = ReceiveMessageOptions.DEFAULT;
			receiveMessageOptions.setReceiveMode(ReceiveMode.PEEK_LOCK);
			while (true) {
				ReceiveQueueMessageResult receiveQueueMessageResult = serviceBusContract
						.receiveQueueMessage(dataAggrgationQueueName, receiveMessageOptions);
				BrokeredMessage brokeredMessage = receiveQueueMessageResult.getValue();
				if (brokeredMessage != null && brokeredMessage.getMessageId() != null) {

					BrokerProperties brokerProperties = brokeredMessage.getBrokerProperties();
					String messageID = brokerProperties.getMessageId();
					appendLog("pollMessageFromQueue", "Info", "MessageID: " + messageID);
					pushLogIntoLoginservice();
					try {
						byte[] byteArr = new byte[2000];
						StringBuilder queueMsg1 = new StringBuilder();
						int numRead = brokeredMessage.getBody().read(byteArr);
						while (-1 != numRead) {
							String queueMsg = new String(byteArr);
							queueMsg = queueMsg.trim();
							queueMsg1 = queueMsg1.append(queueMsg);
							numRead = brokeredMessage.getBody().read(byteArr);
						}

						// Remove message from queue.
						int i = queueMsg1.toString().indexOf('{');
						if (isDebug.equalsIgnoreCase("true")) {
							callEpicService(
									"{ \"ModelName\": \"Trauma\", \"BatchDateTime\": \"2018-06-21T04:25:00-0500\", \"PatientID\": \"E3278\", \"API\": [{ \"Data-aggregator-stage\": [{ \"MethodType\": \"GET\", \"Type\": \"RESTful\", \"Parameters\": [\"PatientID\", \"PatientClass\", \"From\", \"To\", \"UserID\"], \"ParametersValue\": [\"Z4495\", \"l\", \"2018-06-18\", \"2018-06-19\", \"1\"], \"URL\": \"https://apporchard.epic.com/interconnect-ao85prd-username/api/epic/2011/Clinical/Patient/GETPATIENTCONTACTS/Patient/Contacts\", \"Name\": \"GetPatientContacts(2011)\" }, { \"MethodType\": \"POST\", \"Type\": \"RESTful\", \"Parameters\": [\"PatientID\", \"UserID\", \"ContactID\"], \"ParametersValue\": [{ \"Type\": \"Internal\", \"ID\": \"Z4495\" }, { \"Type\": \"Internal\", \"ID\": \"1\" }, { \"Type\": \"CSN\", \"ID\": \"1855\" }], \"URL\": \"https://apporchard.epic.com/interconnect-ao85prd-username/api/epic/2011/Clinical/Patient/GETACTIVEPROBLEMS/ActiveProblems?ExcludeNonHospitalProblems={ExcludeNonHospitalProblems}\", \"Name\": \"GetActiveProblems\" }, { \"MethodType\": \"POST\", \"Type\": \"RESTful\", \"Parameters\": [\"PatientID\", \"ComponentTypes\", \"UserID\"], \"ParametersValue\": [\"Z4495\", [{ \"Value\": \"1802008\", \"Type\": \"\" }], \"1\"], \"URL\": \"https://apporchard.epic.com/interconnect-ao85prd-username/urn:Epic-com:Results.2014.Services.Utility.GetPatientResultComponents\", \"Name\": \"GetPatientResultComponents\" }, { \"MethodType\": \"POST\", \"Type\": \"RESTful\", \"Parameters\": [\"PatientID\", \"ProfileView\"], \"ParametersValue\": [\"Z4495\", \"2\"], \"URL\": \"https://apporchard.epic.com/interconnect-ao85prd-username/api/epic/2013/Clinical/Utility/GetCurrentMedications/CurrentMedications\", \"Name\": \"GetCurrentMedications\" }, { \"MethodType\": \"POST\", \"Type\": \"RESTful\", \"Parameters\": [\"PatientID\", \"PatientIDType\", \"ContactID\", \"FlowsheetRowIDs\"], \"ParametersValue\": [\"E2731\", \"EPI\", \"58706\", [{ \"Type\": \"EXTERNAL\", \"ID\": \"1400000032\" }]], \"URL\": \"https://apporchard.epic.com/interconnect-ao85prd-username/api/epic/2014/Clinical/Patient/GETFLOWSHEETROWS/FlowsheetRows\", \"Name\": \"GetFlowsheetRows\" }] }, { \"Score-retriever-stage\": [{ \"MethodType\": \"POST\", \"Type\": \"RESTful\", \"Parameters\": [\"PatientID\", \"PatientIDType\", \"ContactID\", \"ContactIDType\", \"UserID\", \"UserIDType\", \"FlowsheetID\", \"FlowsheetIDType\", \"Value\", \"InstantValueTaken\", \"FlowsheetTemplateID\", \"FlowsheetTemplateIDType\"], \"ParametersValue\": [\"E2734\", \"EPI\", \"1852\", \"CSN\", \"1\", \"EXTERNAL\", \"10\", \"INTERNAL\", \"2.63\", \"2018-06-12T23:12:00Z\", \"81\", \"INTERNAL\"], \"URL\": \"https://apporchard.epic.com/interconnect-ao85prd-username/api/epic/2011/Clinical/Patient/ADDFLOWSHEETVALUE/FlowsheetValue\", \"Name\": \"AddFlowsheetValue\" }] }], \"ModelVersion\": \"v1\", \"PatientAge\": \"67 y.o.\" }");
							System.out.println("one message debugged...");
							// System.exit(0);
						} else {
							callEpicService(queueMsg1.toString().substring(i)); //
						}
						serviceBusContract.deleteMessage(brokeredMessage); //
					} catch (ServiceException serviceException) {
						String errorResponse = errorUtil
								.getDataAggregatorError("Error in polling message from service bus queue : "
										+ serviceException.getErrorMessage(), pollingMsgError);
						appendLog("pollMessageFromQueue", "Exception", errorResponse);
						pushLogIntoLoginservice();
						serviceBusContract.unlockMessage(brokeredMessage);
						System.exit(0);
					} catch (IOException ioException) {
						String errorResponse = errorUtil.getDataAggregatorError("IO Error: " + ioException.getMessage(),
								ioError);
						appendLog("pollMessageFromQueue", "Exception", errorResponse);
						pushLogIntoLoginservice();
						serviceBusContract.unlockMessage(brokeredMessage);
						System.exit(0);
					}
				} else { // TODO: retry mechanism

					System.out.println("Finishing up - no more messages.");
					System.exit(0);
				}
			}

		} catch (ServiceException serviceException) {
			String errorResponse = errorUtil.getDataAggregatorError(
					"Error in polling message from service bus queue : " + serviceException.getErrorMessage(),
					pollingMsgError);
			appendLog("pollMessageFromQueue", "Exception", errorResponse);
			pushLogIntoLoginservice();
			System.exit(0);
		} catch (Exception exception) {
			String errorResponse = errorUtil.getDataAggregatorError(
					"Error in polling message from service bus queue : " + exception.getMessage(), exceptionError);
			appendLog("pollMessageFromQueue", "Exception", errorResponse);
			pushLogIntoLoginservice();
			System.exit(0);
		}
		System.out.println("Exit pollMessageFromQueue:");
		System.exit(0);
	}

	private void callEpicService(String queuMessage) {
		JSONArray jsonArray;
		JSONObject queueMsgJson;
		try {
			System.out.println("Inside call epic service##############" + queuMessage);

			queueMsgJson = new JSONObject(queuMessage);
			String modelName = queueMsgJson.getString("ModelName");
			String patientID = queueMsgJson.getString("PatientID");
			String batchDateTime = queueMsgJson.getString("BatchDateTime");

			jsonArray = queueMsgJson.getJSONArray("API");
			JSONObject apiObject = jsonArray.getJSONObject(0);
			JSONArray dataAggregatorStageArray = apiObject.getJSONArray("Data-aggregator-stage");
			if (isDebug.equalsIgnoreCase("false")) {
				for (int i = 0; i < dataAggregatorStageArray.length(); i++) {
					String response = fetchEpicData(dataAggregatorStageArray.get(i).toString());
					if (response != null) {
						JSONObject epicResponseJson = new JSONObject(response);
						String requestStatus = epicResponseJson.getString("status");

						if (requestStatus.equals("success")) {
							JSONObject jsonObject = (JSONObject) dataAggregatorStageArray.get(i);
							uploadFileToStorageeBlob(epicResponseJson.getString("response"), patientID, batchDateTime,
									"Original", jsonObject.getString("Name"));
							getDataFromDataStore(queuMessage, patientID, batchDateTime, modelName);
						}

						else {
							String errorResponse = errorUtil.getDataAggregatorError(response, epicError);
							appendLog("callEpicService", "Error", errorResponse);
							pushLogIntoLoginservice();
							System.exit(0);
						}
					}

				}
			} else {
				getDataFromDataStore(queuMessage, patientID, batchDateTime, modelName);
			}
		} catch (JSONException jsonException) {
			String errorResponse = errorUtil.getDataAggregatorError("JSON Error: " + jsonException.getMessage(),
					jsonError);
			appendLog("callEpicService", "Exception", errorResponse);
			pushLogIntoLoginservice();
			System.exit(0);
		} catch (Exception e) {
			String errorResponse = errorUtil.getDataAggregatorError("Exception: " + e.getMessage(), exceptionError);
			appendLog("callEpicService", "Exception", errorResponse);
			pushLogIntoLoginservice();
			System.exit(0);
		}
	}

	private String uploadFileToStorageeBlob(String epicResponseJson, String patientId, String batch, String original,
			String name) {
		System.out.println("inside upload file::::::::::::");
		try {
			CloudStorageAccount storageAccount = CloudStorageAccount.parse(storageConnectionString);

			CloudBlobClient blobClient = storageAccount.createCloudBlobClient();
			CloudBlobContainer container = blobClient.getContainerReference(containerName);
			// Create the container if it does not exist with public access.
			container.createIfNotExists(BlobContainerPublicAccessType.CONTAINER, new BlobRequestOptions(),
					new OperationContext());
			// Get a blob reference for a text file.
			CloudBlockBlob blob = container.getBlockBlobReference(
					"Patients/" + patientId + "/" + batch + "/Patient-Records/" + original + "/" + name);
			// Upload some text into the blob.
			blob.uploadText(epicResponseJson);
		} catch (InvalidKeyException invalidKeyException) {
			String errorResponse = errorUtil.getDataAggregatorError(
					"Please provide valid AccountKey to connect to azure storage : " + invalidKeyException.getMessage(),
					invalidStorageKey);
			appendLog("uploadFileToStorageeBlob", "Exception", errorResponse);
			pushLogIntoLoginservice();
			System.exit(0);
		} catch (URISyntaxException uriSyntaxException) {
			String errorResponse = errorUtil.getDataAggregatorError(
					"Please provide valid URI to connect to azure storage : " + uriSyntaxException.getMessage(),
					uriSyntaxError);
			appendLog("uploadFileToStorageeBlob", "Exception", errorResponse);
			pushLogIntoLoginservice();
			System.exit(0);
		} catch (StorageException e) {
			String errorResponse = errorUtil.getDataAggregatorError("Storage error: " + e.getMessage(), storageError);
			appendLog("uploadFileToStorageeBlob", "Exception", errorResponse);
			pushLogIntoLoginservice();
			System.exit(0);
		} catch (IOException ioException) {
			String errorResponse = errorUtil.getDataAggregatorError("IO Error: " + ioException.getMessage(), ioError);
			appendLog("pollMessageFromQueue", "Exception", errorResponse);
			pushLogIntoLoginservice();
			System.exit(0);
		}
		return "success";
	}

	public String fetchEpicData(String inputParamJson) {

		String response = " ";
		try {
			JSONArray jsonArray = new JSONArray();
			jsonArray.put(new JSONObject(inputParamJson));
			JSONObject jsonObject = new JSONObject();
			jsonObject.put("API", jsonArray);

			RestTemplate restTemplate = new RestTemplate();
			HttpHeaders headers = new HttpHeaders();
			headers.setContentType(MediaType.APPLICATION_JSON);
			HttpEntity<String> entity = new HttpEntity<String>(jsonObject.toString(), headers);
			response = restTemplate.postForObject(epicUrl, entity, String.class);
			System.out.println("epic response" + response);
		} catch (Exception e) {
			String errorResponse = errorUtil.getDataAggregatorError("Epic Exception: " + e.getMessage(),
					exceptionError);
			appendLog("fetchEpicData", "Exception", errorResponse);
			pushLogIntoLoginservice();
			System.exit(0);
		}
		return response;
	}

	/**
	 * This method will fetch the Epic App Orchard authorization secret from
	 * azure key vault
	 * 
	 * @return authSecret
	 */
	public String getSecretFromKeyVault(String key) {
		String authSecret = "";
		try {
			RestTemplate restTemplate = new RestTemplate();
			authSecret = restTemplate.getForObject(KEYVAULT_SERVICE_ENDPOINT + key, String.class);
			JSONObject jsonObject = new JSONObject(authSecret);
			String status = jsonObject.getString("status");
			if("success".equals(status))
			{
				authSecret = jsonObject.getString("response");
			}
			else
			{
				String errorResponse = errorUtil.getDataAggregatorError("Error in  get secret from key vault : "+authSecret, keyVaultError);
				appendLog("getSecretFromKeyVault", "Exception", errorResponse);
				pushLogIntoLoginservice();
			}
		} catch (JSONException jsonException) {
			String errorResponse = errorUtil.getDataAggregatorError("JSON Error: " + jsonException.getMessage(),
					jsonError);
			appendLog("getSecretFromKeyVault", "Exception", errorResponse);
			pushLogIntoLoginservice();
			System.exit(0);
		} catch (Exception e) {
			String errorResponse = errorUtil.getDataAggregatorError("KeyVault Exception: " + e.getMessage(),
					exceptionError);
			appendLog("getSecretFromKeyVault", "Exception", errorResponse);
			pushLogIntoLoginservice();
			System.exit(0);
		}
		return authSecret;
	}

	public String pushLogIntoLoginservice() {
		String response = " ";
		JSONObject logJsonObject = new JSONObject();
		try {
			logJsonObject.put("Logs", logArray);
			RestTemplate restTemplate = new RestTemplate();
			HttpHeaders headers = new HttpHeaders();
			headers.setContentType(MediaType.APPLICATION_JSON);
			HttpEntity<String> entity = new HttpEntity<String>(logJsonObject.toString(), headers);
			response = restTemplate.postForObject(loggingURL, entity, String.class);
			logArray = new JSONArray();
		} catch (JSONException jsonException) {

		}
		return response;
	}

	public void appendLog(String methodName, String type, String message) {
		JSONObject logJsonObject = new JSONObject();
		try {
			logJsonObject.put("Microservice", "Data-Aggregator-service");
			logJsonObject.put("Class", "DataAggregationServiceImpl");
			logJsonObject.put("Method", methodName);
			logJsonObject.put("Status", type);
			logJsonObject.put("Message", message);
			logJsonObject.put("DateTime", new Date());
			logArray.put(logJsonObject);
		} catch (JSONException e) {
		}
	}

	private JSONObject downloadFileFromStorageeBlob(String patientId, String batch, String original, String fileName) {
		System.out.println("inside download file::::::::::::");
		try {
			CloudStorageAccount storageAccount = CloudStorageAccount.parse(storageConnectionString);

			CloudBlobClient blobClient = storageAccount.createCloudBlobClient();
			CloudBlobContainer container = blobClient.getContainerReference(containerName);
			// TODO: replace hard coded value with real time data
			CloudBlockBlob blob = container.getBlockBlobReference(
					// "Patients/E3278/2018-06-21T04:25:00-0500/Patient-Records/Original/"
					// + fileName);
					"Patients - old/" + patientId + "/" + batch + "/Patient-Records/" + original + "/" + fileName);
			try {
				String test = blob.downloadText();
				JSONObject json = new JSONObject(test);
				return (json);
			} catch (JSONException e) {
				String errorResponse = errorUtil.getDataAggregatorError("JSON Error: " + e.getMessage(), jsonError);
				appendLog("downloadFileFromStorageeBlob", "Exception", errorResponse);
				pushLogIntoLoginservice();
				System.exit(0);
			} // responseData.put(test);

		} catch (InvalidKeyException invalidKeyException) {
			String errorResponse = errorUtil.getDataAggregatorError(
					"Please provide valid AccountKey to connect to azure storage : " + invalidKeyException.getMessage(),
					invalidStorageKey);
			appendLog("uploadFileToStorageeBlob", "Exception", errorResponse);
			pushLogIntoLoginservice();
			System.exit(0);
		} catch (URISyntaxException uriSyntaxException) {
			String errorResponse = errorUtil.getDataAggregatorError(
					"Please provide valid URI to connect to azure storage : " + uriSyntaxException.getMessage(),
					uriSyntaxError);
			appendLog("uploadFileToStorageeBlob", "Exception", errorResponse);
			pushLogIntoLoginservice();
			System.exit(0);
		} catch (StorageException e) {
			String errorResponse = errorUtil.getDataAggregatorError("Storage error: " + e.getMessage(), storageError);
			appendLog("uploadFileToStorageeBlob", "Exception", errorResponse);
			pushLogIntoLoginservice();
			System.exit(0);
		} catch (IOException ioException) {
			String errorResponse = errorUtil.getDataAggregatorError("IO Error: " + ioException.getMessage(), ioError);
			appendLog("pollMessageFromQueue", "Exception", errorResponse);
			pushLogIntoLoginservice();
			System.exit(0);
		}

		return null;
	}

	public void getDataFromDataStore(String queueMsg, String patientID, String batchDateTime, String modelName) {

		// downloadFileFromStorageeBlob("E3278", "2018-06-21T04:03:26-0500",
		// "Original", fileNames);
		// This message will come from queue. currently it is hardcoded as we do
		// not have data.

		// queueMsg = "{ \"ModelName\": \"Trauma\", \"BatchDateTime\":
		// \"2018-06-21T04:03:26-0500\", \"PatientID\": \"E3278\", \"API\": [{
		// \"Data-aggregator-stage\": [{ \"MethodType\": \"GET\", \"Type\":
		// \"RESTful\", \"Parameters\": [\"PatientID\", \"PatientClass\",
		// \"From\", \"To\", \"UserID\"], \"ParametersValue\": [\"Z4495\",
		// \"l\", \"2018-06-18\", \"2018-06-19\", \"1\"], \"URL\":
		// \"https://apporchard.epic.com/interconnect-ao85prd-username/api/epic/2011/Clinical/Patient/GETPATIENTCONTACTS/Patient/Contacts\",
		// \"Name\": \"GetPatientContacts(2011)\" }, { \"MethodType\": \"POST\",
		// \"Type\": \"RESTful\", \"Parameters\": [\"PatientID\", \"UserID\",
		// \"ContactID\"], \"ParametersValue\": [{ \"Type\": \"Internal\",
		// \"ID\": \"Z4495\" }, { \"Type\": \"Internal\", \"ID\": \"1\" }, {
		// \"Type\": \"CSN\", \"ID\": \"1855\" }], \"URL\":
		// \"https://apporchard.epic.com/interconnect-ao85prd-username/api/epic/2011/Clinical/Patient/GETACTIVEPROBLEMS/ActiveProblems?ExcludeNonHospitalProblems={ExcludeNonHospitalProblems}\",
		// \"Name\": \"GetActiveProblems\" }, { \"MethodType\": \"POST\",
		// \"Type\": \"RESTful\", \"Parameters\": [\"PatientID\",
		// \"ComponentTypes\", \"UserID\"], \"ParametersValue\": [\"Z4495\", [{
		// \"Value\": \"1802008\", \"Type\": \"\" }], \"1\"], \"URL\":
		// \"https://apporchard.epic.com/interconnect-ao85prd-username/urn:Epic-com:Results.2014.Services.Utility.GetPatientResultComponents\",
		// \"Name\": \"GetPatientResultComponents\" }, { \"MethodType\":
		// \"POST\", \"Type\": \"RESTful\", \"Parameters\": [\"PatientID\",
		// \"ProfileView\"], \"ParametersValue\": [\"Z4495\", \"2\"], \"URL\":
		// \"https://apporchard.epic.com/interconnect-ao85prd-username/api/epic/2013/Clinical/Utility/GetCurrentMedications/CurrentMedications\",
		// \"Name\": \"GetCurrentMedications\" }, { \"MethodType\": \"POST\",
		// \"Type\": \"RESTful\", \"Parameters\": [\"PatientID\",
		// \"PatientIDType\", \"ContactID\", \"FlowsheetRowIDs\"],
		// \"ParametersValue\": [\"E2731\", \"EPI\", \"58706\", [{ \"Type\":
		// \"EXTERNAL\", \"ID\": \"1400000032\" }]], \"URL\":
		// \"https://apporchard.epic.com/interconnect-ao85prd-username/api/epic/2014/Clinical/Patient/GETFLOWSHEETROWS/FlowsheetRows\",
		// \"Name\": \"GetFlowsheetRows\" }] }, { \"Score-retriever-stage\": [{
		// \"MethodType\": \"POST\", \"Type\": \"RESTful\", \"Parameters\":
		// [\"PatientID\", \"PatientIDType\", \"ContactID\", \"ContactIDType\",
		// \"UserID\", \"UserIDType\", \"FlowsheetID\", \"FlowsheetIDType\",
		// \"Value\", \"InstantValueTaken\", \"FlowsheetTemplateID\",
		// \"FlowsheetTemplateIDType\"], \"ParametersValue\": [\"#PATIENTID#\",
		// \"INTERNAL\", \"#CONTACTID#\", \"#CONTACTIDTYPE#\", \"1\", \"I\",
		// \"1\", \"INTERNAL\", \"#SCORE#\", \"#CURRENT_DATE#\", \"81\",
		// \"INTERNAL\"], \"URL\":
		// \"https://apporchard.epic.com/interconnect-ao85prd-username/api/epic/2011/Clinical/Patient/ADDFLOWSHEETVALUE/FlowsheetValue\",
		// \"Name\": \"AddFlowsheetValue\" }] }], \"ModelVersion\": \"v1\",
		// \"PatientAge\": \"67 y.o.\", }";
		
		//final String storageConnectionString = "DefaultEndpointsProtocol=https;" + "AccountName=" + accountName
			//	+ ";" + "AccountKey=" + accountKey + ";" + "EndpointSuffix=core.windows.net";
		final String storageConnectionString = getSecretFromKeyVault(STORAGE_ACCOUNT_ACCESS_KEY_NAME);	

		final String PARTITION_KEY = "PartitionKey";
		final String ROW_KEY = "RowKey";
		try {

			JSONObject queueMsgJson = new JSONObject();
			try {
				queueMsgJson = new JSONObject(queueMsg);
			} catch (JSONException jsonException) {
				String errorResponse = errorUtil.getDataAggregatorError("Model json are malformed",
						malformedModelJsonError);
				appendLog("getDataFromDataStore", "Exception", errorResponse);
				pushLogIntoLoginservice();
				System.exit(0);
			}
			String partitionKeyValue = "";
			String rowKeyValue = "";
			try {
				partitionKeyValue = queueMsgJson.getString("ModelName");
				rowKeyValue = queueMsgJson.getString("ModelVersion");
			} catch (JSONException jsonException) {
				String errorResponse = errorUtil.getDataAggregatorError("Please provide the valid Model details",
						modelDetailsError);
				appendLog("getDataFromDataStore", "Exception", errorResponse);
				pushLogIntoLoginservice();
				System.exit(0);
			}

			// Get data from Configuration DB
			CloudStorageAccount storageAccount = CloudStorageAccount.parse(storageConnectionString);
			CloudTableClient tableClient = storageAccount.createCloudTableClient();
			CloudTable cloudTable = tableClient.getTableReference(tableName);
			String rowKeyFilter = TableQuery.generateFilterCondition(ROW_KEY, QueryComparisons.EQUAL, rowKeyValue);
			String partitionKeyFilter = TableQuery.generateFilterCondition(PARTITION_KEY, QueryComparisons.EQUAL,
					partitionKeyValue);
			String combinedFilter = TableQuery.combineFilters(rowKeyFilter, Operators.AND, partitionKeyFilter);
			TableQuery<DataAggregatorConfig> partitionQuery = TableQuery.from(DataAggregatorConfig.class)
					.where(combinedFilter);

			for (DataAggregatorConfig entity : cloudTable.execute(partitionQuery)) {
				JSONObject configJson = new JSONObject();
				JSONObject mappingJson = new JSONObject();
				JSONObject traumaModelContratcMappingJson = new JSONObject();
				try {
					try {
						mappingJson = new JSONObject(entity.getScoreRetrieverContractMapping());
						configJson = new JSONObject(entity.getConfiguration());
						traumaModelContratcMappingJson = new JSONObject(entity.getTraumaModelContratcMapping());
					} catch (JSONException jsonException) {
						String errorResponse = errorUtil.getDataAggregatorError(
								"Configuration or mapping json are malformed", malformedTableJson);
						appendLog("pollMessageFromQueue", "Exception", errorResponse);
						pushLogIntoLoginservice();
						System.exit(0);
					}

					JSONArray apiJSonArray = configJson.optJSONArray("API");
					JSONObject apiJsonObject = new JSONObject();
					apiJsonObject.put("API", apiJSonArray);
					JSONObject tableJsonObject = new JSONObject();
					tableJsonObject.put("configuration", apiJsonObject);
					JSONObject schedulerContractJson = new JSONObject();
					schedulerContractJson.put("queueMessage", queueMsgJson);
					JSONArray dataJsonArray = new JSONArray();

					dataJsonArray.put(schedulerContractJson);
					dataJsonArray.put(tableJsonObject);
					List<List<String>> lisOfflowInvoker = JsonPath.parse(apiJsonObject.toString())
							.read("$.API..Score-retriever-stage");
					List<String> flowInvokerList = new ArrayList<>();
					int flowInvokerListSize = lisOfflowInvoker.size();

					for (int i = 0; i < flowInvokerListSize; i++) {
						flowInvokerList = lisOfflowInvoker.get(i);
					}
					flowInvokerListSize = flowInvokerList.size();
					JSONArray flowInvokerJsonArray = new JSONArray(flowInvokerList);
					int flowInvokerJsonArrLength = flowInvokerJsonArray.length();

					List<String> fileNames = new ArrayList<>();
					fileNames.add("GetActiveProblems");
					fileNames.add("GetCurrentMedications");
					fileNames.add("GetFlowsheetRows");
					fileNames.add("GetPatientContacts");
					fileNames.add("GetPatientResultComponents");
					for (int i = 0; i < flowInvokerJsonArrLength; i++) {
						JSONObject jsonObject = new JSONObject();
						for (String fileName : fileNames) {
							jsonObject.put(fileName,
									downloadFileFromStorageeBlob(patientID, batchDateTime, "Original", fileName));
						}
						dataJsonArray.put(jsonObject);
					}
					JSONObject dataJsonObject = new JSONObject();
					dataJsonObject.put("data", dataJsonArray);
					ObjectMapper objectMapper = new ObjectMapper();
					// String json = "[{ \"PatientID\":
					// \"queueMessage..PatientID\", \"Patient.Age\":
					// \"queueMessage..PatientAge\",
					// \"GetActiveProblems..Problems\": [{
					// \"Problems.Problem.Comment\": \"Comment\",
					// \"Problems.Problem.DiagnosisIDs\":
					// \"DiagnosisIDs[?(@.Type == \\\"Internal\\\")]\",
					// \"Problems.Problem.IsChronic\": \"IsChronic\",
					// \"Problems.Problem.IsHospitalProblem\":
					// \"IsHospitalProblem\",
					// \"Problems.Problem.IsPresentOnAdmission\":
					// \"IsPresentOnAdmission\", \"Problems.Problem.NotedDate\":
					// \"NotedDate\", \"Problems.Problem.ProblemClass\":
					// \"ProblemClass\" }],
					// \"GetPatientResultComponents..ResultComponents\": [{
					// \"Component.ComponentID\": \"Component..ComponentID\",
					// \"Component.Abnormality.Title\":
					// \"Component.Abnormality.Title\",
					// \"Component.PrioritizedDate\":
					// \"Component.PrioritizedDate\",
					// \"Component.PrioritizedTime\":
					// \"Component.PrioritizedTime\",
					// \"Component.ReferenceRange\":
					// \"Component.ReferenceRange\", \"Component.Status.Title\":
					// \"Component.Status.Title\", \"Component.Units\":
					// \"Component.Units\", \"Component.Value\":
					// \"Component.Value\" }], \"GetPatientContacts..Contacts\":
					// [{ \"Contact.ID\": \"IDs..ID[?(@.Type == \\\"CSN\\\")]\",
					// \"Contact.DateTime\": \"DateTime\",
					// \"Contact.DepartmentID\": \"DepartmentIDs..ID[?(@.Type ==
					// \\\"INTERNAL\\\")]\", \"Contact.DepartmentName\":
					// \"DepartmentName\", \"GetFlowsheetRows..FlowsheetRows\":
					// [{ \"FlowsheetRow.Name\": \"FlowsheetRow..Name\",
					// \"FlowsheetRow.FlowsheetRowID\":
					// \"FlowsheetRow..FlowsheetRowID..IDType..ID[?(@.Type ==
					// \\\"INTERNAL\\\")]\", \"FlowsheetRow.RawValue\":
					// \"FlowsheetRow..FlowsheetColumns..FlowsheetColumn..RawValue\",
					// \"FlowsheetRow.Instant\":
					// \"FlowsheetRow..FlowsheetColumns..FlowsheetColumn..Instant\"
					// }] }] }]";
					String json1 = objectMapper.writeValueAsString(
							createHashMapFromJsonString(traumaModelContratcMappingJson.toString().trim(),
									dataJsonObject.toString(), dataJsonObject.toString()));
					uploadFileToStorageeBlob(json1, patientID, batchDateTime, "Transformed", modelName);
					System.out.println("***********#####################**********#*#*#*#*#*#* " + json1);

					JSONArray mappingJsonArr = mappingJson.optJSONArray("params");
					int mappingJsonArrLength = mappingJsonArr.length();

					JSONObject primaryJsonObject = mappingJson.optJSONObject("primary");
					if (primaryJsonObject.length() != 0) {
						String primaryName = primaryJsonObject.optString("name");
						String primaryValue = primaryJsonObject.optString("value");
						List<List<String>> primaryValueList = JsonPath.parse(dataJsonObject.toString())
								.read("$.data.." + primaryValue);
						List<String> privaleValue = new ArrayList<>();
						int primaryValueListSize = primaryValueList.size();
						for (int l = 0; l < primaryValueListSize; l++) {
							privaleValue = primaryValueList.get(l);
						}
						primaryValueListSize = privaleValue.size();
						for (int i = 0; i < primaryValueListSize; i++) {
							JSONObject dataAggregatorContract = new JSONObject();
							for (int j = 0; j < mappingJsonArrLength; j++) {
								JSONObject mappingJsonObject = mappingJsonArr.getJSONObject(j);
								String name = mappingJsonObject.optString("name");
								String value = mappingJsonObject.optString("value");
								String type = mappingJsonObject.optString("type");
								String parent = mappingJsonObject.optString("parent");
								boolean iteration = mappingJsonObject.optBoolean("iteration");
								boolean fromPrimaryLoop = mappingJsonObject.optBoolean("fromPrimaryLoop");
								JSONArray conditionsArr = mappingJsonObject.optJSONArray("conditions");
								int conditionArrLength = conditionsArr.length();
								List<String> contractValueList = new ArrayList<>();
								// Primary is ALWAYS Iterable. all Iterable !=
								// Primary
								if ("String".equalsIgnoreCase(type)) {
									if (!value.isEmpty()) {
										contractValueList = getContractValueForPrimary(i, primaryName, name, value,
												primaryValue, iteration, fromPrimaryLoop, conditionsArr,
												dataJsonObject);
										String contractValue = null;
										if (null != contractValueList && !contractValueList.isEmpty()) {
											contractValue = contractValueList.get(0);
										} else {
											contractValue = value;
										}
										dataAggregatorContract = pushObjectIntoDataAggContract(parent, name,
												contractValue, dataAggregatorContract, contractValueList);
									} else {
										// TODO: error code
									}
								} else if ("Object".equalsIgnoreCase(type)) {
									if (value.isEmpty()) {
										// Push an empty array against the name
										JSONArray array = new JSONArray();
										dataAggregatorContract.put(name, array);
									} else {
										contractValueList = getContractValueForPrimary(i, primaryName, name, value,
												primaryValue, iteration, fromPrimaryLoop, conditionsArr,
												dataJsonObject);
										// TODO: Create a common method
										String contractValue = contractValueList.get(0);
										dataAggregatorContract = pushObjectIntoDataAggContract(parent, name,
												contractValue, dataAggregatorContract, contractValueList);
									}
								} else if ("Array".equalsIgnoreCase(type)) {
									if (value.isEmpty()) {
										JSONArray array = new JSONArray();
										dataAggregatorContract.put(name, array);
									} else {
										if (conditionArrLength != 0) {
											for (int k = 0; k < conditionArrLength; k++) {
												JSONObject conditionJsonObject = conditionsArr.getJSONObject(k);
												String conditionKey = conditionJsonObject.optString("conditionKey");
												String conditionValue = conditionJsonObject.optString("conditionValue");
												if (!iteration && !fromPrimaryLoop) {
													contractValueList = JsonPath.parse(dataJsonObject.toString())
															.read("$.data.." + value + "[?(@." + conditionKey + " == '"
																	+ conditionValue + "')]");
												} else {
													value = primaryValue + "[" + i + "].." + value;
													contractValueList = JsonPath.parse(dataJsonObject.toString())
															.read("$.data.." + value + "[?(@." + conditionKey + " == '"
																	+ conditionValue + "')].." + name);
												}
											}
										} else {
											if (!iteration && !fromPrimaryLoop) {
												List<List<String>> list = JsonPath.parse(dataJsonObject.toString())
														.read("$.data.." + value);
												for (int h = 0; h < list.size(); h++) {
													contractValueList = list.get(h);
												}
											} else {
												contractValueList = JsonPath.parse(dataJsonObject.toString())
														.read("$.data.." + value + "[" + i + "].." + name);
											}
										}
										if (dataAggregatorContract.has(parent)) {
											JSONObject jsonObject = new JSONObject();
											jsonObject.put(name, contractValueList);
											dataAggregatorContract.accumulate(parent, jsonObject);
										} else {
											dataAggregatorContract.put(name, value);

										}
									}
								}
							}
							pushMessageIntoQueue(dataAggregatorContract);

						}
					} else {
						JSONObject dataAggregatorContract = new JSONObject();
						for (int j = 0; j < mappingJsonArrLength; j++) {
							JSONObject contractJsonObject = mappingJsonArr.getJSONObject(j);
							String name = contractJsonObject.optString("name");
							String value = contractJsonObject.optString("value");
							String type = contractJsonObject.optString("type");
							String parent = contractJsonObject.optString("parent");
							boolean iteration = contractJsonObject.optBoolean("iteration");
							JSONArray conditionsArr = contractJsonObject.optJSONArray("conditions");
							JSONArray replaceArray = contractJsonObject.optJSONArray("replace");
							int conditionArrLength = conditionsArr.length();
							List<String> contractValueList = new ArrayList<>();
							if ("String".equalsIgnoreCase(type)) {
								if (!value.isEmpty()) {
									contractValueList = getContractValue(j, name, value, iteration, conditionsArr,
											dataJsonObject);
									String contractValue = null;
									if (null != contractValueList && !contractValueList.isEmpty()) {
										contractValue = String.valueOf((contractValueList.get(0)));
									} else {
										contractValue = value;
									}
									// TODO: Common method
									dataAggregatorContract = pushObjectIntoDataAggContract(parent, name, contractValue,
											dataAggregatorContract, contractValueList);

								} else {
									// TODO: error code
								}
							} else if ("Object".equalsIgnoreCase(type)) {
								if (value.isEmpty()) {
									// Push an empty array against the name
									JSONArray array = new JSONArray();
									dataAggregatorContract.put(name, array);
								} else {
									// TODO: Create a common method
									contractValueList = getContractValue(j, name, value, iteration, conditionsArr,
											dataJsonObject);
									String contractValue = contractValueList.get(0);
									// TODO: Common method
									dataAggregatorContract = pushObjectIntoDataAggContract(parent, name, contractValue,
											dataAggregatorContract, contractValueList);
								}

							} else if ("Array".equalsIgnoreCase(type)) {

								if (value.isEmpty()) {
									JSONArray array = new JSONArray();
									dataAggregatorContract.put(name, array);
								} else {
									if (conditionArrLength != 0) {
										for (int k = 0; k < conditionArrLength; k++) {
											JSONObject conditionJsonObject = conditionsArr.optJSONObject(k);
											String condition = conditionJsonObject.optString("condition");
											String conditionValue = conditionJsonObject.optString("conditionValue");
											if (!iteration) {
												contractValueList = JsonPath.parse(dataJsonObject.toString())
														.read("$.data.." + value + "[?(@." + condition + " == '"
																+ conditionValue + "')]");
											} else {
												contractValueList = JsonPath.parse(dataJsonObject.toString())
														.read("$.data.." + value + "[?(@." + condition + " == '"
																+ conditionValue + "')].." + name);
											}
										}
									} else {
										if (!iteration) {
											List<List<String>> list = JsonPath.parse(dataJsonObject.toString())
													.read("$.data.." + value);
											for (int h = 0; h < list.size(); h++) {
												contractValueList = list.get(h);
											}
										} else {
											contractValueList = JsonPath.parse(dataJsonObject.toString())
													.read("$.data.." + value + "[" + j + "].." + name);
										}
									}

									if (dataAggregatorContract.has(parent)) {
										JSONObject jsonObject = new JSONObject();
										jsonObject.put(name, contractValueList);
										dataAggregatorContract.accumulate(parent, jsonObject);
									} else {
										dataAggregatorContract.put(name, value);

									}
								}
							}
							if (null != replaceArray && replaceArray.length() > 0) {
								String result = dataAggregatorContract.toString();
								for (int k = 0; k < replaceArray.length(); k++) {
									JSONObject replaceObject = replaceArray.optJSONObject(k);
									String condition = replaceObject.optString("search");
									String conditionValue = replaceObject.optString("value");
									List<List<String>> replaceValue = JsonPath.parse(dataJsonObject.toString())
											.read("$.data.." + conditionValue);
									result = result.replaceAll(condition,
											replaceValue.toString().substring(2, replaceValue.toString().length() - 2));
									dataAggregatorContract = new JSONObject(result);
								}

								System.out.println("Queue as per new contract::::::: " + result);
							}

						}

						System.out.println("DataContract is @@@@@@@@@@@@@@@@@@@@@ " + dataAggregatorContract);
						pushMessageIntoQueue(dataAggregatorContract);
					}
				} catch (JSONException e) {
					String errorResponse = errorUtil.getDataAggregatorError(e.getMessage(), tablejsonObjectError);
					appendLog("getDataFromDataStore", "Exception", errorResponse);
					pushLogIntoLoginservice();
					System.exit(0);
				}
			}
		} catch (InvalidKeyException invalidKeyException) {
			String errorResponse = errorUtil
					.getDataAggregatorError("Please provide valid AccountKey to connect to azure storage table : "
							+ invalidKeyException.getMessage(), accountKeyError);
			appendLog("getDataFromDataStore", "Exception", errorResponse);
			pushLogIntoLoginservice();
			System.exit(0);

		} catch (URISyntaxException uriSyntaxException) {
			String errorResponse = errorUtil.getDataAggregatorError(
					"Please provide valid URI to connect to azure storage table : " + uriSyntaxException.getMessage(),
					tableURIError);
			appendLog("getDataFromDataStore", "Exception", errorResponse);
			pushLogIntoLoginservice();
			System.exit(0);
		} catch (StorageException e) {
			String errorResponse = errorUtil.getDataAggregatorError(
					"Please provide valid table name or table does not exist", tableStorageError);
			appendLog("getDataFromDataStore", "Exception", errorResponse);
			pushLogIntoLoginservice();
			System.exit(0);
		} catch (Exception e) {
			String errorResponse = errorUtil.getDataAggregatorError("Exception in getDataFromDataStore()",
					exceptionError);
			appendLog("getDataFromDataStore", "Exception", errorResponse);
			pushLogIntoLoginservice();
			System.exit(0);
		}
	}

	public List<String> getContractValueForPrimary(int i, String primaryName, String name, String value,
			String primaryValue, boolean iteration, boolean fromPrimaryLoop, JSONArray conditionsArray,
			JSONObject dataJsonObject) {
		List<String> contractValueList = new ArrayList<>();
		int conditionArrLength = conditionsArray.length();
		try {
			if (conditionArrLength != 0) {
				for (int k = 0; k < conditionArrLength; k++) {
					JSONObject conditionJsonObject = conditionsArray.optJSONObject(k);
					String conditionKey = conditionJsonObject.optString("conditionKey");
					String conditionValue = conditionJsonObject.optString("conditionValue");
					if (!iteration && !fromPrimaryLoop) // TODO: Check this
														// condition, and below
														// as
														// well
					{
						contractValueList = JsonPath.parse(dataJsonObject.toString())
								.read("$.data.." + value + "[?(@." + conditionKey + " == '" + conditionValue + "')]");
					} else {
						// It can be primary
						// if(primaryName.equals(name))
						value = primaryValue + "[" + i + "].." + value;
						contractValueList = JsonPath.parse(dataJsonObject.toString()).read("$.data.." + value + "[?(@."
								+ conditionKey + " == '" + conditionValue + "')].." + name);
						// TODO:
						// It cannot be primary
					}
				}
			} else {
				if (!iteration && !fromPrimaryLoop) {
					contractValueList = JsonPath.parse(dataJsonObject.toString()).read("$.data.." + value);
				} else if (fromPrimaryLoop) {
					value = primaryValue + "[" + i + "].." + value;
					contractValueList = JsonPath.parse(dataJsonObject.toString()).read("$.data.." + value);
				} else {
					contractValueList = JsonPath.parse(dataJsonObject.toString())
							.read("$.data.." + value + "[" + i + "].." + name);
				}
			}
		} catch (Exception e) {
			String errorResponse = errorUtil.getDataAggregatorError("Exception in getContractValueForPrimary()",
					exceptionError);
			appendLog("getContractValueForPrimary", "Exception", errorResponse);
			pushLogIntoLoginservice();
			System.exit(0);
		}
		return contractValueList;
	}

	public List<String> getContractValue(int i, String name, String value, boolean iteration, JSONArray conditionsArray,
			JSONObject dataJsonObject) {
		List<String> contractValueList = new ArrayList<>();
		int conditionArrLength = conditionsArray.length();
		try {
			if (conditionArrLength != 0) {
				for (int k = 0; k < conditionArrLength; k++) {
					JSONObject conditionJsonObject = conditionsArray.optJSONObject(k);
					String condition = conditionJsonObject.optString("condition");
					String conditionValue = conditionJsonObject.optString("conditionValue");
					if (!iteration) {
						contractValueList = JsonPath.parse(dataJsonObject.toString())
								.read("$.data.." + value + "[?(@." + condition + " == '" + conditionValue + "')]");
					} else {
						contractValueList = JsonPath.parse(dataJsonObject.toString()).read(
								"$.data.." + value + "[?(@." + condition + " == '" + conditionValue + "')].." + name);
					}
				}
			} else {
				if (!iteration) {
					contractValueList = JsonPath.parse(dataJsonObject.toString()).read("$.data.." + value);
				} else {
					contractValueList = JsonPath.parse(dataJsonObject.toString())
							.read("$.data.." + value + "[k].." + name);

				}
			}
		} catch (Exception e) {
			String errorResponse = errorUtil.getDataAggregatorError("Exception in getContractValue()", exceptionError);
			appendLog("getContractValue", "Exception", errorResponse);
			pushLogIntoLoginservice();
			System.exit(0);
		}
		return contractValueList;
	}

	public JSONObject pushObjectIntoDataAggContract(String parent, String name, String value,
			JSONObject dataAggregatorContract, List<String> contractValueList) {
		try {
			if (dataAggregatorContract.has(parent)) {
				JSONObject jsonObject = new JSONObject();
				jsonObject.put(name, contractValueList);
				dataAggregatorContract.accumulate(parent, jsonObject);
			} else {
				dataAggregatorContract.put(name, value.trim());
			}
		} catch (Exception e) {
			String errorResponse = errorUtil.getDataAggregatorError("Exception in pushObjectIntoDataAggContract()",
					exceptionError);
			appendLog("pushObjectIntoDataAggContract", "Exception", errorResponse);
			pushLogIntoLoginservice();
			System.exit(0);
		}
		return dataAggregatorContract;
	}

	public void pushMessageIntoQueue(JSONObject dataAggregatorContract) {
		try {
			LocalDateTime localDateTime = LocalDateTime.now();
			DateTimeFormatter formatters = DateTimeFormatter.ofPattern("uuuu-MM-d");
			String dataAggregator = dataAggregatorContract.toString();
			dataAggregator = dataAggregator.replaceAll("#CURRENT_DATE#", localDateTime.format(formatters));
			Pattern pattern = Pattern.compile("#CURRENT_DATE-\\d#");
			Matcher matcher = pattern.matcher(dataAggregator);
			while (matcher.find()) {
				String matchedString = matcher.group();
				String[] prevoiusArray = matchedString.split("-");
				String previouDay = prevoiusArray[1];
				localDateTime = localDateTime.minusDays(Long.parseLong(previouDay.replace("}", "")));
				dataAggregator = dataAggregator.replace(matchedString, localDateTime.format(formatters));
			}

			Configuration config = ServiceBusConfiguration.configureWithSASAuthentication(nameSpace, sasKeyName,
					sasKeyKeyVault, serviceBusRootUri);
			ServiceBusContract serviceBusContract = ServiceBusService.create(config);
			BrokeredMessage brokeredMessage = new BrokeredMessage(dataAggregator);
			serviceBusContract.sendQueueMessage(scoreRetrieverQueueName, brokeredMessage);

			System.out.println("final string is !!!!!!!!!!!!!!!!!!!!!!!!" + dataAggregator);
		} catch (ServiceException serviceException) {
			String errorResponse = errorUtil.getDataAggregatorError(
					"Error in pushing message into queue : " + serviceException.getErrorMessage(), pushingMsgError);
			appendLog("pushMessageIntoQueue", "Exception", errorResponse);
			System.exit(0);
		}
	}

	public String getBatchDateTime() {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ssZZZZ");
		Date datetime = new Date();
		sdf.setTimeZone(TimeZone.getTimeZone(timeZone));
		return sdf.format(datetime);
	}

	public Map<String, Object> createHashMapFromJsonString(String json, String dataJson, String mainData)
			throws Exception {
		JsonParser parser = new JsonParser();
		LinkedHashMap<String, Object> map = new LinkedHashMap<String, Object>();
		try {
			if (json.trim().startsWith("[")) {
				json = json.trim().substring(1, json.length() - 1);
			}
			System.out.println(json);
			JsonObject object = (JsonObject) parser.parse(json);
			Set<Map.Entry<String, JsonElement>> set = object.entrySet();
			Iterator<Map.Entry<String, JsonElement>> iterator = set.iterator();

			while (iterator.hasNext()) {

				Map.Entry<String, JsonElement> entry = iterator.next();
				String key = entry.getKey();
				JsonElement value = entry.getValue();

				if (null != value) {
					if (!value.isJsonPrimitive()) {
						if (value.isJsonObject()) {

							map.put(key, createHashMapFromJsonString(value.toString(), dataJson, mainData));
						} else if (value.isJsonArray() && value.toString().contains(":")) {

							JsonArray array = value.getAsJsonArray();
							if (null != array) {
								net.minidev.json.JSONArray js = new net.minidev.json.JSONArray();
								for (int i = 0; i < array.size(); i++) {
									net.minidev.json.JSONArray length = JsonPath.parse(mainData)
											.read("$.data.." + key + ".length()");
									net.minidev.json.JSONArray jsonArray = JsonPath.parse(mainData)
											.read("$.data.." + key);
									if ((jsonArray.toString().trim().regionMatches(0, "[[", 0, 2))) {
										Object obj = jsonArray.get(0);
										jsonArray = (net.minidev.json.JSONArray) obj;
									}
									for (int j = 0; j < Integer.parseInt(length.get(0).toString()); j++) {
										net.minidev.json.JSONObject jsonObj = new net.minidev.json.JSONObject();
										jsonObj.put("data", jsonArray.get(j));
										js.appendElement(createHashMapFromJsonString(array.get(i).toString(),
												jsonObj.toString(), mainData));
									}
								}
								String[] test = (StringUtils.split(key, ".."));
								map.put(test[test.length - 1], js);
							}
						} else if (value.isJsonArray() && !value.toString().contains(":")) {
							map.put(key, value.getAsJsonArray());
						}
					} else {
						net.minidev.json.JSONArray test = JsonPath.parse(dataJson)
								.read("$.data.." + value.getAsString());
						String abc = "";
						if (null != test && !test.isEmpty() && test.toString().startsWith("[\"")) {
							abc = test.toJSONString().substring(2, test.toString().length() - 2);
						} else if (null != test && !test.isEmpty()) {
							abc = test.toJSONString().substring(1, test.toString().length() - 1);
						}
						map.put(key, abc);
					}
				}
			}
		} catch (Exception e) {
			String errorResponse = errorUtil.getDataAggregatorError("Exception in createHashMapFromJsonString()",
					exceptionError);
			appendLog("createHashMapFromJsonString", "Exception", errorResponse);
			pushLogIntoLoginservice();
			System.exit(0);
		}
		return map;
	}
}
