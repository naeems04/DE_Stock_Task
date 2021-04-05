# naeems04 - Data Engineering Assesment

### Architecture
![](stockstreaming/Stock_Stream_Architecture.png)


### First Use Case
#Project Structure
##stockstreaming
-Streaming Dataflow pipeline code
1. main_streaming.py	 				- The entry point for the Dataflow streaming pipeline.
2. run.sh 							- The script to execute the Dataflow pipeline
3. Stock_Stream_Architecture.png		- Architecture diagram
4. tests								- The unit testcases for the pipeline
	1. unit_test.py					- Test cases for checking top 3 average, 1, 5 and 15 minutes window over 10 sec testcases
		2. main_test.py					- Generating dummy data for /test_data/input/stream_data.txt
5. test-data							- Unit testing input data
		1. input						- Input test data 
		   - input_data.txt 			- Containing data for top 3 average calculation without window
		   - stream_data.txt			- Containing data for top 3 average calculation with window for 1, 5 and 15 minutes
		   
##stock_publisher
A docker based application for continously calling Rest Stock Quote Endpoint and publishing data to Google PubSub
1. connector.py 						- The logic for calling Rest Api and publishing is in this file
2. config.cnf 						- configuration for Stock data endpoint, companies to be considered, Google ProjectID and PubSub topic name
3. requirements.txt					- All the depndencies to be included here
4. Dockerfile						- Docker file


### Second use-case to find top price stock every 5 minutes
1. top_price_streaming.py 			- The logic for calculating top price stock every 5 minutes interval and inserting into BigQuery table.
2. tests/unit_top_price_.py			- Unit testcases for the use-case.
	
					
# Running the dataflow
-run.sh has the command to run the dataflow pipeline.
Below are the arguments that need to be passed to the pipeline.
1. project - Project Name in Google
2. runner  - DataflowRunner for Prod and DirectRunner for local environment
3. staging_location - Path of GoogleCloud storage bucket for staging location
4. temp_location - Path of GoogleCloud storage bucket for temporary location
5. streaming	 -  For indicating pipeline type as streaming
6. input_topic   - PubSuc topic name from where data is read
7. output_table  - BigQuery output table having schema as [symbol:String, average:FLOAT, timestamp:String, aggregation_type:String]
8. region        - Region in which services are run
	
	
#Main Steps in Streaming Pipeline:
- Reading from PubSub topic
- Parsing the json returned from the topic and fetching only the required fields for further processing
- Forming Key Value pair
- Based on Hopping windowing, starting every 10 seconds and capture data for 1, 5 and 15 minutes below steps are performed:
	1. Calculate mean using MeanCombineFn function for the beam.
	2. Combine Globally for the values to calculate top 3 mean
	3. Add window end timestamp and aggregation type in the data for further processing.
	NOTE: aggregation type is 1, 5 or 15 based on the window. To identify the records after its inserted into BigQuery table.
- Data received from Hopping window is converted into json
- Output is written into BigQuery table

### Manual Deployment
- Refer deployment_manual_steps.txt
- Also deploy_CICD_steps.txt is the file which indicates creating templates and job from console.


### Future Scope - Need to develop
CICD pipeline need to be created to deploy the artifacts without manual intervention.




