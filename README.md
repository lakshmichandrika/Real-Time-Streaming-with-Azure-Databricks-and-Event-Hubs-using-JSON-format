# Real-Time-Streaming-with-Azure-Databricks-and-Event-Hubs-using-JSON-format
we are creating real time data in Azure Event Hubs and then store and process the data in our Databricks Lakehouse, implementing the Bronze, Silver, and Gold layers of the Medallion Architecture. The final step involves creating a near real-time report on Power BI, showcasing the power of streaming analytics

Prerequisites
1.Active Azure subscription with access to Azure Databricks and Event Hubs.

2.Databricks Workspace with Unity Catalog Enabled.

3.Azure Event Hubs Service.

4.Power BI Desktop (Windows).

5.Familiarity with Python, Spark, SQL, and basic data engineering concepts.

![image](https://github.com/user-attachments/assets/b2c2403e-165f-4aed-ab20-954dac9c891e)

1.Data Sources: Streaming data from IoT devices or social media feeds. (Simulated in Event Hubs)

2.Ingestion: Azure Event Hubs for capturing real-time data.

3.Processing: Azure Databricks for stream processing using Structured Streaming.

4.Storage: Processed data stored Azure Data Lake (Delta Format).

5.Visualisation: Data visualized using Power BI.

process:

1.create an event bub namespace-event hub-shared access policy

2.in event hub-features-generate data(preview)-copy the input  json data-send

![image](https://github.com/user-attachments/assets/67a2a6f2-fca9-4c27-8fe3-97ff4ba869b8)

3.lly change some values in json and give input as 2 to 3 inputs.

4.create databricks workspace and give compute settings

Single Node Compute Cluster: 12.2 LTS (includes Apache Spark 3.3.2, Scala 2.12)

Databricks Workspace (Unity Catalog enabled)

![image](https://github.com/user-attachments/assets/f30088ff-702f-4575-9e49-3d69533dab4d)

5.click on cluster-go to libraries-install new-maven-search packages

![image](https://github.com/user-attachments/assets/49f80cc9-efdc-4463-8e63-31998569864e)

![image](https://github.com/user-attachments/assets/380bebc7-eb52-459e-9842-d4bc3201432c)



























