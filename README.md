# Customer360
Customer360 is to classify the live streaming data with the help of Machine Learning Modules.

# Product Perspective

This product is a replacement for the existing filter system in data pipelines which classify the streaming data into their respective databases. The basic concept of Customer360 is to find an more accurate and robust way of classifying high streaming data which in the case of version one of the product is done by Apache Kafka which acts as the data pipeline which streams data from the API in this case Postman. The data is sent to Kafka Producer which is in a Flask micro web framework and queued to Kafka Consumer which in turn with the help of the machine learning model -XGBoost which is an optimized distributed gradient boosting library designed to be highly efficient, flexible and portable is used to classify the data. Once the data is classified it is sent to its respective databases. Apache Cassandra is used as the default database as it is Elastically Scalable, High Availability and Fault Tolerance, High Performance and is Schema free.

![image1](https://github.com/Yash-Z/Customer360/blob/master/image/C3.png)
