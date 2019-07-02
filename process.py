from kafka import KafkaConsumer
import json
import pandas as pd
import numpy as np
import pickle
from cassandra_db import *
import pprint
from pyspark.ml.classification import GBTClassifier,GBTClassificationModel
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
# from pyspark.mllib.tree import GradientBoostedTrees, GradientBoostedTreesModel


sc = SparkContext("local", "fraud_data")

spark=SparkSession.builder.appName("fraud_data").getOrCreate()

spark.conf.set("spark.sql.execution.arrow.enabled", "true")

consumer = KafkaConsumer('yash',
                         group_id='my-group',
                         bootstrap_servers=['localhost:9092'])


xg_boost_model = PipelineModel.load("model")
print("loaded")

filename = "header_test.sav"
header_test = pickle.load(open(filename, 'rb'))

for message in consumer:

    response = json.loads((message.value).decode())

    for data in response:
        test_list = [float(data[x]) for x in header_test]
        dfObj = pd.DataFrame(np.nan, index=list('a'), columns=header_test)

        for x in range(len(test_list)):
            dfObj[header_test[x]] = test_list[x] 

        data.pop("")

        print("\n#############")
        print("# Json Data #")
        print("#############\n")
        pprint.pprint(data)

        df = spark.createDataFrame(dfObj)
        y_pred = xg_boost_model.transform(df)

        # Cassandra Database
        print("\n##########")
        print("# Result #")
        print("##########\n")

        result = int(y_pred.select("prediction").collect()[0]['prediction'])

        if result == 0:
        	print("\nResult :", result, "=> User Is Credible")
        	json_value = json.dumps(data)
        	session_1.execute(credible_insert_statement, [json_value]) 
            
        elif result == 1:
        	print("\nResult :", result, "=> User Is NonCredible")
        	json_value = json.dumps(data)
        	session_1.execute(non_credible_insert_statement, [json_value])

        print("\n"+"="*50)

#ml_process.py