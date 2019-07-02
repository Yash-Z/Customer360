import json
import pandas as pd
import numpy as np
from imblearn.over_sampling import SMOTE
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml import Pipeline
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.feature import StringIndexer, VectorIndexer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler
# from pyspark.mllib.util import MLUtils
# from pyspark.mllib.tree import GradientBoostedTrees, GradientBoostedTreesModel
# from pyspark.mllib.util import MLUtils


sc = SparkContext("local", "fraud_data")
spark=SparkSession.builder.appName("fraud_data").getOrCreate()

data = spark.read.csv('fraud_data.csv', header=True, inferSchema=True)
print(data.columns)

assembler=VectorAssembler(inputCols=['LIMIT_BAL','EDUCATION','PAY_1','PAY_2','PAY_3','PAY_4','PAY_5','PAY_6','BILL_AMT1','BILL_AMT2','BILL_AMT3','BILL_AMT4','BILL_AMT5','BILL_AMT6','PAY_AMT1','PAY_AMT2','PAY_AMT3','PAY_AMT4','PAY_AMT5','PAY_AMT6','Sex_marr','AgeBin'],outputCol='features')
label_stringIdx = StringIndexer(inputCol = 'def_pay', outputCol = 'label')
output_data=assembler.transform(data)
(trainingData, testData) = data.randomSplit([0.7, 0.3])
#print("Training Dataset Count: " + str(trainingData.count()))
#print("Test Dataset Count: " + str(testData.count()))
gbt = GBTClassifier(labelCol="def_pay", featuresCol="features", maxIter=10)
pipeline = Pipeline(stages=[label_stringIdx, assembler, gbt])
model = pipeline.fit(trainingData)
predictions = model.transform(testData)

model.write().overwrite().save("model")
#model.save(sc, "target/tmp/myRandomForestClassificationModel")
#predictions.select("prediction", "def_pay", "features").show(100)
evaluator = MulticlassClassificationEvaluator(
    labelCol="def_pay", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print("Accuracy:",accuracy)
print("Test Error = %g" % (1.0 - accuracy))
gbtModel = model.stages[2]
print(gbtModel)  # summary only
