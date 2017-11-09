# DataAnalysis
pull data from Kafka , do something and send back to Kafka

从10.108.219.61:9092的Kafka中device topic中拉取数据，数据格式： {"uid":"922291","data":10,"current_time":"2017-11-06 17:44:45"}

把计算结果存储到10.108.219.61:9092的Kafka中AnalysisData topic中，数据格式为： (uid, 方差，数量，平均值)

提交到spark集群中：~/project/spark/bin/spark-submit --master spark://10.108.218.58:7077 --class com.tjlcast.DeviceData ./DataAnalysis.jar
