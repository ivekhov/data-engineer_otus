Запуск

~/spark/spark-3.0.1-bin-hadoop2.7/bin/spark-submit --master local[*] --class com.example.BostonCrimesMap ./target/scala-2.12/Lesson07Spark-assembly-0.1.jar ./data/crime.csv ./data/offense_codes.csv ./output/parquet2


