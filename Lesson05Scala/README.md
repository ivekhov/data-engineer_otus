How to start project on local machine 
on local machine command :
should be spark 3 version in home directory
should be data-file .json in data catalogue https://storage.googleapis.com/otus_sample_data/winemag-data.json.tgz
should be compiled fat-jar by sbt assembly

~/spark/spark-3.0.1-bin-hadoop2.7/bin/spark-submit --master local[*] --class JsonReader   ./target/scala-2.11/Lesson05Scala-assembly-0.1.jar ./data/winemag-data-130k-v2.json


-----

asciinema recording

[![asciicast](https://asciinema.org/a/EoKBSfVkFOTU1VPiqrRE8sWLG.svg)](https://asciinema.org/a/EoKBSfVkFOTU1VPiqrRE8sWLG)

----


-----

Как настроить проект в Idea

1 создать новый проект 
scala - sbt
выбирать версии
	scalaVersion := "2.11.12"
	sbt.version = 1.3.13

2 создать и заполнить файл
./build.sbt
 	libraryDependencies ++= Seq(
		  "org.apache.spark" %% "spark-sql" % "2.4.7" % Provided
	)

	подтягивается основной спарк и спарк-sql
	% Provided - отвечает за то, чтобы не паковать в фэт джарник спарк который и так на кластере


3 поставить 1 галку
в preferences project ->  build execution -> Build tools -> sbt: sbt shell use for  builds


4 добавить в файл в корне проекта ссылку на плагин ассембли
./project/plugins.sbt:
	resolvers += Resolver.bintrayRepo("eed3si9n", "sbt-plugins")
	addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.10")


5 находясь в файле ./build.sbt , выполняет Load Sbt Changes


6 код пишем в классах ./src/main/scala  - тип файлов Scala Object (обычноу)




ПОДГОТОВКА К РАЗМЕЩЕНИЮ В ПРОДАКШЕНЕ НА КЛАСТЕРЕ

шаг 0 - предыстория в sbt shell> 
	compile
	package
Но для кластера нужны в том числе и другие библиотеки - поэтому нужен fat jar для кластера. он раскладывается ярном на ноды

1 
Нужен плагин sbt assembly - см п 5 выше
в sbt shell> 
	assembly

2
Запуск  спарком из терминала

~/spark/spark-3.0.1-bin-hadoop2.7/bin/spark-submit --master local target/scala-2.11/scala_intro-assembly-0.1.jar

(версия под 2 спарк на основе версии Java 1.8 - не работает, нужен спарк 3)

