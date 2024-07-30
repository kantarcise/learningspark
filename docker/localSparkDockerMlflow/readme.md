# Spark ML with Mlflow 🍒

## What is going on ?

For the Mlflow applications in Chapter 11, like [MlflowSimple](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/MlflowSimple.scala), we need a [Mlflow Tracking Server](https://mlflow.org/docs/latest/getting-started/tracking-server-overview/index.html#method-1-start-your-own-mlflow-server) running to use the awesome features of Mlflow.

Let's use docker-compose.

### 1) Fire up Container ✨

Let's fire up your container first.

In this directory (where the readme.md you reading is), open a terminal and run:

```bash
docker compose up
```

Depending on whether or not you have the images in your local system, this may take some time. No worries!

In a new terminal, run:

```bash
watch -n 1 docker ps -a
```

To monitor that your Mlflow container are up and running!

### 2) Wait Until Mlflow is UP! 📜

In the terminal where you ran `docker compose up`, wait until you see:

```bash
mlflowContainer  | [YOUR_LOCAL_TIME] [30] [INFO] Listening at: http://0.0.0.0:5000 (30)
```

Now you are ready to use Mlflow!

### 3) Run the Spark Application

We have used the `--packages` option before, while we are submitting some Spark Applications, like [WordCountToPostgres](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/WordCountToPostgres.scala).

Let's use `--jars` this time!

We are going to be using the [Mlflow Client](https://mvnrepository.com/artifact/org.mlflow/mlflow-client/2.14.1) and we can download it as a jar to configure it as a `--jars` while we are submitting our application.

First, download the jar from MVN Repository. You can use the link above, or [this one works too](https://repo1.maven.org/maven2/org/mlflow/mlflow-client/2.14.1/). 

The jar is called: `mlflow-client-2.14.1.jar`.

Second, let's get a jar for our Spark Application with `sbt clean package` and submit the application with:

```bash

cd /your/directory/forprojectroot

spark-submit --jars /home/sezai/Downloads/mlflow-client-2.14.1.jar --class learningSpark.MlflowSimple /home/sezai/IdeaProjects/learningspark/target/scala-2.12/learningspark_2.12-0.1.0-SNAPSHOT.jar
```

Change the jar name/path according to your filesystem!

### 4) Let's see the results! 🎈

You can simply open a web browser and see all the results for the experiment & run!

If you click on the latest run, you will see an Overview, Model Metrics, Artifacts and more!