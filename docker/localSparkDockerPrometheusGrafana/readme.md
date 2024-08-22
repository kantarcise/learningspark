# Monitoring Spark Streaming Applications, with Prometheus and Grafana 🥳

Apart from the Web UI we get which is included in a Spark Setup, we can also monitor Spark applications with Prometheus and Grafana.

[Prometheus](https://prometheus.io/) is an open-source monitoring and alerting toolkit designed for recording real-time metrics in a time-series database, with a powerful query language for analyzing those metrics.

And [Grafana](https://grafana.com/oss/) is an open-source analytics and monitoring platform that provides customizable dashboards and visualizations for understanding complex data metrics, often used in conjunction with data sources like Prometheus.

## Boo! Can you make it relatable?

Sure, here is a wonderful metaphor from GPT4o!

### Prometheus

**Metaphor**: **The Watchful Gardener**
- 
- **Definition**: Imagine Prometheus as a diligent gardener who constantly monitors every plant in a huge garden. This gardener checks on the plants regularly, recording their growth, health, and any issues like pests or wilting. If something starts going wrong, the gardener takes note and raises an alert so the garden can be tended to quickly. In the world of technology, Prometheus is like this gardener, but instead of plants, it watches over servers, applications, and other systems, collecting and storing data about their performance and health.

### Grafana

**Metaphor**: **The Artful Display Artist**
- 
- **Definition**: Think of Grafana as an artist who takes all the data the gardener (Prometheus) has collected and turns it into beautiful, easy-to-understand pictures. This artist uses charts, graphs, and dashboards to display the garden's health and growth in a way that anyone can understand at a glance. In the tech world, Grafana takes the data collected by tools like Prometheus and turns it into visual dashboards that help people see how their systems are doing, spot problems, and understand trends over time.

## Okay, how can we setup these for Spark ?

I'm glad you asked, here is a step-by-step approach for it!

### 1) Configure Spark to Be Running In Standalone Mode

While we are at it, let's not just monitor a single Spark Application running in Local mode, lets set up a cluster!

Let's do some configurations:

First, get to the directory where you extracted Spark. This might be:

```bash
cd /usr/local/spark/
```

Get to the Configuration directory:

```bash
cd /conf
```

We are going to change 3 different files here.

**First**, copy the `workers.template` file to make `workers`

```bash
cp workers.template workers
```
Open the file and comment out `localhost`, this way we are telling Spark that we want some workers in this host!

The end of the `workers` file should look like this:

```markdown
# A Spark Worker will be started on each of the machines listed below.
localhost
```

**Second**, copy the `spark-env.sh.template` file to make the `spark-env.sh`:

```bash
cp spark-env.sh.template spark-env.sh
```

Under the `Options for the daemons used in the standalone deploy mode` you will see some example options, commented out for us.

Let's Tell spark what kind of worker we want by typing:

```
SPARK_WORKER_CORES=2
SPARK_WORKER_MEMORY=2g
```

Now we have a configuration for a single Worker in our local pc. Save and exit.

**Third** copy the `metrics.properties.template` file to make the `metrics.properties` (you now understand how it works 🥰 ):

At the very end of this file, comment out the lines where we are given an example Prometheus setup for Metric Sink!

```
# Example configuration for PrometheusServlet
*.sink.prometheusServlet.class=org.apache.spark.metrics.sink.PrometheusServlet
*.sink.prometheusServlet.path=/metrics/prometheus
master.sink.prometheusServlet.path=/metrics/master/prometheus
applications.sink.prometheusServlet.path=/metrics/applications/prometheus
driver.sink.prometheusServlet.path=/metrics/driver/prometheus
```

After all of this, now we can start our Spark Cluster in Standalone Mode! 💙

### 2) Start a Spark Cluster

Now we get to use some cool `.sh` files to start a Spark Cluster!

Under `/sbin` let's first start the master:

```bash
./start-master.sh
```

After the Master starts you can see it in `localhost:8080` You will see the Spark Master Host and Port in the Top Right!

Now we can start our worker! We should specify the master host and port for the worker, and we'll do that just like a parameter.

```bash
./start-worker YOUR_SPARK_MASTER_HOST_AND_PORT(SOMETHING LIKE - spark://some_ip:7077)
```

Now we can submit some Spark Applications for our cluster and see all the metrics for it!

### 3) Start the Containers 🌱

- Before starting the containers, you need to change one value inside `prometheus.yml`, which is your local IP.

So change the IP of the host from `192.168.5.10` to your local IP. You local IP can be found with:

```bash
ifconfig
```

- After you changed `prometheus.yml`, open a terminal in this directory (where the readme.md you reading is) and run:

```bash
docker compose up
```

Depending on whether you have the images in your local system, this may take some time. No worries!

In a new terminal, run:

```bash
watch -n 1 docker ps -a
```

To monitor that your Prometheus and Grafana containers are up and running!

You can see the endpoints becoming accessible in Prometheus: `http://localhost:9090/targets?search=`

At this point 3 out of 5 targets should be up! 

After we submit our application, the **spark-driver** and **spark-executor** will be up too!


### 4) Submit the Jar and See the Action! 🏉

Just like in [WordCountKafka](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/WordCountKafka.scala) get a jar with `sbt clean package` and submit the application with:

```bash
./bin/spark-submit --master spark://sezai:7077 --class learningSpark.MultipleStreamingQueriesApp /home/sezai/IdeaProjects/learningspark/target/scala-2.12/learningspark_2.12-0.1.0-SNAPSHOT.jar
```

In this command, change the master `host:port` according to your setup.

### 5) Connect Prometheus and Grafana & Make A Visualization in Grafana

- Open up Grafana at: `http://localhost:3000/` Your credentials are **username:** `admin` - **password:** `admin`.

- In the homepage on the right, you'll see somewhere saying "Add your first data source". That is where we'll connect Prometheus to Grafana.

- Select Prometheus, give the server url as `http://prometheus:9090/` and **save & test**, all the way down in the page. 

- Now we can make a Dashboard using Spark endpoints! With the help of an LLM, you can design your own dashboard, or import the [one we have here]() as a starter!

Have fun visualizing! 🎉