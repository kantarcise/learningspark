## Do we have to type the schema? 🤔

In [BloggersDataset](https://github.com/kantarcise/learningspark/blob/main/src/main/scala/BloggersDataset.scala) we have a code block like :

```scala
// Make a Dataset by reading from the JSON file
// We are using the case class we wrote for it!
val blogsDS = spark
  .read
  // another way to get the schema, from a case class!
  .schema(implicitly[Encoder[BlogWriters]].schema)
  .json(bloggersFilePath)
  .as[BlogWriters]
```

### Detailed Explanation

This code block is designed to read a JSON file and convert it into a Dataset of `BlogWriters` case class instances in Apache Spark. Let's break down each part to understand what is happening:

#### 1. Reading the JSON file
```scala
val blogsDS = spark
  .read
  .json(bloggersFilePath)
```

- `spark.read`: This is the entry point for reading data in Spark. It returns a `DataFrameReader` object.
- `.json(bloggersFilePath)`: The `json` method reads a JSON file from the specified path (`bloggersFilePath`) and returns a DataFrame. A DataFrame in Spark is a dataset organized into named columns.

#### 2. Setting the Schema Using a Case Class
```scala
.schema(implicitly[Encoder[BlogWriters]].schema)
```

- **Case Class**: `BlogWriters` is a case class that defines the structure of the data you expect to read from the JSON file.
  
  ```scala
  case class BlogWriters(Id: Int,
                       First: String,
                       Last: String,
                       Url: String,
                       Published: String,
                       Hits: Int,
                       Campaigns: Seq[String]
                      )
  ```

- **Encoder**: In Spark, an `Encoder` is responsible for converting between JVM objects (instances of `BlogWriters`) and Spark's internal binary format. [Encoders](https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/Encoder.html) are essential for converting data between Datasets and DataFrames.
  
- **implicitly**: `implicitly[Encoder[BlogWriters]]` is a Scala function that retrieves an implicit `Encoder` for the `BlogWriters` case class from the current scope. An implicit `Encoder` is typically provided by importing the appropriate Spark SQL implicits.
  
- **schema**: The `schema` method on the `Encoder` returns a `StructType`, which is a Spark SQL data type representing a row and its columns. This `StructType` describes the structure of the `BlogWriters` case class.

By calling `.schema(implicitly[Encoder[BlogWriters]].schema)`, you are explicitly setting the schema of the DataFrame being read from the JSON file to match the schema of the `BlogWriters` case class. This ensures that Spark interprets the structure of the JSON data according to the `BlogWriters` schema.

#### 3. Converting the DataFrame to a Dataset
```scala
.as[BlogWriters]
```

- The `as[BlogWriters]` method converts the DataFrame into a Dataset of `BlogWriters` instances. A Dataset is a distributed collection of data where each element is an object of the specified case class (`BlogWriters` in this case).

### Summary

- **Reading JSON File**: The JSON file is read into a DataFrame.
- **Setting Schema**: The schema of the DataFrame is explicitly set using the schema of the `BlogWriters` case class obtained through an implicit `Encoder`.
- **Converting to Dataset**: The DataFrame is converted into a Dataset of `BlogWriters` instances.

This process ensures that the data read from the JSON file is correctly interpreted and mapped to the `BlogWriters` case class, allowing you to work with a strongly-typed Dataset in Spark.

Thank you GPT-4o!