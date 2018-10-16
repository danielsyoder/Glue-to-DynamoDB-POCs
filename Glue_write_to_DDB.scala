%spark

//Scala Glue - Zeppelin Notebook
//POC is based on Amazon Reviews public dataset

import com.amazonaws.services.glue.ChoiceOption
import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.MappingSpec
import com.amazonaws.services.glue.ResolveSpec
import com.amazonaws.services.glue.errors.CallSite
import com.amazonaws.services.glue.util.GlueArgParser
import com.amazonaws.services.glue.util.Job
import com.amazonaws.services.glue.util.JsonOptions
import org.apache.spark.SparkContext
import scala.collection.JavaConverters._

//Configure the emr-dynamodb-connector 
import org.apache.hadoop.io.Text;
import org.apache.hadoop.dynamodb.DynamoDBItemWritable
import com.amazonaws.services.dynamodbv2.model.AttributeValue
import org.apache.hadoop.dynamodb.read.DynamoDBInputFormat
import org.apache.hadoop.dynamodb.write.DynamoDBOutputFormat
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.io.LongWritable
import java.util.HashMap
import org.apache.spark.rdd.RDD 
import org.apache.spark.sql.Row
import org.apache.hadoop.mapred.FileOutputCommitter

val glueContext:GlueContext = new GlueContext(sc)

//Addresses DirectOutConnector error
sc.hadoopConfiguration.set("mapred.output.committer.class", "org.apache.hadoop.mapred.FileOutputCommitter")

//Set DynamoDB tables, region, and r/w percent
var ddbConf = new JobConf(sc.hadoopConfiguration)
ddbConf.set("dynamodb.servicename", "dynamodb")
ddbConf.set("dynamodb.endpoint", "dynamodb.us-east-1.amazonaws.com")
ddbConf.set("dynamodb.regionid", "us-east-1")
ddbConf.set("dynamodb.input.tableName", "InputTableName")
ddbConf.set("dynamodb.output.tableName", "OutputTableName")
ddbConf.set("dynamodb.throughput.write.percent", "1.1")
ddbConf.set("dynamodb.throughput.read.percent", "1.1")
ddbConf.set("mapred.input.format.class", "org.apache.hadoop.dynamodb.read.DynamoDBInputFormat")
ddbConf.set("mapred.output.format.class", "org.apache.hadoop.dynamodb.write.DynamoDBOutputFormat")

//Set source table as any Glue indexed table
val datasource0 = glueContext.getCatalogSource(database = "default", tableName = "GlueTableCleaned", redshiftTmpDir = "", transformationContext = "datasource0").getDynamicFrame()

//Map each column (cannot have null items)
val applymapping1 = datasource0.applyMapping(mappings = Seq(("marketplace", "string", "marketplace", "string"), ("customer_id", "string", "customer_id", "string"), ("review_id", "string", "review_id", "string"), ("product_id", "string", "product_id", "string"), ("product_parent", "string", "product_parent", "string"), ("product_title", "string", "product_title", "string"), ("star_rating", "int", "star_rating", "string"), ("helpful_votes", "int", "helpful_votes", "string"), ("total_votes", "int", "total_votes", "string"), ("vine", "string", "vine", "string"), ("verified_purchase", "string", "verified_purchase", "string"), ("rev_head", "string", "review_headline", "string"), ("rev_body", "string", "review_body", "string"), ("review_date", "date", "review_date", "string"), ("year", "int", "year", "string"), ("product_category", "string", "product_category", "string")), caseSensitive = false, transformationContext = "applymapping1")
val resolvechoice2 = applymapping1.resolveChoice(choiceOption = Some(ChoiceOption("make_struct")), transformationContext = "resolvechoice2")
val dropnullfields3 = resolvechoice2.dropNulls(transformationContext = "dropnullfields3")

val DynamoDF = dropnullfields3.toDF()
//DynamoDF.show() --> uncomment to sample DataFrame table after mapping

//Below creates a DynamoDB Item-Attribute map from the RDD
var ddbInsertFormattedRDD = DynamoDF.rdd.map(a => {
var ddbMap = new HashMap[String, AttributeValue]()

var marketplace = new AttributeValue()
marketplace.setS(a.get(0).toString)
ddbMap.put("marketplace", marketplace)

var customer_id = new AttributeValue()
customer_id.setS(a.get(1).toString)
ddbMap.put("customer_id", customer_id)

var review_id = new AttributeValue()
review_id.setS(a.get(2).toString)
ddbMap.put("review_id", review_id)

var product_id = new AttributeValue()
product_id.setS(a.get(3).toString)
ddbMap.put("product_id", product_id)

var product_parent = new AttributeValue()
product_parent.setS(a.get(4).toString)
ddbMap.put("product_parent", product_parent)

var product_title = new AttributeValue()
product_title.setS(a.get(5).toString)
ddbMap.put("product_title", product_title)

var star_rating = new AttributeValue()
star_rating.setN(a.get(6).toString)
ddbMap.put("star_rating", star_rating)

var helpful_votes = new AttributeValue()
helpful_votes.setN(a.get(7).toString)
ddbMap.put("helpful_votes", helpful_votes)

var total_votes = new AttributeValue()
total_votes.setN(a.get(8).toString)
ddbMap.put("total_votes", total_votes)

var vine = new AttributeValue()
vine.setS(a.get(9).toString)
ddbMap.put("vine", vine)

var verified_purchase = new AttributeValue()
verified_purchase.setS(a.get(10).toString)
ddbMap.put("verified_purchase", verified_purchase)

var review_headline = new AttributeValue()
review_headline.setS(a.get(11).toString)
ddbMap.put("review_headline", review_headline)

var review_body = new AttributeValue()
review_body.setS(a.get(12).toString)
ddbMap.put("review_body", review_body)

var review_date = new AttributeValue()
review_date.setS(a.get(13).toString)
ddbMap.put("review_date", review_date)

var year = new AttributeValue()
year.setN(a.get(14).toString)
ddbMap.put("year", year)

var product_category = new AttributeValue()
product_category.setS(a.get(15).toString)
ddbMap.put("product_category", product_category)

var item = new DynamoDBItemWritable()
item.setItem(ddbMap)

(new Text(""), item)

}
)

//Write to DynamoDB
ddbInsertFormattedRDD.saveAsHadoopDataset(ddbConf)

