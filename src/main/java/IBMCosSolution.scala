import org.apache.hadoop.fs.LocalFileSystem
import org.apache.hadoop.hdfs.DistributedFileSystem
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;

import org.apache.spark.sql.SaveMode
import java.util.Properties

object IBMCosSolution {
  val inputpath="C:/Users/Rahul_Pandey4/eclipse-workspacegg/IBMCos/src/main/resources/emp-data.csv"
  val outputpath="cos://candidate-exercise.myCos/empdataoutput.parquet"
  
import org.apache.spark.sql.types._
     val schema = new StructType()
      .add("Name",StringType,true)
      .add("Gender",StringType,true)
      .add("Department",StringType,true)
      .add("Salary",StringType,true)
      .add("Loc",StringType,true)
      .add("Rating",StringType,true)

//1.	Function that connects to IBM Cloud Object Store (COS)
 def setHadoopConfigurationForIBMCOS(sparkSession: SparkSession, path: String) = {
 try{
  sparkSession.sparkContext.hadoopConfiguration.set("fs.stocator.scheme.list", "cos")
  sparkSession.sparkContext.hadoopConfiguration.set("fs.cos.impl", "com.ibm.stocator.fs.ObjectStoreFileSystem")
  sparkSession.sparkContext.hadoopConfiguration.set("fs.stocator.cos.impl", "com.ibm.stocator.fs.cos.COSAPIClient")
  sparkSession.sparkContext.hadoopConfiguration.set("fs.stocator.cos.scheme", "cos")
  sparkSession.sparkContext.hadoopConfiguration.set("fs.cos.myCos.access.key", "0aba66146f3b450cacebaa908046d17e")
  sparkSession.sparkContext.hadoopConfiguration.set("fs.cos.myCos.secret.key", "27b804de3b329a680dbf148fd76da208f33e8a5aaaea4cbd")
  sparkSession.sparkContext.hadoopConfiguration.set("fs.cos.myCos.endpoint", "http://s3-api.us-geo.objectstorage.softlayer.net")
  sparkSession.sparkContext.hadoopConfiguration.set("fs.cos.myCos.v2.signer.type", "false")
  sparkSession.sparkContext.hadoopConfiguration.set("fs.cos.myCos.create.bucket","true")
//val df=sparkSession.read.csv(inputpath)
//df.printSchema()
 }catch{  
            case e: Exception => println(e.printStackTrace)  
   }  
  }
 
  //2.	Function that reads a CSV file (emp-data.csv) from the COS bucket
  def readFromCOS(sparkSession: SparkSession, inputpath: String ): DataFrame = {
    import sparkSession.implicits._
    val readdf=sparkSession.read.format("csv").option("header", "true").schema(schema).load(inputpath)
    readdf.show()
    readdf
}

/*
4.	Write Scala code to:
a.	Create a table based COS data schema read in Step 2
b.	Write the contents from Step 2 to the table 
  */
def db2Writedataframe(sparkSession: SparkSession, databaseURL: String, dbtable: String,readdf:DataFrame) = {

 val connectionProperties=new Properties
 connectionProperties.put( "user","postgres")
 connectionProperties.put("password", "admin")
 connectionProperties.put("jdbcUrl","jdbc:postgresql://localhost:5432/training")
 connectionProperties.put("jdbcDriver", "org.postgresql.Driver")
 connectionProperties.put("dbname","trining")
readdf.printSchema()
val table="empdata2"
val username = "postgres";
val password = "admin";
val url ="jdbc:postgresql://localhost:5432/training"
Class.forName("org.postgresql.Driver")
val saveMode=SaveMode.Append
val df=readdf.write.mode(saveMode).jdbc(url, table, connectionProperties)

 }

/*
 5.	Write Scala code to read the same data from the database, calculate and display the following:
a.	Gender ratio in each department
b.	Average salary in each department
c.	Male and female salary gap in each department
 * 
 * 
 */
def readDataBaseDF(sparkSession: SparkSession ,path:String): DataFrame = {
   // read the same data from the database 
   val db2Connect = sparkSession.read
  .format("jdbc")
  .option("url", "jdbc:postgresql://localhost:5432/training")
  .option("dbtable", "empdata2")
  .option("user", "postgres")
  .option("password", "admin")
  .option("driver", "org.postgresql.Driver")
  .load()
    
    import sparkSession.implicits._

    //5.	Write Scala code to read the same data from the database, calculate and display the following:
    val ratioDepartmentDf=db2Connect.registerTempTable("empdata")
   //a.	Gender ratio in each department
    val genderratiodf=sparkSession.sql("select distinct(Department), Gender, count(*) over( partition by Department, Gender) cnt from empdata order by Department desc, Gender desc")
    genderratiodf.show()

    // b.	Average salary in each department
   val avgSalary_df=sparkSession.sql("SELECT DEPARTMENT,avg(cast(REPLACE(REPLACE(salary, '$', ''), ',', '') as decimal)) AVGSalary FROM empdata group by DEPARTMENT")
   avgSalary_df.show()
    
   //c.	Male and female salary gap in each department
   val salarygapDF=sparkSession.sql("select m.department,coalesce(male_avg_slary-female_avg_slary,0) AS field_alias from (select department,avg(cast(REPLACE(REPLACE(salary, '$', ''), ',', '') as decimal)) male_avg_slary from empdata where gender='Male' group by department) m full outer join (select department,avg(cast(REPLACE(REPLACE(salary, '$', ''), ',', '') as decimal)) as female_avg_slary from empdata where gender='Female' group by department) f on m.department=f.department")
   salarygapDF.show()
   
   //6.	Scala code to write one the calculated data as a Parquet back to COS
  
    avgSalary_df.write.parquet(outputpath)
       
    genderratiodf

  }
  def main(args: Array[String]): Unit = {
        val sparkSession = SparkSession
          .builder()
          .appName("IBMExample")
          .master("local[*]")
          .getOrCreate()
          
val connectIBMCos=setHadoopConfigurationForIBMCOS(sparkSession,inputpath)
 val readdf=  readFromCOS(sparkSession,inputpath)   
 val dbconnectex=db2Writedataframe(sparkSession,"","",readdf)
 val filteredf= readDataBaseDF(sparkSession,"")

  }
  }
