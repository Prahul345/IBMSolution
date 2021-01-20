


object DB2Connect {
  /*
   *
 3.	Setup a DB2 database
a.	Setup an account in IBM Public Cloud (cloud.ibm.com)
b.	Create a simple DB2 database
   * 
   */
 def connectURL (): java.sql. Connection  = {
 val url  =  "jdbc:db2://dashdb-txn-sbox-yp-lon02-02.services.eu-gb.bluemix.net:50000/BLUDB"
val username =  "gpc37490"    
val password =  "b812qtjjfktg@p9n"
// load the driver and acquire connection.    
Class. forName ( "com.ibm.db2.jcc.DB2Driver" )   // needed   only for db2jcc.jar, not db2jcc4.jar
var connection = java.sql.DriverManager. getConnection ( url ,  username ,  password )
 connection  
     
 }
def main ( args : Array[String]) {
println ( "obtaining connection..." )
val con : java.sql. Connection  =  connectURL ()
println ( "connection obtained." ) 
//throwing error while creating table as I dont have permission to create table
//var st  =  con . createStatement ()
//var rs  =  st . executeQuery ( "CREATE TABLE employee ( Name CHAR(50),Gender CHAR(10),Department CHAR(20),Salary CHAR(20),Loc CHAR(20),Rating CHAR(5) ) ORGANIZE BY COLUMN " )

//rs . close ()       
//st . close ()
}
}