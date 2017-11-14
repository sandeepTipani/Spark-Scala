package sparkdemo.sparkScala
import java.sql.DriverManager

case class EmployeeCommission(
    firstname:String,lastname:String,salary:Double,commission:Double
    )
    {
  override def toString():String ={
    "firstname" + firstname + ":" + "lastname" +lastname +":" +"commission_Amount" +":" +getCommissionAmount
  }
  
  def getCommissionAmount :Any ={
    if (commission==null)
    {
      "N/A"
    }
    else salary * commission
    
  }
    }

object JDBCConnection {
  def main (args:Array [String])={
    val driver ="com.mysql.jdbc.Driver"
    val url="jdbc://localhost:3306/hr"
    val uname="root"
    val pwd="cloudera"
    
    Class.forName(driver);
    val connection=DriverManager.getConnection(url,uname,pwd)
    val statement=connection.createStatement()
    val resultSet=statement.executeQuery("select * from employees")
    
    /*while(resultSet.next()){
      val result =EmployeeCommission(resultSet.getString("firstname"),resultSet.getString("lasttname"),
          resultSet.getDouble("salary"),resultSet.getDouble("commission"))
          
          println(result)
          }
          */
  //Iterator converts the Tuple into a collection so that you apply the map function to process..instead of using the traditional WHILE
    Iterator.continually(resultSet,resultSet.next).
  takeWhile(rec=>rec._2).
  map(_._1).map(rec=>{
    EmployeeCommission(resultSet.getString("firstname"),resultSet.getString("lasttname"),
          resultSet.getDouble("salary"),resultSet.getDouble("commission"))
  }).foreach(rec=>{
    println(rec)
  })
 }
}