import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{desc, lit, max}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession}

object maxSalary {

  def main (args : Array[String] ) : Unit = {

    val spark_conf = new SparkConf().setAppName("Read File").setMaster("local")
    val sc = new SparkContext(spark_conf)
    val emp_csv = "src/main/resources/employee.csv"
    val dept_csv = "src/main/resources/department.csv"

    val spark = SparkSession.builder()
      .config(spark_conf)
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    val empdf = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "True")
      .load(emp_csv)

    val deptdf = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "True")
      .load(dept_csv)

    println("\nDataFrame : print employee with max salary")
    println("------------------------------------------")

    empdf.withColumn("max_p", max("Sal").over(Window.partitionBy()))
      .where($"Sal" === $"max_p")
      .drop("max_p","DeptNo")
      .show()

    //empdf.orderBy(desc("Sal")).take(1)
    val emp_dept= empdf.join(deptdf).where(empdf("DeptNo") === deptdf("DeptNo")).drop(empdf("DeptNo"))
    //emp_dept.show
    //emp_dept.printSchema()

    val dept_max = emp_dept.groupBy("DeptNo","DeptName")
                            .sum("Sal").orderBy(desc("sum(Sal)"))

    println("\nDataFrame : show dept no and dept name consuming the max salary")
    println("-------------------------------------------------------------- ")
    dept_max.show()

    println("\nRDD: Employee with Max Salary ")
    println("----------------------------- ")

    val emp = sc.textFile(emp_csv)
      .mapPartitionsWithIndex( (idx, row) => if(idx==0) row.drop(1) else row )
      .map(x => ((x.split(",")(0),x.split(",")(1).toString), x.split(",")(2).toInt))

    val maxSal = emp.reduceByKey(((acc,value)=> acc+value )).sortBy(_._2, ascending = false).first()

    println(maxSal)
   // maxSal.collect().foreach(println)

    println("\nRDD: DeptNo and Department Name with Max Salary ")
    println("----------------------------------------------- ")

    val emprdd = sc.textFile(emp_csv)
      .mapPartitionsWithIndex( (idx, row) => if(idx==0) row.drop(1) else row )

    val deptrdd = sc.textFile(dept_csv)
      .mapPartitionsWithIndex( (idx, row) => if(idx==0) row.drop(1) else row )

    val emppairRdd = emprdd.map(line => line.split(",").map(elem => elem.trim)).map(r => (r(3), r))
    val deptpairRdd = deptrdd.map(line => line.split(",").map(elem => elem.trim)).map(r => (r(0), r))
    val joined = emppairRdd.join(deptpairRdd)
    val deptmaxSal=joined.map( {case (x,y) => ((x,y._2(1)),y._1(2).toInt)}).reduceByKey(((acc,value)=> acc+value )).sortBy(_._2, ascending = false)

    deptmaxSal.foreach(println)

    println("\nSpark SQL: Employee with Max Salary ")
    println("------------------------------------- ")

    empdf.createOrReplaceTempView("v_employee")
    deptdf.createOrReplaceTempView("v_department")

    val empMaxSal= spark.sql("select fname,lname, sal as max_sal from v_employee where sal= (select max(sal) from v_employee)")

    empMaxSal.show()

    println("\nSpark SQL: Department Total Salary ")
    println("------------------------------------- ")

    val deptTotalSal= spark.sql("select d.deptno, d.deptname, sum(e.sal) as total_sal from v_employee e, v_department d where e.deptno=d.deptno group by d.deptno, d.deptname order by 3 desc")

    deptTotalSal.show()




  }

}
