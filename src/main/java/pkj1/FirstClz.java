package pkj1;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.*;
import org.apache.spark.sql.SparkSession.Builder;
import static org.apache.spark.sql.functions.*;
public class FirstClz {

	public void move() {
		SparkSession sparkSession=new Builder()
				.appName("testName")
				.master("local")
				.getOrCreate();
		
		Dataset<Row> df=sparkSession.read().format("csv")
				.option("header", true)
				.load("C:\\Users\\Taha513\\Desktop\\ali.csv");
		//df=df.withColumn("Area_count", concat(df.col("Area") , lit(","), df.col("count")));
		//df=df.filter(df.col("Issuing_Bank").rlike("al")).orderBy(df.col("CVV/CVV2").desc()); //regular expretion
		df.show(100);
	}
}
