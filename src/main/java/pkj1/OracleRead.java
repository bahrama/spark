package pkj1;

import java.io.*;
import java.net.StandardSocketOptions;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.sum;

public class OracleRead {
	public void move() {
		SparkSession spark = new SparkSession.Builder()
				.appName("CSV to DB")
				.master("local[*]")
				.getOrCreate();

		String dbConnectionUrl = "jdbc:oracle:thin:@//172.16.100.21:1521/COTA.tahagasht.dc"; // <<- You need to create this database
		//String dbConnectionUrl = "jdbc:oracle:thin:@localhost:1521:orclali"; // <<- You need to create this database
				Properties prop = new Properties();
				prop.setProperty("driver", "oracle.jdbc.driver.OracleDriver");
				prop.setProperty("user", "alikhah");
				//prop.setProperty("user", "hr");
				prop.setProperty("password", "t@h@g@sht");
				//prop.setProperty("password", "hr");
				Dataset<Row> df=spark.read().option("queryTimeout", "100")
    	       .jdbc(dbConnectionUrl, "CADM.T_HOTEL_MAPPING", prop);
                System.out.println(df.count());
                df.show(10);

		String dbConnectionUrlPsq = "jdbc:postgresql://localhost:5432/postgres"; // <<- You need to create this database
		Properties prop2 = new Properties();
		prop2.setProperty("driver", "org.postgresql.Driver");
		prop2.setProperty("user", "postgres");
		prop2.setProperty("password", "ali680313");
		//df.where(df.col("f_supplier").equalTo(62))
		df.write()
				.mode(SaveMode.Overwrite)
				.jdbc(dbConnectionUrlPsq, "T_HOTEL_MAPPING_NEW", prop2);



/*		String dbConnectionUrlPsq = "jdbc:postgresql://localhost:5432/postgres";
		Properties prop2 = new Properties();
		prop2.setProperty("driver", "org.postgresql.Driver");
		prop2.setProperty("user", "postgres");
		prop2.setProperty("password", "ali680313");
		Dataset<Row> df2=spark.read().option("queryTimeout", "100")
				.jdbc(dbConnectionUrlPsq, "t_hotel_mapping", prop2);
		 List<Long> longList = new ArrayList<>();
		 df2.where(col("f_supplier").equalTo(65)).select(col("f_hotel")).orderBy(col("id").desc()).collectAsList().forEach(p->{
			 longList.add(Long.valueOf(p.toString().replace("[","").replace("]","")));
		 });
		 List<String> stringList = new ArrayList<>();
		df2.where(col("f_supplier").equalTo(65)).select(col("c_hotel_code")).orderBy(col("id").desc()).collectAsList().forEach(p->{
			stringList.add(p.toString().replace("[","").replace("]",""));
		});
		Map<String,Long> longStringMap = new HashMap<>();
        Long size =1214L;
        for(int i =0;i<=size;i++){
			longStringMap.put(stringList.get(i),longList.get(i));
		}

		//int i = 90;
		//while (i >= 90 && i < 91) {
			//i++;

				Dataset<Row> df = spark.read().format("csv")
						.option("header", true)
						.load("C:\\Users\\Taha513\\Downloads\\result.csv");
				df.select("id", "city_id", "name", "name_en", "neighbourhood",
								"image/0/url", "image/1/url",
								"image/2/url")
						.withColumnRenamed("image/0/url", "image1_url")
						.withColumnRenamed("image/1/url", "image2_url")
						.withColumnRenamed("image/2/url", "image3_url")
						.show();
				//////////////postgres
				df.foreach((ForeachFunction<Row>) row -> {
							try {
								String foldername = longStringMap.get((String) row.getAs("id")).toString();
								System.out.println((String) row.getAs("id"));
								System.out.println((String) row.getAs("image/0/url"));
								System.out.println((String) row.getAs("image/1/url"));
								System.out.println((String) row.getAs("image/2/url"));
*//*			System.out.println((String) row.getAs("image2_url"));
			System.out.println((String) row.getAs("image3_url"));*//*
								try {

									Path path = Paths.get("D:\\channelead-image\\" + foldername);

									Files.createDirectories(path);

									System.out.println("Directory is created!");

									//Files.createDirectory(path);

								} catch (IOException e) {
									System.err.println("Failed to create directory!" + e.getMessage());
								}

								try {
									Thread.sleep(500);
									URL url2 = new URL(row.getAs("image/0/url"));
									InputStream in = new BufferedInputStream(url2.openStream());
									ByteArrayOutputStream out = new ByteArrayOutputStream();
									byte[] buf = new byte[1024];
									int n = 0;
									while (-1 != (n = in.read(buf))) {
										out.write(buf, 0, n);
									}
									byte[] response = out.toByteArray();
									FileOutputStream fos = new FileOutputStream("D:\\channelead-image\\" + foldername + "\\0.jpg");
									fos.write(response);
									fos.close();
									out.close();
									in.close();
								} catch (Exception e) {
									System.out.println("END");
								}
								try {
									Thread.sleep(500);
									URL url2 = new URL(row.getAs("image/1/url"));
									InputStream in = new BufferedInputStream(url2.openStream());
									ByteArrayOutputStream out = new ByteArrayOutputStream();
									byte[] buf = new byte[1024];
									int n = 0;
									while (-1 != (n = in.read(buf))) {
										out.write(buf, 0, n);
									}
									byte[] response = out.toByteArray();
									FileOutputStream fos = new FileOutputStream("D:\\channelead-image\\" + foldername + "\\1.jpg");
									fos.write(response);
									fos.close();
									out.close();
									in.close();
								} catch (Exception e) {
									System.out.println("END");
								}
								try {
									Thread.sleep(500);
									URL url2 = new URL(row.getAs("image/2/url"));
									InputStream in = new BufferedInputStream(url2.openStream());
									ByteArrayOutputStream out = new ByteArrayOutputStream();
									byte[] buf = new byte[1024];
									int n = 0;
									while (-1 != (n = in.read(buf))) {
										out.write(buf, 0, n);
									}
									byte[] response = out.toByteArray();
									FileOutputStream fos = new FileOutputStream("D:\\channelead-image\\" + foldername + "\\2.jpg");
									fos.write(response);
									fos.close();
									out.close();
									in.close();
								} catch (Exception e) {
									System.out.println("END");
								}
							}catch (Exception e){
								System.out.println("Errrrrrr");
							}
						}
				);*/

		//}

/*			String dbConnectionUrlPsq = "jdbc:postgresql://localhost:5432/postgres"; // <<- You need to create this database
			Properties prop2 = new Properties();
			prop2.setProperty("driver", "org.postgresql.Driver");
			prop2.setProperty("user", "postgres");
			prop2.setProperty("password", "ali680313"); // <- The password you used while installing Postgres

			df
					.select("id", "city_id", "name", "name_en", "neighbourhood",
							"start_location/address/city/code", "start_location/address/city/name",
							"start_location/address/line_1", "start_location/address/line_2",
							"start_location/geo", "start_location/address/city/image/0/url")
					.withColumnRenamed("start_location/address/city/image/0/url", "image_url")
					.withColumnRenamed("start_location/geo", "location")
					.withColumnRenamed("start_location/address/line_2", "address2")
					.withColumnRenamed("start_location/address/line_1", "address1")
					.withColumnRenamed("start_location/address/city/name", "city_name")
					.withColumnRenamed("start_location/address/city/code", "city_code")
					.write()
					.mode(SaveMode.Append)
					.jdbc(dbConnectionUrlPsq, "t_hotel_channelead", prop2);*/
		//}

/*	Dataset<Row> df = spark.read().format("csv")
				.option("header", true)
				.load("D:\\eghamat24\\1.csv");
		df.select("name","id","name_en","city_id","address")
				.show();

		String dbConnectionUrlPsq = "jdbc:postgresql://localhost:5432/postgres";
		Properties prop2 = new Properties();
		prop2.setProperty("driver", "org.postgresql.Driver");
		prop2.setProperty("user", "postgres");
		prop2.setProperty("password", "ali680313");

		df.select("name","id","name_en","city_id","address")
				.write()
				.mode(SaveMode.Append)
				.jdbc(dbConnectionUrlPsq, "eghamat24_hotel", prop2);*/

/*		Dataset<Row> df = spark.read().format("csv")
				.option("header", true)
				.load("D:\\cityma------\\map.csv");
		df.select("city_code","city_name")
				.show();

		String dbConnectionUrlPsq = "jdbc:postgresql://localhost:5432/postgres";
		Properties prop2 = new Properties();
		prop2.setProperty("driver", "org.postgresql.Driver");
		prop2.setProperty("user", "postgres");
		prop2.setProperty("password", "ali680313");

		df.select("city_code","city_name")
				.write()
				.mode(SaveMode.Append)
				.jdbc(dbConnectionUrlPsq, "dreamdays_city", prop2);*/



/*		Dataset<Row> df=spark.read().option("queryTimeout", "100")
				.jdbc(dbConnectionUrl, "T_HOTEL_MAPPING", prop);
		System.out.println(df.count());
		df.show(10);*/

/*		String dbConnectionUrlPsq = "jdbc:postgresql://localhost:5432/postgres";
		Properties prop2 = new Properties();
		prop2.setProperty("driver", "org.postgresql.Driver");
		prop2.setProperty("user", "postgres");
		prop2.setProperty("password", "ali680313");
		Dataset<Row> df=spark.read().option("queryTimeout", "100")
				.jdbc(dbConnectionUrlPsq, "t_city_mapping", prop2);
		//System.out.println(df.count());
		df.show(10);
		String dbConnectionUrl = "jdbc:oracle:thin:@//172.16.101.20:1539/XEPDB1"; // <<- You need to create this database
		Properties prop = new Properties();
		prop.setProperty("driver", "oracle.jdbc.driver.OracleDriver");
		prop.setProperty("user", "tahagasht");
		prop.setProperty("password", "tahagasht");
		df.select( "f_city", "f_supplier", "c_supplier_city").where(df.col("f_supplier").equalTo(62))
				.write()
				.mode(SaveMode.Append)
				.jdbc(dbConnectionUrl, "t_city_mapping", prop);*/
	/////////////
/*		Dataset<Row> df = spark.read().format("csv")
				.option("header", true)
				.load("C:\\Users\\Taha513\\Downloads\\output.csv");
		df.select("Id", "Picture_Url","Name","City_Name","Type_Name","Grade_Name","Chain_Name","State_Name","Address","Hotel_Rank","Rank_Name","City_Id","State_Id","Has_Free_Transfer")
				.show();

	String dbConnectionUrlPsq = "jdbc:postgresql://localhost:5432/postgres";
		Properties prop2 = new Properties();
		prop2.setProperty("driver", "org.postgresql.Driver");
		prop2.setProperty("user", "postgres");
		prop2.setProperty("password", "ali680313");

		df.select("Id", "Picture_Url","Name","City_Name","Type_Name","Grade_Name","Chain_Name","State_Name","Address","Hotel_Rank","Rank_Name","City_Id","State_Id","Has_Free_Transfer")
				.write()
				.mode(SaveMode.Append)
				.jdbc(dbConnectionUrlPsq, "t_iran_hotel", prop2);*/
	}

	}

