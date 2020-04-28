package com.personal.spark;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

public class RestCaller {

    // TODO: This could be splitted into many functions making it reusable.
    // i.e. a function for reading urls, one for creating spark session, one for reading df from a url...
    // right now is the most basic form of reading a showing. 
    public void restConsumer(){
        try {

            // Spark session creation
            SparkSession session= SparkSession.builder()
                    .appName("Basic Spark2")
                    .enableHiveSupport()
                    .master("local[*]")
                    .getOrCreate();

            // We read the url from the web
            URL url = new URL("https://datos.madrid.es/egob/catalogo/300107-0-agenda-actividades-eventos.csv");
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            if (conn.getResponseCode() != 200) {
                throw new RuntimeException("Failed : HTTP Error code : "
                        + conn.getResponseCode());
            }

            // We use basic JAVA i/o to "read the file" and transform it
            // into something "SPARK READABLE (a row in this case)"
            InputStreamReader in = new InputStreamReader(conn.getInputStream());
            BufferedReader br = new BufferedReader(in);
            String output;

            List<Row> listRows = new ArrayList<Row>();
            List<StructField> listFields = new ArrayList<StructField>();
            List<String[]> anomalies = new ArrayList<String[]>();


            for (String s :  br.readLine().split(";")){
                System.out.println(s);
                if(s.length()>1) // Prevention from random "chars" at header
                    listFields.add(DataTypes.createStructField(s, DataTypes.StringType,true));
            }

            StructType str = DataTypes.createStructType(listFields);

            while ((output = br.readLine()) != null) {
                String[] array = output.split(";");
                // Post replace as if we do it early we risk breaking the
                // dataset info, this way we have empty fields instead of ""
                for (int i= 0; i<array.length;i++)
                    array[i]=array[i].replace("\"","");

                if(array.length>str.size()) {
                    // We check for any anomaly when parsing the dataset
                    // an anomaly is set when probably one of the fields of the the row might have
                    // the "column" separator inside a "column" value (i.e. inside a description).
                    // We save them in case we want to check what's hapenning with it.
                    // A dataset with the anomaly could be something to look on.
                    anomalies.add(array);
                    continue;
                }

                listRows.add(RowFactory.create(array));
            }


            Dataset ds = session.createDataFrame(listRows, str);

            ds.show();

            conn.disconnect();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
