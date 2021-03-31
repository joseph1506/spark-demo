package com.joe.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Function1;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;

public class WordCount {

    public static void count(String fileName){
        SparkConf conf= new SparkConf()
                .setMaster("local")
                .setAppName("Word Counter");

        JavaSparkContext sparkContext= new JavaSparkContext(conf);
        SparkSession sparkSession= new SparkSession.Builder()
                .config(conf)
                .getOrCreate();

        Dataset<Row> df= sparkSession.read().format("org.apache.spark.csv")
                        .option("header",true)
                        .option("inferSchema",false)
                        .csv(fileName);
        /* df.write()
                .format("org.apache.spark.csv")
                .option("header",true)
                .option("inferSchema",true)
                .csv("E:\\prop"); */
        List<String> columns = Arrays.asList("Borough","Block","Lot","TotalUnits","SalePrice");
        Dataset<Row> df2= df.selectExpr(convertListToSeq(columns));
        //Dataset<Row> df2= df.select(col("Borough"));
        /*df2.write()
                .format("org.apache.spark.csv")
                .option("header",true)
                .option("inferSchema",true)
                .csv("E:\\prop1");*/
        df2.show();
        List<StructField> fields = Arrays.asList(df2.schema().fields());
        //fields.add(DataTypes.createStructField("EffectivePrice",DataTypes.IntegerType,true));
        StructField effPrice= DataTypes.createStructField("EffectivePrice",DataTypes.StringType,true);
        List<StructField> newFields= new ArrayList<>();
        newFields.addAll(fields);
        newFields.add(effPrice);


        StructType newSchema= DataTypes.createStructType(newFields);

        Dataset<Row> df3=df2.map((MapFunction<Row, Row>) row-> newFunc(row), RowEncoder.apply(newSchema));
        df3.show();

        /*JavaRDD<String> lines= sparkContext.textFile(fileName);
        Dataset<String> linesDF= */
    }

    public static Seq<String> convertListToSeq(List<String> columns) {
        return JavaConverters.asScalaIteratorConverter(columns.iterator()).asScala().toSeq();
    }

    public static Row newFunc(Row row){
        String totalUnits= row.getAs("TotalUnits");
        String lot= row.getAs("TotalUnits");
        String salesPrice= row.getAs("SalePrice");
        int effPrice= 0;

            if(getValue(totalUnits)>10 && getValue(lot)>30){
                effPrice = getValue(salesPrice)/2;
            } else {
                effPrice = getValue(salesPrice)+100;
            }
            String rowStr= row.mkString("||");
            rowStr= rowStr+"||"+ effPrice;
            String[] items= rowStr.split("\\|\\|");
            return RowFactory.create(items);
    }

    /*private static String[] getItems(String rowStr, String del) {
        StringTokenizer tokenizer= new StringTokenizer(rowStr,del);
        String[] tokens= new String[tokenizer.countTokens()];
        while(tokenizer.hasMoreTokens()){
            tokens[]
        }
        return new String[0];
    }*/

    private static int getValue(String value) {
        return value!=null? Integer.parseInt(value):0;
    }

    private static String getStringValue(int i) {
        return String.valueOf(i);
    }

}
