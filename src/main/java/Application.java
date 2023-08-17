import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.expressions.Encode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.awt.*;
import java.util.Arrays;
import java.util.Iterator;

public class Application {
    public static void main(String[] args) throws StreamingQueryException {
       System.setProperty("hadoop.home.dir","C:\\hadoop-common-2.2.0-bin-master");

        SparkSession sparkSession = SparkSession.builder().appName("SparkStreamingMessageListener").master("local").getOrCreate();

        //streamm olduğu için readstream dedik. iletişim kanalıda port üzerinden sürekli dinleyen soket ile olacakğı için formatımız soket.
        //option olarak socetin ip sini ve portunu veriyoruz
        // sonra dinleme portunu veriyoruz
        // ve bunları rawdata datasetine yazacak
        Dataset<Row> rawData = sparkSession.readStream().format("socket").option("host", "localhost").option("port", "8005").load();

        // encodermantığı ile raw datayı strignge çeviriyoruz.Neden virazdan flat map yapacağız string üzerinde daha kolay olduğu için
        Dataset<String> data = rawData.as(Encoders.STRING());

        //mesajdaki herbir satırı string alarak tutuluyor biz bunu boşluğa göre parse edip kelimelerini saymamız lazım
        //çünkü uygulşamamız kelime sayıcı

        Dataset<String> stringDataset = data.flatMap(new FlatMapFunction<String, String>() {

            public Iterator<String> call(String s) throws Exception {

                //S --> satırın tamamı bunu boşluğa söre böleceğiz
                //Array listesi olarak dön bunun içinde s leri boşluklara göre ayır.
                return Arrays.asList(s.split(" ")).iterator();
            }
        }, Encoders.STRING());

        // keliemeleri grupladık
         //sayısını öğrenmek için
        Dataset<Row> groupedData = stringDataset.groupBy("value").count();

        //stream olarak ekrena yaz
        //ne zaman yaz analiz tamamlanınca "complete"
        //nereye consola
        StreamingQuery start = groupedData.writeStream().outputMode("complete").format("console").start();

        //ben kapatana kadar çalış
        start.awaitTermination();


    }
}
