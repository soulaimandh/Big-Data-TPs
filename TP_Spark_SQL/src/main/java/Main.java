import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;

import static org.apache.spark.sql.functions.countDistinct;

public class Main {

    public static void main(String[] args) {
        Logger.getLogger("").setLevel(Level.OFF);
        SparkSession ss = SparkSession.builder().master("local[*]").appName("tp spark sql").getOrCreate();

        Map<String,String> options = new HashMap<>();

        options.put("driver","com.mysql.cj.jdbc.Driver");
        options.put("url","jdbc:mysql://localhost:3306/db_hopital");
        options.put("user","root");
        options.put("password","");


        Dataset<Row> df_consultations = ss.read().format("jdbc").options(options).option("dbtable","consultations").load();
        Dataset<Row> df_medecins = ss.read().format("jdbc").options(options).option("dbtable","medecins").load();
        Dataset<Row> df_patients = ss.read().format("jdbc").options(options).option("dbtable","patients").load();


        long rows = df_consultations.count();

        System.out.print("le nombre de consultations Total --> ");
        System.out.println(rows);

        System.out.println("le nombre de consultations par jour : ");
        df_consultations.select("DATE_CONSULTATION").groupBy("DATE_CONSULTATION").count().show();

        Dataset<Row> df2 = df_consultations.join(df_medecins, df_medecins.col("ID").equalTo(df_consultations.col("ID_MEDECIN")), "inner");

        System.out.println("le nombre de consultation par médecin : ");
        System.out.println("version Sprk");
        df2.groupBy("NOM","PRENOM").count().withColumnRenamed("count", "NOMBRE DE CONSULTATION").show();
        System.out.println("version SQL");
        ss.read().format("jdbc").options(options).option("query","SELECT m.nom, m.prenom, COUNT(*) AS 'NOMBRE DE CONSULTATION' FROM medecins m INNER JOIN consultations c ON m.id = c.id_medecin GROUP BY m.nom, m.prenom").load().show();


        System.out.println("le nombre de patients par médecin : ");
        System.out.println("version Sprk");
        df2.groupBy("NOM", "PRENOM").agg(countDistinct("ID_PATIENT").as("NOMBRE DE PATIENTS")).show();
        System.out.println("version SQL");
        ss.read().format("jdbc").options(options).option("query","SELECT m.nom, m.prenom, COUNT(DISTINCT c.id_patient) AS 'NOMBRE DE PATIENTS' FROM consultations c INNER JOIN medecins m ON c.id_medecin = m.id GROUP BY m.nom, m.prenom").load().show();
    }
}