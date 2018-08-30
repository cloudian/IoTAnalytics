/*
Kodiak Conrad
Cloudian Internship Analytics Project
August 2018
 */

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.FileInputStream;
import java.io.InputStream;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import scala.Tuple2;
import java.sql.Timestamp;
import java.time.DayOfWeek;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.SignStyle;
import java.util.*;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
import static java.time.temporal.ChronoField.*;
import java.time.LocalDate;

import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.plot.CombinedDomainXYPlot;
import org.jfree.chart.axis.AxisLocation;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.renderer.xy.StandardXYItemRenderer;
import org.jfree.chart.renderer.xy.XYItemRenderer;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.ChartFrame;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;
import java.awt.Color;

public class DataManager {

    private static final int DATA_TIME = 2;
    private static final int ROAD_SURFACE_TEMP = 4;
    private static final int AIR_TEMP = 5;
    private static final int EVENT_DATE = 1;

    public static void main(String[] args) throws Exception {
        Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
        Logger.getLogger("org.apache.spark.storage.BlockManager").setLevel(Level.ERROR);

        String userAccessKey;
        String userSecretKey;

        String endpoint;
        String bucketName;
        String conditionObjectName;
        String accidentObjectName;

        Properties props = new Properties();
        InputStream input = new FileInputStream("config.properties");
        props.load(input);
        try {
            userAccessKey = props.getProperty("accessKey");
            userSecretKey = props.getProperty("secretKey");
            endpoint = props.getProperty("endpoint");
            bucketName = props.getProperty("bucket");
            conditionObjectName = props.getProperty("conditionObject");
            accidentObjectName = props.getProperty("accidentObject");

        } catch (Exception e) {
            System.out.println("config.properties file not set up properly");
            throw e;
        }
        String s3SparkConditionPath = "s3a://" + bucketName + "/" + conditionObjectName;
        String s3SparkAccidentPath = "s3a://" + bucketName + "/" + accidentObjectName;

        SparkSession spark = SparkSession.builder().
                appName("road_analytics").
                config("spark.master", "local").
                getOrCreate();

        spark.sparkContext().hadoopConfiguration().set("fs.s3a.endpoint", endpoint);
        spark.sparkContext().hadoopConfiguration().set("fs.s3a.access.key", userAccessKey);
        spark.sparkContext().hadoopConfiguration().set("fs.s3a.secret.key", userSecretKey);
        spark.sparkContext().hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        spark.sparkContext().hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false");

        StructType conditionSchema = new StructType(new StructField[]{
                new StructField("StationName", DataTypes.StringType, true, Metadata.empty()),
                new StructField("StationLocation", DataTypes.StringType, true, Metadata.empty()),
                new StructField("DateTime", DataTypes.TimestampType, true, Metadata.empty()),
                new StructField("RecordId", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("RoadSurfaceTemperature", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("AirTemperature", DataTypes.DoubleType, true, Metadata.empty())});
        StructType accidentSchema = new StructType(new StructField[]{
                new StructField("EventId", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Date", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Longitude", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("Latitude", DataTypes.DoubleType, true, Metadata.empty())});

        Dataset<Row> df = spark.read()
                .format("csv")
                .option("header", true)
                .schema(conditionSchema)
                .csv(s3SparkConditionPath);
        Dataset<Row> conditions = queryImportantData(spark, df);

        Dataset<Row> accidents = spark.read()
                .format("csv")
                .option("header", true)
                //.option("inferSchema", true)
                .schema(accidentSchema)
                .csv(s3SparkAccidentPath);

        HashMap<DayOfWeek, Cluster> daysOfWeek = createClusters(spark, conditions);
        separateAccidents(accidents, daysOfWeek);
        List<GraphableCluster> graphData = aggregateData(spark, daysOfWeek);
        showGraph(graphData);

        spark.stop();
    }

    //Simplifies dataset to only include recirdings during timeframe for which we have accident data.
    private static Dataset<Row> queryImportantData(SparkSession spark, Dataset<Row> df) {
        df.createOrReplaceTempView("Roads");
        String query = "SELECT * " +
                "FROM Roads " +
                "WHERE DateTime BETWEEN '2017-01-01 00:00:00' AND '2017-12-31 23:59:59'";

        Dataset<Row> r = spark.sql(query);
        return r;
    }

    //Separates all rows of temperature recordings based on the day of week they were taken.
    private static HashMap<DayOfWeek, Cluster> createClusters(SparkSession spark, Dataset<Row> conditions) {
        Iterator<Row> iterator = conditions.toLocalIterator();
        HashMap<DayOfWeek, Cluster> clusters = new HashMap<>();
        SimpleDateFormat weekdayFormat = new SimpleDateFormat("EEEE");
        while (iterator.hasNext()) {
            Row row = iterator.next();
            LocalDate date = ((Timestamp)row.get(DATA_TIME)).toLocalDateTime().toLocalDate();
            DayOfWeek day = date.getDayOfWeek();
            if (!clusters.containsKey(day)) {
                clusters.put(day, new Cluster(day));
            }
            clusters.get(day).addConditionData(row);
        }
        return clusters;
    }

    //Separates accidents based on day of week which they were recorded.
    private static void separateAccidents(Dataset<Row> df, HashMap<DayOfWeek, Cluster> clusters) throws Exception {
        Iterator<Row> iterator = df.toLocalIterator();
        DateTimeFormatter accidentFormat = new DateTimeFormatterBuilder()
                .appendValue(MONTH_OF_YEAR, 1,  2, SignStyle.NEVER).appendLiteral("/")
                .appendValue(DAY_OF_MONTH, 1,2, SignStyle.NEVER).appendLiteral("/")
                .appendValue(YEAR).appendLiteral(" ")
                .appendValue(HOUR_OF_DAY).appendLiteral(":")
                .appendValue(MINUTE_OF_HOUR).toFormatter();
        while (iterator.hasNext()) {
            Row row = iterator.next();
            String strDate = (String) row.get(EVENT_DATE);
            LocalDate date = LocalDate.parse(strDate, accidentFormat);
            clusters.get(date.getDayOfWeek()).addAccidentData(row);
        }
    }

    //Averages out the temperature by hour for each day of the week.
    //Accumulates the number of accidents occurring per hour for each day of the week.
    private static List<GraphableCluster> aggregateData(SparkSession spark, HashMap<DayOfWeek, Cluster> clusters) {
        List<GraphableCluster> graphData = new ArrayList<>();
        for (DayOfWeek clusterName : clusters.keySet()) {
            GraphableCluster gCluster = new GraphableCluster(clusterName);
            Cluster cluster = clusters.get(clusterName);
            JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
            JavaRDD<Row> rddC = jsc.parallelize(cluster.getConditions());
            JavaPairRDD<String, Tuple2<Double, Double>> rddC2 = rddC
                    .mapToPair(r -> new Tuple2<>(r.get(DATA_TIME).toString().substring(11, 13),
                            new Tuple2<>(new Tuple2<>((Double) r.get(ROAD_SURFACE_TEMP), 1), new Tuple2<>((Double) r.get(AIR_TEMP), 1))))
                    .reduceByKey((r, s) -> new Tuple2<>(new Tuple2<>(r._1._1 + s._1._1, r._1._2 + s._1._2), new Tuple2<>(r._2._1 + s._2._1, r._2._2 + s._2._2)))
                    .mapToPair(r -> new Tuple2<>(r._1, new Tuple2<>((1.0 * r._2._1._1 / r._2._1._2), (1.0 * r._2._2._1 / r._2._2._2))));
            List<Tuple2<String, Tuple2<Double, Double>>> conditionData = rddC2.collect();
            gCluster.setRoadData(conditionData);

            JavaRDD<Row> rddA = jsc.parallelize(cluster.getAccidents());
            JavaPairRDD<String, Integer> rddA2 = rddA
                    .mapToPair(r -> new Tuple2<>(r.get(EVENT_DATE).toString().substring(r.get(EVENT_DATE).toString().indexOf(" ") + 1, r.get(EVENT_DATE).toString().indexOf(":")), 1))
                    .reduceByKey((r, s) -> r + s);
            List<Tuple2<String, Integer>> accidentData = rddA2.collect();
            gCluster.setAccidentData(accidentData);
            graphData.add(gCluster);
        }
        return graphData;
    }

    //Graphs the data for each day of the week on a shared domain
    private static void showGraph(List<GraphableCluster> graphData) {

        final XYSeriesCollection accidents = new XYSeriesCollection();
        final XYSeriesCollection roadWeather = new XYSeriesCollection();
        final XYSeriesCollection airWeather = new XYSeriesCollection();

        Collections.sort(graphData);
        Color[] colors = {Color.BLUE, Color.GREEN, Color.MAGENTA, Color.RED, Color.YELLOW, Color.CYAN, Color.GRAY};
        for (GraphableCluster cluster : graphData) {
            XYSeries incidents = new XYSeries(cluster.dayOfWeek.toString());
            XYSeries roadTemps = new XYSeries(cluster.dayOfWeek.toString());
            XYSeries airTemps = new XYSeries(cluster.dayOfWeek.toString());

            for (Tuple2<String, Tuple2<Double, Double>> roadDataPoint : cluster.getRoadData()) {
                int hour = Integer.parseInt(roadDataPoint._1);
                double roadSurfaceTemp = roadDataPoint._2._1;
                double airTemp = roadDataPoint._2._2;
                roadTemps.add(hour, roadSurfaceTemp);
                airTemps.add(hour, airTemp);
            }
            for (Tuple2<String, Integer> accidentDataPoint : cluster.getAccidentData()) {
                int hour = Integer.parseInt(accidentDataPoint._1);
                int numAccidents = accidentDataPoint._2;
                incidents.add(hour, numAccidents);
            }
            accidents.addSeries(incidents);
            roadWeather.addSeries(roadTemps);
            airWeather.addSeries(airTemps);
        }

        final XYItemRenderer renderer1 = new StandardXYItemRenderer();
        final NumberAxis rangeAxis1 = new NumberAxis("Air Temperature (Fahrenheit)");
        final XYItemRenderer renderer2 = new StandardXYItemRenderer();
        final NumberAxis rangeAxis2 = new NumberAxis("Road Temperature (Fahrenheit)");
        final XYItemRenderer renderer3 = new StandardXYItemRenderer();
        final NumberAxis rangeAxis3 = new NumberAxis("Number of Accidents");
        for (int i = 0; i < 7; i ++) {
            renderer1.setSeriesVisibleInLegend(i, false);
            renderer2.setSeriesVisibleInLegend(i, false);
            renderer1.setSeriesPaint(i, colors[i]);
            renderer2.setSeriesPaint(i, colors[i]);
            renderer3.setSeriesPaint(i, colors[i]);
        }

        final XYPlot subplot1 = new XYPlot(airWeather, null, rangeAxis1, renderer1);
        subplot1.setRangeAxisLocation(AxisLocation.BOTTOM_OR_LEFT);
        final XYPlot subplot2 = new XYPlot(roadWeather, null, rangeAxis2, renderer2);
        subplot2.setRangeAxisLocation(AxisLocation.BOTTOM_OR_LEFT);

        final XYPlot subplot3 = new XYPlot(accidents, null, rangeAxis3, renderer3);
        subplot3.setRangeAxisLocation(AxisLocation.BOTTOM_OR_LEFT);

        final NumberAxis domainAxis = new NumberAxis("Hour of Day");
        final CombinedDomainXYPlot plot = new CombinedDomainXYPlot(domainAxis);
        plot.setGap(10.0);

        plot.add(subplot1, 1);
        plot.add(subplot2, 1);
        plot.add(subplot3, 1);
        plot.setOrientation(PlotOrientation.VERTICAL);

        JFreeChart chart = new JFreeChart("Conditions By Day of Week",
                JFreeChart.DEFAULT_TITLE_FONT, plot, true);
        ChartFrame frame = new ChartFrame("Results", chart);
        frame.pack();
        frame.setVisible(true);
    }

    //Inner class for holding rows of data after they have been separated.
    private static class Cluster {
        private DayOfWeek day;
        private ArrayList<Row> conditions;
        private ArrayList<Row> accidents;

        private Cluster(DayOfWeek weekDay) {
            day = weekDay;
            conditions = new ArrayList<>();
            accidents = new ArrayList<>();
        }

        private void addConditionData(Row row) {
            conditions.add(row);
        }

        private void addAccidentData(Row row) {
            accidents.add(row);
        }

        private ArrayList<Row> getConditions() {
            return conditions;
        }

        private ArrayList<Row> getAccidents() {
            return accidents;
        }
    }

    //Inner class for holding lists of graphable information after spark queries have been run.
    private static class GraphableCluster implements Comparable<GraphableCluster> {
        private DayOfWeek dayOfWeek;
        private List<Tuple2<String, Tuple2<Double, Double>>> roadData;
        private List<Tuple2<String, Integer>> accidentData;

        private GraphableCluster(DayOfWeek day) {
            dayOfWeek = day;
            roadData = null;
            accidentData = null;
        }

        private void setRoadData(List<Tuple2<String, Tuple2<Double, Double>>> data) {
            roadData = data;
        }

        private void setAccidentData(List<Tuple2<String, Integer>> data) {
            accidentData = data;
        }

        private List<Tuple2<String, Tuple2<Double, Double>>> getRoadData() {
            return roadData;
        }

        private List<Tuple2<String, Integer>> getAccidentData() {
            return accidentData;
        }
        @Override
        public int compareTo(GraphableCluster other) {
            return this.dayOfWeek.compareTo(other.dayOfWeek);
        }
    }
}


