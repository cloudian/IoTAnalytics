import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.jfree.data.time.TimeSeriesCollection;
import org.jfree.data.xy.DefaultXYDataset;
import org.jfree.data.xy.XYDataset;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;
import scala.Tuple2;

import java.sql.Time;
import java.sql.Timestamp;

import java.util.*;
import java.text.SimpleDateFormat;
import java.awt.geom.Point2D;

import org.jfree.data.time.TimeSeries;
import org.jfree.data.time.Day;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.plot.CombinedDomainXYPlot;
import org.jfree.chart.axis.AxisLocation;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.axis.DateAxis;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.renderer.xy.StandardXYItemRenderer;
import org.jfree.chart.renderer.xy.XYItemRenderer;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.ChartFrame;
import org.jfree.data.statistics.Regression;
import org.jfree.chart.ChartPanel;
import javax.swing.JPanel;

public class DataManager {

    // the full input data file is at s3://us-east-1.elasticmapreduce.samples/flightdata/input

    /*
      You are free to change the contents of main as much as you want. We will be running a separate main for
       grading purposes.
     */
    public static final int StationName = 0;
    public static final int StationLocation = 1;
    public static final int DateTime = 2;
    public static final int RoadSurfaceTemperature = 4;
    public static final int AirTemperature = 5;
    public static final int EventDate = 1;
    public static final int Longitude = 2;
    public static final int Latitude = 3;

    public static void main(String[] args) throws Exception {

        Logger.getLogger("org.apache.spark").setLevel(Level.ERROR);
        Logger.getLogger("org.apache.spark.storage.BlockManager").setLevel(Level.ERROR);

        String userAccessKey = "00c36f16c2600f70ae60";
        String userSecretKey = "XsSbmCIfcYrX5NdCBj7n1QSaU2lhdgDJJBDlT7VE";

        String endpoint ="tims4.mobi-cloud.com:80";
        String bucketName = "analytics";
        String conditionObjectName = "edited3.csv";
        String accidentObjectName = "accident_data.csv";
        //String conditionObjectName = "road-weather-information-stations.csv";
        String s3SparkConditionPath = "s3a://" + bucketName + "/" + conditionObjectName;
        String s3SparkAccidentPath = "s3a://" + bucketName + "/" + accidentObjectName;
        boolean csv = true;

        //local spark
        SparkSession spark = SparkSession.builder().
                appName("road_analytics").
                config("spark.master", "local").
                getOrCreate();

        // SparkSession spark = SparkSession.builder().appName("road_analytics").getOrCreate()

        spark.sparkContext().hadoopConfiguration().set("fs.s3a.endpoint", endpoint);
        spark.sparkContext().hadoopConfiguration().set("fs.s3a.access.key", userAccessKey);
        spark.sparkContext().hadoopConfiguration().set("fs.s3a.secret.key", userSecretKey);
        spark.sparkContext().hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        spark.sparkContext().hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false");

        // load data sets from hyperstore
        StructType conditionSchema = new StructType(new StructField[] {
                new StructField("StationName", DataTypes.StringType, true, Metadata.empty()),
                new StructField("StationLocation", DataTypes.StringType, true, Metadata.empty()),
                new StructField("DateTime", DataTypes.TimestampType, true, Metadata.empty()),
                new StructField("RecordId", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("RoadSurfaceTemperature", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("AirTemperature", DataTypes.DoubleType, true, Metadata.empty())});
        StructType accidentSchema = new StructType(new StructField[] {
                new StructField("EventId", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Date", DataTypes.StringType, true, Metadata.empty()),
                new StructField("Longitude", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("Latitude", DataTypes.DoubleType, true, Metadata.empty())});

        Dataset<Row> df = getDataFrame(spark, csv, s3SparkConditionPath, conditionSchema);
        Dataset<Row> conditions = queryImportantData(spark, df);


        Dataset<Row> accidents = getDataFrame(spark, csv, s3SparkAccidentPath, accidentSchema);
        accidents.limit(10).show();
        //For grabbing data locally from computer
        //Dataset<Row> accidents = spark2.read().format("csv").option("header", true).schema(accidentSchema).csv("../seattle_data/accident_data.csv");

        // separate the condition data by location
        HashMap<String, Cluster> clusters = createClusters(spark, conditions);
        System.out.println(clusters.size() + " cluster size");
        separateAccidents(accidents, clusters);
        List<GraphableCluster> graphData = aggregateData(spark, clusters);


        for (GraphableCluster cluster : graphData) {
            //TimeSeries roadTemps = new TimeSeries("Road Temperature");
            //TimeSeries airTemps = new TimeSeries("Air Temperature");
            //TimeSeries incidents = new TimeSeries("Accidents");
            //SimpleDateFormat roadFormat = new SimpleDateFormat("yyyy-MM-dd");
            //SimpleDateFormat accidentFormat = new SimpleDateFormat("MM/dd/yy");
            XYSeries incidents = new XYSeries("Time of Day");
            XYSeries roadTemps = new XYSeries("Road Temperature");
            XYSeries airTemps = new XYSeries("Air Temperature");


            //Create subplot for weather data;
            final XYSeriesCollection weather = new XYSeriesCollection();
            for (Tuple2<String, Tuple2<Double, Double>> roadDataPoint : cluster.getRoadData()) {
                //Date date = roadFormat.parse(roadDataPoint._1);
                //Day day = new Day(date);
                int hour = Integer.parseInt(roadDataPoint._1);
                double roadSurfaceTemp = roadDataPoint._2._1;
                double airTemp = roadDataPoint._2._2;
                roadTemps.add(hour, roadSurfaceTemp);
                airTemps.add(hour, airTemp);

            }
            weather.addSeries(roadTemps);
            weather.addSeries(airTemps);

            final XYItemRenderer renderer1 = new StandardXYItemRenderer();
            final NumberAxis rangeAxis1 = new NumberAxis("Temperature (Fahrenheit)");
            final XYPlot subplot1 = new XYPlot(weather, null, rangeAxis1, renderer1);
            subplot1.setRangeAxisLocation(AxisLocation.BOTTOM_OR_LEFT);

            //Create subplot for accident data
            for (Tuple2<String, Integer> accidentDataPoint : cluster.getAccidentData()) {
                //Date date = accidentFormat.parse(accidentDataPoint._1);
                int hour = Integer.parseInt(accidentDataPoint._1);
                //date.setYear(117);
                //System.out.println(date);
                int numAccidents = accidentDataPoint._2;
                //incidents.add(new Day(date), numAccidents);
                incidents.add(hour, numAccidents);
            }

            final XYItemRenderer renderer2 = new StandardXYItemRenderer();
            final NumberAxis rangeAxis2 = new NumberAxis("Number of Accidents");
            final XYPlot subplot2 = new XYPlot(new XYSeriesCollection(incidents), null, rangeAxis2, renderer2);
            subplot2.setRangeAxisLocation(AxisLocation.BOTTOM_OR_LEFT);

            final NumberAxis domainAxis = new NumberAxis("Hour of Day");
            final CombinedDomainXYPlot plot = new CombinedDomainXYPlot(domainAxis);
            plot.setGap(10.0);

            // add the subplots...
            plot.add(subplot1, 1);
            plot.add(subplot2, 1);
            plot.setOrientation(PlotOrientation.VERTICAL);

            JFreeChart chart = new JFreeChart("Conditions at "+cluster.stationName,
                    JFreeChart.DEFAULT_TITLE_FONT, plot, true);
            ChartFrame frame = new ChartFrame("Results", chart);
            frame.pack();
            frame.setVisible(true);

            //JFreeChart chart2 = new JFreeChart("Accidents at "+cluster.stationName,
              //      JFreeChart.DEFAULT_TITLE_FONT, subplot2, true);
            //ChartFrame frame2 = new ChartFrame("Results", chart2);
            //frame2.pack();
            //frame2.setVisible(true);
            //final XYDataset airSet = new TimeSeriesCollection(airTemps);  
            //final XYDataset accidentSet = new TimeSeriesCollection(incidents);


            //System.out.println(cluster.getAccidentData().toString());
            //System.out.println(cluster.getRoadData().toString());

        }

        // shut down
        spark.stop();
    }


    private static Dataset<Row> getDataFrame (SparkSession spark, boolean csv, String s3SparkPath, StructType customSchema) {
        Dataset<Row> df;
        if (csv) {
            df = spark.read().
                    format("csv").
                    option("header", true).
                    schema(customSchema).
                    csv(s3SparkPath);
        } else {
            df = spark.read().json(s3SparkPath);
        }
        return df;
    }

    private static Dataset<Row> queryImportantData (SparkSession spark, Dataset<Row> df) {
        df.createOrReplaceTempView("Roads");
        String query = "SELECT * " +
                "FROM Roads " +
                "WHERE DateTime BETWEEN '2017-01-01 00:00:00' AND '2017-12-31 23:59:59'";
                //"WHERE DateTime BETWEEN '2014-03-03 12:42:00' AND '2014-03-03 12:48:59'";

        Dataset<Row> r = spark.sql(query);
        r.show();
        return r;
    }

    private static HashMap<String, Cluster> createClusters(SparkSession spark, Dataset<Row> conditions) {
        //conditions.createOrReplaceTempView("Roads");
        //Dataset<Row> locations = spark.sql("SELECT DISTINCT StationName, StationLocation FROM Roads");
        Iterator<Row> iterator = conditions.toLocalIterator();
        HashMap<String, Cluster> clusters = new HashMap<String, Cluster>();
        while (iterator.hasNext()) {
            Row row = iterator.next();
            String locationName = (String)row.get(StationName);
            if (!clusters.containsKey(locationName)) {
                System.out.println("Created new location " +  locationName);
                String loc = (String)row.get(StationLocation);
                String[] station = loc.split(",");
                String strLong = station[1].split(":")[1].replaceAll("[ \'{}]", "");
                String strLat = station[2].split(":")[1].replaceAll("[ \'{}]", "");
                double longitude = Double.parseDouble(strLong);
                double latitude = Double.parseDouble(strLat);
                Point2D.Double point = new Point2D.Double(longitude, latitude);
                clusters.put(locationName, new Cluster(locationName, point));
            }
            clusters.get(locationName).addConditionData(row);
        }
        return clusters;
    }

    private static void separateAccidents (Dataset<Row> df, HashMap<String, Cluster> clusters) {
        Iterator<Row> iterator = df.toLocalIterator();
        while (iterator.hasNext()) {
            Row row = iterator.next();

            double longitude = (Double)row.get(Longitude);
            double latitude = (Double)row.get(Latitude);
            Point2D.Double point = new Point2D.Double(longitude, latitude);
            String closestLocation = "";
            double closestDistance = Double.MAX_VALUE;
            //System.out.println("Point : " + point);
            for (String locName : clusters.keySet()) {
                double distance = point.distance(clusters.get(locName).getLocation());
                if (distance < closestDistance) {
                    closestDistance = distance;
                    closestLocation = locName;
                }
            }
            //String closestLocation = "AuroraBridge";
            System.out.println("Accident data went to " + closestLocation);
            clusters.get(closestLocation).addAccidentData(row);
            System.out.println(row.get(EventDate));
        }
    }

    private static List<GraphableCluster> aggregateData (SparkSession spark, HashMap<String, Cluster> clusters) {
        List<GraphableCluster> graphData = new ArrayList<>();
        List<String > times = Arrays.asList("06", "07", "08", "09", "10", "16", "17", "18", "19", "20");
        for (String clusterName : clusters.keySet()) {
            GraphableCluster gCluster = new GraphableCluster(clusterName);
            Cluster cluster = clusters.get(clusterName);
            JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
            JavaRDD<Row> rddC = jsc.parallelize(cluster.getConditions());
            JavaPairRDD<String, Tuple2<Double, Double>> rddC2 = rddC
                    //.filter(r -> times.contains(r.get(DateTime).toString().substring(11, 13)))
                    .mapToPair(r -> new Tuple2<>(r.get(DateTime).toString().substring(11,13),
                    new Tuple2<>(new Tuple2<>((Double) r.get(RoadSurfaceTemperature), 1), new Tuple2<>((Double) r.get(AirTemperature), 1))))
                    .reduceByKey((r,s) -> new Tuple2<>(new Tuple2<>(r._1._1 + s._1._1, r._1._2 + s._1._2), new Tuple2<>(r._2._1 + s._2._1, r._2._2 + s._2._2)))
                    .mapToPair(r -> new Tuple2<>(r._1, new Tuple2<>((1.0 * r._2._1._1 / r._2._1._2), (1.0 * r._2._2._1 / r._2._2._2))));
            //rdd2.repartition(1).saveAsTextFile("tempfile " + clusterName);
            List<Tuple2<String, Tuple2<Double, Double>>> conditionData = rddC2.collect();
            gCluster.setRoadData(conditionData);

            JavaRDD<Row> rddA = jsc.parallelize(cluster.getAccidents());
            JavaPairRDD<String, Integer> rddA2 = rddA.mapToPair(r -> new Tuple2<>(r.get(EventDate).toString()
                    .substring(r.get(EventDate).toString().indexOf(" ")+1, r.get(EventDate).toString().indexOf(":")), 1)).reduceByKey((r,s) -> r + s);
            List<Tuple2<String, Integer>> accidentData = rddA2.collect();
            gCluster.setAccidentData(accidentData);
            graphData.add(gCluster);
        }
        return graphData;
    }

    private static class Cluster {
        private String stationName;
        private Point2D.Double location;
        private ArrayList<Row> conditions;
        private ArrayList<Row> accidents;

        public Cluster (String name, Point2D.Double loc) {
            stationName = name;
            location = loc;
            conditions = new ArrayList<>();
            accidents = new ArrayList<>();
        }
        public String getName() {
            return stationName;
        }
        public Point2D.Double getLocation() {
            return location;
        }
        public void addConditionData(Row row) {
            conditions.add(row);
        }
        public void addAccidentData(Row row) {
            accidents.add(row);
        }
        public ArrayList<Row> getConditions() {
            return conditions;
        }
        public ArrayList<Row> getAccidents() {
            return accidents;
        }
    }

    private static class GraphableCluster {
        public String stationName;
        private List<Tuple2<String, Tuple2<Double, Double>>> roadData;
        private List<Tuple2<String, Integer>> accidentData;

        public GraphableCluster (String name) {
            stationName = name;
            roadData = null;
            accidentData = null;
        }

        public void setRoadData (List<Tuple2<String, Tuple2<Double, Double>>> data) {
            roadData = data;
        }
        public void setAccidentData (List<Tuple2<String, Integer>> data) {
            accidentData = data;
        }
        public List<Tuple2<String, Tuple2<Double, Double>>> getRoadData () {
            return roadData;
        }
        public List<Tuple2<String, Integer>> getAccidentData () {
            return accidentData;
        }
    }
}
