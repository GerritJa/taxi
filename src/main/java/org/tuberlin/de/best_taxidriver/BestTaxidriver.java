package org.tuberlin.de.best_taxidriver;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.tuberlin.de.geodata.MapCoordToDistrict;
import org.tuberlin.de.read_data.Taxidrive;

/**
 * Created by gerrit on 11/6/15.
 */
public class BestTaxidriver {
    public static void main(String[] args) throws Exception {
        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        String baseOutputPath = "hdfs:///results/eval_bestDriver/";
        String taxiDatasetPath = "hdfs:///TaxiData/sorted_data.csv";
        String districtsPath = "hdfs:///data/ny_districts.csv"; //local : "data/districts"


        if (args.length == 3) {
            taxiDatasetPath = args[0]; // local :"data/testData.csv"
            districtsPath = args[1]; //local : "data/districts"
            baseOutputPath = args[2]; //local : "data/eval_bestDriver/"
        }


        // load taxi data from csv-file
        /*DataSet<Taxidrive> taxidrives = env.readCsvFile(taxiDatasetPath)
                .pojoType(Taxidrive.class,
                        "taxiID",
                        "licenseID",
                        "pickup_datetime",
                        "dropoff_datetime",
                        "trip_time_in_secs",
                        "trip_distance",
                        "pickup_longitude",
                        "pickup_latitude",
                        "dropoff_longitude",
                        "dropoff_latitude",
                        "payment_type",
                        "fare_amount",
                        "surcharge",
                        "mta_tax",
                        "tip_amount",
                        "tolls_amount",
                        "total_amount");*/


        DataSet<Taxidrive> taxidrives = MapCoordToDistrict.readData(env, taxiDatasetPath, districtsPath);

        taxidrives = taxidrives.filter(new FilterFunction<Taxidrive>() {
            @Override
            public boolean filter(Taxidrive taxidrive) throws Exception {
                return taxidrive.getTrip_time_in_secs() > 0;
            }
        });

        DataSet<Statistic> statisticsPerDrive = taxidrives.map(new DriveToDriverStatMapper());

        DataSet<Statistic> overallStatisticDataset = statisticsPerDrive.reduce(new StatReducer());
        overallStatisticDataset.writeAsText(baseOutputPath + "overallStat.txt");

        DataSet<Statistic> statsByDriver = createDriverStatistics(taxidrives);

        statsByDriver = statsByDriver
                .sortPartition("total_amount_afterTax", Order.DESCENDING)
                .setParallelism(1);

        statsByDriver.first(10).writeAsText(baseOutputPath + "bestDriverStat.txt");


        Statistic bestDriver = statsByDriver.first(1).collect().get(0);
        //String bestDriverId = "DB1B4490DA4A46A7A9A009DF6A503675";
        String bestDriverId = bestDriver.licenseID;

       /* System.out.println("best driver stat " + bestDriver.toString() + "\n" +
                "overallStatistic " + overallStatistic.toString());*/


        DataSet<Taxidrive> bestDriverTrips = taxidrives.filter(new FilterFunction<Taxidrive>() {
            @Override
            public boolean filter(Taxidrive taxidrive) throws Exception {
                return taxidrive.licenseID.equals(bestDriverId);
            }
        });


        calculateAmountDistributionByDayAndTime(taxidrives, "data/eval_bestDriver/amount_distribution_all.csv");
        calculateAmountDistributionByDayAndTime(bestDriverTrips, "data/eval_bestDriver/amount_distribution_bestDriver.csv");

        calculateAmountDistributionByTime(taxidrives, "data/eval_bestDriver/amount_distribution_all_byTime.csv");
        calculateAmountDistributionByWeekday(taxidrives, "data/eval_bestDriver/amount_distribution_all_byDay.csv");
        calculateAmountDistributionByDistrict(taxidrives, "data/eval_bestDriver/amount_distribution_all_byDistrict.csv");

        calculateAmountDistributionByTime(bestDriverTrips, "data/eval_bestDriver/amount_distribution_bestDriver_byTime.csv");
        calculateAmountDistributionByWeekday(bestDriverTrips, "data/eval_bestDriver/amount_distribution_bestDriver_byDay.csv");
        calculateAmountDistributionByDistrict(bestDriverTrips, "data/eval_bestDriver/amount_distribution_bestDriver_byDistrict.csv");


        env.execute("Driver Highscore");
    }


    private static void calculateAmountDistributionByDistrict(DataSet<Taxidrive> taxidrives, String outputpath) {
        taxidrives.map(new MapFunction<Taxidrive, Tuple6<Integer, Integer, Integer, String, Double, String>>() {
            @Override
            public Tuple6<Integer, Integer, Integer, String, Double, String> map(Taxidrive taxidrive) throws Exception {
                return new Tuple6<Integer, Integer, Integer, String, Double, String>(
                        1,
                        getDateTime(taxidrive.getPickup_datetime()).getDayOfWeek(),
                        getDateTime(taxidrive.getPickup_datetime()).getHourOfDay(),
                        taxidrive.getPickupNeighborhood(),
                        taxidrive.getTotal_amount() - taxidrive.mta_tax - taxidrive.tolls_amount,
                        taxidrive.getPickupBorough()
                );
            }
        }).groupBy(3, 5)
                .aggregate(Aggregations.SUM, 0)
                .and(Aggregations.SUM, 4)
                .sortPartition(2, Order.ASCENDING)
                .setParallelism(1)
                .writeAsCsv(outputpath);
    }

    private static void calculateAmountDistributionByTime(DataSet<Taxidrive> taxidrives, String outputpath) {
        taxidrives.map(new MapFunction<Taxidrive, Tuple5<Integer, Integer, Integer, String, Double>>() {
            @Override
            public Tuple5<Integer, Integer, Integer, String, Double> map(Taxidrive taxidrive) throws Exception {
                return new Tuple5<Integer, Integer, Integer, String, Double>(
                        1,
                        getDateTime(taxidrive.getPickup_datetime()).getDayOfWeek(),
                        getDateTime(taxidrive.getPickup_datetime()).getHourOfDay(),
                        taxidrive.getPickupNeighborhood(),
                        taxidrive.getTotal_amount() - taxidrive.mta_tax - taxidrive.tolls_amount
                );
            }
        }).groupBy(2)
                .aggregate(Aggregations.SUM, 0)
                .and(Aggregations.SUM, 4)
                .sortPartition(2, Order.ASCENDING)
                .setParallelism(1)
                .writeAsCsv(outputpath);
    }

    private static void calculateAmountDistributionByWeekday(DataSet<Taxidrive> taxidrives, String outputpath) {
        taxidrives.map(new MapFunction<Taxidrive, Tuple5<Integer, Integer, Integer, String, Double>>() {
            @Override
            public Tuple5<Integer, Integer, Integer, String, Double> map(Taxidrive taxidrive) throws Exception {
                return new Tuple5<Integer, Integer, Integer, String, Double>(
                        1,
                        getDateTime(taxidrive.getPickup_datetime()).getDayOfWeek(),
                        getDateTime(taxidrive.getPickup_datetime()).getHourOfDay(),
                        taxidrive.getPickupNeighborhood(),
                        taxidrive.getTotal_amount() - taxidrive.mta_tax - taxidrive.tolls_amount
                );
            }
        }).groupBy(1)
                .aggregate(Aggregations.SUM, 0)
                .and(Aggregations.SUM, 4)
                .sortPartition(1, Order.ASCENDING)
                .setParallelism(1)
                .writeAsCsv(outputpath);
    }


    private static void calculateAmountDistributionByDayAndTime(DataSet<Taxidrive> taxidrives, String outputpath) {
        taxidrives.map(new MapFunction<Taxidrive, Tuple5<Integer, Integer, Integer, String, Double>>() {
            @Override
            public Tuple5<Integer, Integer, Integer, String, Double> map(Taxidrive taxidrive) throws Exception {
                return new Tuple5<Integer, Integer, Integer, String, Double>(
                        1,
                        getDateTime(taxidrive.getPickup_datetime()).getDayOfWeek(),
                        getDateTime(taxidrive.getPickup_datetime()).getHourOfDay(),
                        taxidrive.getPickupNeighborhood(),
                        taxidrive.getTotal_amount() - taxidrive.mta_tax - taxidrive.tolls_amount
                );
            }
        }).groupBy(1, 2)
                .aggregate(Aggregations.SUM, 0)
                .and(Aggregations.SUM, 4)
                .sortPartition(1, Order.ASCENDING)
                .sortPartition(2, Order.ASCENDING)
                .setParallelism(1)
                .writeAsCsv(outputpath);

    }


    private static DataSet<Statistic> createDriverStatistics(DataSet<Taxidrive> taxidrives) throws Exception {
        return taxidrives.map(new DriveToDriverStatMapper())
                .groupBy("licenseID")
                .reduce(new StatReducer());
    }


    public static final class DriveToDriverStatMapper implements MapFunction<Taxidrive, Statistic> {
        @Override
        public Statistic map(Taxidrive taxidrive) throws Exception {
            Statistic statistic = new Statistic();
            statistic.countOfTrips = 1;
            statistic.licenseID = taxidrive.licenseID;
            statistic.trip_distanceSum = taxidrive.getTrip_distance();
            statistic.tip_amountSum = taxidrive.getTip_amount();
            statistic.total_amountSum = taxidrive.getTotal_amount();
            statistic.tolls_amountSum = taxidrive.getTolls_amount();
            statistic.fare_amount = taxidrive.fare_amount;
            statistic.tripTimeSum_inSecs = taxidrive.trip_time_in_secs;
            statistic.mta_tax = taxidrive.mta_tax;
            statistic.total_amount_afterTax = statistic.getTotalAmountAfterTax();

            DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
            DateTime dateTime = formatter.parseDateTime(taxidrive.getPickup_datetime());
            statistic.weekdays[dateTime.getDayOfWeek() - 1] = 1;
            statistic.daytime[dateTime.getHourOfDay()] = 1;
            return statistic;
        }
    }

    public static final class StatReducer implements ReduceFunction<Statistic> {
        @Override
        public Statistic reduce(Statistic d1, Statistic d2) throws Exception {
            if (!d1.licenseID.equals(d2.licenseID)) d1.licenseID = "not available";
            d1.tip_amountSum += d2.tip_amountSum;
            d1.countOfTrips += d2.countOfTrips;
            d1.tolls_amountSum += d2.tolls_amountSum;
            d1.fare_amount += d2.fare_amount;
            d1.tip_amountSum += d2.tip_amountSum;
            d1.total_amountSum += d2.total_amountSum;
            d1.tripTimeSum_inSecs += d2.tripTimeSum_inSecs;
            d1.mta_tax += d2.mta_tax;
            d1.total_amount_afterTax = d1.getTotalAmountAfterTax();
            d1.mergeWeekdays(d2.weekdays);
            d1.mergeDaytime(d2.daytime);
            return d1;
        }
    }

    private static DateTime getDateTime(String s) {
        DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
        DateTime dateTime = formatter.parseDateTime(s);
        return dateTime;
    }
}
