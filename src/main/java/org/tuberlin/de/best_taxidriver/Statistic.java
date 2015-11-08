package org.tuberlin.de.best_taxidriver;

import org.apache.flink.api.java.tuple.Tuple11;

import java.util.Arrays;


/**
 * Created by gerrit on 26.10.15.
 */
public class Statistic {
    public String licenseID;
    public int tripTimeSum_inSecs;
    public double trip_distanceSum;
    public double tip_amountSum;
    public double tolls_amountSum;
    public double mta_tax;
    public double fare_amount;
    public double total_amountSum;
    public double total_amount_afterTax;
    public int countOfTrips;

    public int[] weekdays;
    public int[] daytime;


    public Statistic() {
        weekdays = new int[7];
        daytime = new int[24];
    }

    public double getAvgTripTime(){
        return tripTimeSum_inSecs/ countOfTrips;
    }

    public double getAvgTollAmount(){
        return tolls_amountSum/ countOfTrips;
    }

    public double getAvgTotalAmount(){
        return total_amountSum/ countOfTrips;
    }

    public double getAvgTripDistance(){
        return trip_distanceSum/ countOfTrips;
    }

    public double getAvgTotalAmountAfterTax() {
        return getTotalAmountAfterTax() / countOfTrips;
    }

    public double getTotalAmountAfterTax() {
        return total_amountSum - mta_tax - tolls_amountSum;
    }

    public Tuple11<String, Integer, Double, Double, Double, Double, Integer, Double, Double, Double, Double> getStatAsTuple(){
        return new Tuple11<>(licenseID,
                tripTimeSum_inSecs,
                trip_distanceSum,
                tip_amountSum,
                tolls_amountSum,
                total_amountSum,
                countOfTrips,
                getAvgTripTime(),
                getAvgTollAmount(),
                getAvgTotalAmount(),
                getAvgTripDistance()
        );
    }

    public static String getCSVHeader(){
        return "licenseID," +
                "tripTimeSum_inSecs," +
                "trip_distanceSum," +
                "tip_amountSum," +
                "tolls_amountSum," +
                "total_amountSum,"+
                "countOfTrips," +
                "avgTripTime," +
                "avgTollAmount," +
                "avgTotalAmount," +
                "avgTripDistance\n";
    }

    @Override
    public String toString() {
        return "Statistic{" +
                "licenseID='" + licenseID + '\'' +
                ", tripTimeSum_inSecs=" + tripTimeSum_inSecs +
                ", trip_distanceSum=" + trip_distanceSum +
                ", tip_amountSum=" + tip_amountSum +
                ", tolls_amountSum=" + tolls_amountSum +
                ", total_amountSum=" + total_amountSum +
                ", countOfTrips=" + countOfTrips +
                ", avgtripTime="+getAvgTripTime()+
                ", avgTripDistance="+getAvgTripDistance()+
                ", avgTollAmount" + getAvgTollAmount()+
                ", avgTotalAmount="+getAvgTotalAmount()+
                ", daytimeOfTrips=" + Arrays.toString(daytime) +
                ", weekdayOfTrips=" + Arrays.toString(weekdays) +
                '}';
    }


    public void mergeWeekdays(int[] weekdays) {
        mergeNumericArray(this.weekdays, weekdays);
    }

    public void mergeDaytime(int[] daytime) {
        mergeNumericArray(this.daytime, daytime);
    }

    private void mergeNumericArray(int[] targetArr, int[] arr) {
        for (int i = 0; i < targetArr.length; i++) {
            targetArr[i] += arr[i];
        }
    }


}
