package brooklyn.management.schedule;

import java.util.Calendar;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import brooklyn.util.time.Time;

import com.google.common.base.Predicate;

// TODO Move to brooklyn.util.time.TimePredicates
public class TimePredicates {

    public static Predicate<Long> multipleOf(final long val) {
        return new Predicate<Long>() {
            @Override public boolean apply(Long input) {
                return (input != null) && input % val == 0;
            }
            @Override
            public String toString() {
                return "MultipleOf(" + val + ")";
            }};
    }

    public static Predicate<Long> daysSince(final long startTime, final Predicate<Long> dayCheck) {
        return new Predicate<Long>() {
            @Override public boolean apply(Long input) {
                if (input == null) return false;
                long daysBetween = daysBetweenRounded(startTime, input);
                return dayCheck.apply(daysBetween);
            }
            @Override
            public String toString() {
                return "DaysSince(" + Time.makeDateString(startTime) + ", check=" + dayCheck + ")";
            }};
    }
    
    public static Predicate<Long> weeksSince(final long startTime, final Predicate<Long> weekCheck) {
        return new Predicate<Long>() {
            @Override public boolean apply(Long input) {
                if (input == null) return false;
                long daysBetween = weeksBetweenRounded(startTime, input);
                return weekCheck.apply(daysBetween);
            }
            @Override
            public String toString() {
                return "WeeksSince(" + Time.makeDateString(startTime) + ", check=" + weekCheck + ")";
            }};
    }
    
    
    public static Predicate<Long> monthsSince(final long startTime, final Predicate<Long> monthCheck) {
        return new Predicate<Long>() {
            @Override public boolean apply(Long input) {
                if (input == null) return false;
                long daysBetween = monthsBetweenRounded(startTime, input);
                return monthCheck.apply(daysBetween);
            }
            @Override
            public String toString() {
                return "MonthsSince(" + Time.makeDateString(startTime) + ", check=" + monthCheck + ")";
            }};
    }
    
    public static long daysBetweenRounded(long startTime, long endTime) {
        long timeDiffMillis = endTime - startTime;
        return Math.round(((double)timeDiffMillis) / TimeUnit.DAYS.toMillis(1));
    }
    
    public static long weeksBetweenRounded(long startTime, long endTime) {
        long timeDiffMillis = endTime - startTime;
        return Math.round(((double)timeDiffMillis) / TimeUnit.DAYS.toMillis(7));
    }
    
    public static long monthsBetweenRounded(long startTime, long endTime) {
        Calendar startDate = toCalendarUtc(startTime);
        Calendar endDate = toCalendarUtc(endTime);
        
        int yearsDiff = endDate.get(Calendar.YEAR) - startDate.get(Calendar.YEAR);
        int monthsDiff = endDate.get(Calendar.MONTH) - startDate.get(Calendar.MONTH);
        int daysDiff = endDate.get(Calendar.DAY_OF_MONTH) - startDate.get(Calendar.DAY_OF_MONTH);
        return Math.round(((double)yearsDiff) * 12 + monthsDiff + ((double)daysDiff) / 28);
    }
    
    public static Calendar toCalendarUtc(long time) {
        Calendar result = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        result.setTimeInMillis(time);
        return result;
    }
}
