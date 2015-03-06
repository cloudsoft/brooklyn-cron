package brooklyn.management.schedule;

import javax.annotation.Nullable;

import brooklyn.util.text.Strings;

import com.google.common.annotations.Beta;
import com.google.common.base.Preconditions;


/**
 * Represents a cron schedule, e.g. see {@linkplain http://en.wikipedia.org/wiki/Cron#CRON_expression}.
 * 
 * @author aled
 */
public class CronSchedule {

    // TODO Move to core brooklyn brooklyn.util.text.Strings
    /** 
     * cf. guava Preconditions.checkXxxx
     * 
     * @throws NullPointerException if string is null
     * @throws IllegalArgumentException if string is empty or only contains white space
     */
    private static String checkNonBlank(String s, @Nullable String errorMessageTemplate, @Nullable Object... errorMessageArgs) {
        Preconditions.checkNotNull(s, errorMessageTemplate, errorMessageArgs);
        Preconditions.checkArgument(Strings.isNonBlank(s), errorMessageTemplate, errorMessageArgs);
        return s;
    }

    public static CronSchedule fromParts(String minute, String hour, String dayOfMonth, String month, String dayOfWeek) {
        return new CronSchedule("0", minute, hour, dayOfMonth, month, dayOfWeek);
    }

    @Beta
    public static CronSchedule fromParts(String second, String minute, String hour, String dayOfMonth, String month, String dayOfWeek) {
        return new CronSchedule(second, minute, hour, dayOfMonth, month, dayOfWeek);
    }

    private final String second;
    private final String minute;
    private final String hour;
    private final String dayOfMonth;
    private final String month;
    private final String dayOfWeek;

    protected CronSchedule(String second, String minute, String hour, String dayOfMonth, String month, String dayOfWeek) {
        this.second = checkNonBlank(second, "second").trim();
        this.minute = checkNonBlank(minute, "minute").trim();
        this.hour = checkNonBlank(hour, "hour").trim();
        this.dayOfMonth = checkNonBlank(dayOfMonth, "dayOfMonth").trim();
        this.month = checkNonBlank(month, "month").trim();
        this.dayOfWeek = checkNonBlank(dayOfWeek, "dayOfWeek").trim();
    }
    
    @Override
    public String toString() {
        return "cron(" + getSecond() + " " + getMinute() + " " + getHour() + " " + getDayOfMonth() + " " + getMonth() + " " + getDayOfWeek() + ")";
    }
    
    @Beta
    public String getSecond() {
        return second;
    }
    
    public String getMinute() {
        return minute;
    }
    
    public String getHour() {
        return hour;
    }
    
    public String getDayOfMonth() {
        return dayOfMonth;
    }
    
    public String getMonth() {
        return month;
    }
    
    public String getDayOfWeek() {
        return dayOfWeek;
    }
}
