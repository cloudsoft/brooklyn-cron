package brooklyn.management.schedule;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.concurrent.TimeUnit;

import org.testng.annotations.Test;

public class TimePredicatesTest {

    public static final long DAY_IN_MILLIS = TimeUnit.DAYS.toMillis(1);
    public static final long WEEK_IN_MILLIS = TimeUnit.DAYS.toMillis(7);
    public static final long APPROX_MONTH_IN_MILLIS = TimeUnit.DAYS.toMillis(365) / 12;
    
    private long now = System.currentTimeMillis();
    
    @Test
    public void testDaysBetween() throws Exception {
        assertEquals(TimePredicates.daysBetweenRounded(now, now + DAY_IN_MILLIS), 1);
        assertEquals(TimePredicates.daysBetweenRounded(now, now + DAY_IN_MILLIS + DAY_IN_MILLIS/4), 1);
        assertEquals(TimePredicates.daysBetweenRounded(now, now + DAY_IN_MILLIS - DAY_IN_MILLIS/4), 1);

        assertEquals(TimePredicates.daysBetweenRounded(now, now + DAY_IN_MILLIS*2), 2);
        assertEquals(TimePredicates.daysBetweenRounded(now, now + DAY_IN_MILLIS*2 + DAY_IN_MILLIS/4), 2);
        assertEquals(TimePredicates.daysBetweenRounded(now, now + DAY_IN_MILLIS*2 - DAY_IN_MILLIS/4), 2);

        assertEquals(TimePredicates.daysBetweenRounded(now, now), 0);
        assertEquals(TimePredicates.daysBetweenRounded(now, now + DAY_IN_MILLIS/4), 0);
        assertEquals(TimePredicates.daysBetweenRounded(now, now - DAY_IN_MILLIS/4), 0);
    }
    
    @Test
    public void testWeeksBetween() throws Exception {
        assertEquals(TimePredicates.weeksBetweenRounded(now, now + WEEK_IN_MILLIS), 1);
        assertEquals(TimePredicates.weeksBetweenRounded(now, now + WEEK_IN_MILLIS + WEEK_IN_MILLIS/4), 1);
        assertEquals(TimePredicates.weeksBetweenRounded(now, now + WEEK_IN_MILLIS - WEEK_IN_MILLIS/4), 1);

        assertEquals(TimePredicates.weeksBetweenRounded(now, now + WEEK_IN_MILLIS*2), 2);
        assertEquals(TimePredicates.weeksBetweenRounded(now, now + WEEK_IN_MILLIS*2 + WEEK_IN_MILLIS/4), 2);
        assertEquals(TimePredicates.weeksBetweenRounded(now, now + WEEK_IN_MILLIS*2 - WEEK_IN_MILLIS/4), 2);

        assertEquals(TimePredicates.weeksBetweenRounded(now, now), 0);
        assertEquals(TimePredicates.weeksBetweenRounded(now, now + WEEK_IN_MILLIS/4), 0);
        assertEquals(TimePredicates.weeksBetweenRounded(now, now - WEEK_IN_MILLIS/4), 0);
    }
    
    @Test
    public void testMonthsBetween() throws Exception {
        assertEquals(TimePredicates.monthsBetweenRounded(now, now + APPROX_MONTH_IN_MILLIS), 1);
        assertEquals(TimePredicates.monthsBetweenRounded(now, now + APPROX_MONTH_IN_MILLIS + APPROX_MONTH_IN_MILLIS/4), 1);
        assertEquals(TimePredicates.monthsBetweenRounded(now, now + APPROX_MONTH_IN_MILLIS - APPROX_MONTH_IN_MILLIS/4), 1);
        
        assertEquals(TimePredicates.monthsBetweenRounded(now, now + APPROX_MONTH_IN_MILLIS*2), 2);
        assertEquals(TimePredicates.monthsBetweenRounded(now, now + APPROX_MONTH_IN_MILLIS*2 + APPROX_MONTH_IN_MILLIS/4), 2);
        assertEquals(TimePredicates.monthsBetweenRounded(now, now + APPROX_MONTH_IN_MILLIS*2 - APPROX_MONTH_IN_MILLIS/4), 2);
        
        assertEquals(TimePredicates.monthsBetweenRounded(now, now), 0);
        assertEquals(TimePredicates.monthsBetweenRounded(now, now + APPROX_MONTH_IN_MILLIS/4), 0);
        assertEquals(TimePredicates.monthsBetweenRounded(now, now - APPROX_MONTH_IN_MILLIS/4), 0);
    }
    
    @Test
    public void testMultipleOf() throws Exception {
        assertTrue(TimePredicates.multipleOf(2).apply(0L));
        assertTrue(TimePredicates.multipleOf(2).apply(2L));
        assertTrue(TimePredicates.multipleOf(2).apply(4L));
        assertFalse(TimePredicates.multipleOf(2).apply(1L));
    }
    
    @Test
    public void testDaysSince() throws Exception {
        assertTrue(TimePredicates.daysSince(now, TimePredicates.multipleOf(2)).apply(now));
        assertTrue(TimePredicates.daysSince(now, TimePredicates.multipleOf(2)).apply(now + DAY_IN_MILLIS*2));
        assertTrue(TimePredicates.daysSince(now, TimePredicates.multipleOf(2)).apply(now + DAY_IN_MILLIS*2 + DAY_IN_MILLIS/4));
        assertFalse(TimePredicates.daysSince(now, TimePredicates.multipleOf(2)).apply(now + DAY_IN_MILLIS));
    }
    
    @Test
    public void testWeeksSince() throws Exception {
        assertTrue(TimePredicates.weeksSince(now, TimePredicates.multipleOf(2)).apply(now));
        assertTrue(TimePredicates.weeksSince(now, TimePredicates.multipleOf(2)).apply(now + WEEK_IN_MILLIS*2));
        assertTrue(TimePredicates.weeksSince(now, TimePredicates.multipleOf(2)).apply(now + WEEK_IN_MILLIS*2 + WEEK_IN_MILLIS/4));
        assertFalse(TimePredicates.weeksSince(now, TimePredicates.multipleOf(2)).apply(now + WEEK_IN_MILLIS));
    }
    
    @Test
    public void testMonthsSince() throws Exception {
        assertTrue(TimePredicates.monthsSince(now, TimePredicates.multipleOf(2)).apply(now));
        assertTrue(TimePredicates.monthsSince(now, TimePredicates.multipleOf(2)).apply(now + APPROX_MONTH_IN_MILLIS*2));
        assertTrue(TimePredicates.monthsSince(now, TimePredicates.multipleOf(2)).apply(now + APPROX_MONTH_IN_MILLIS*2 + APPROX_MONTH_IN_MILLIS/4));
        assertFalse(TimePredicates.monthsSince(now, TimePredicates.multipleOf(2)).apply(now + APPROX_MONTH_IN_MILLIS));
    }
}
