package brooklyn.management.schedule.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import brooklyn.entity.basic.ApplicationBuilder;
import brooklyn.entity.basic.Entities;
import brooklyn.management.ManagementContext;
import brooklyn.management.schedule.CronSchedule;
import brooklyn.management.schedule.Job;
import brooklyn.management.schedule.ScheduleHandle;
import brooklyn.test.Asserts;
import brooklyn.test.entity.TestApplication;
import brooklyn.util.exceptions.Exceptions;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class BrooklynSchedulerTest {

    private static final Logger LOG = LoggerFactory.getLogger(BrooklynSchedulerTest.class);

    private static final long OVERHEAD_MS = 1000;
    
    private TestApplication app;
    private ManagementContext managementContext;
    private BrooklynSchedulerImpl scheduler;

    @BeforeMethod(alwaysRun=true)
    public void setUp() throws Exception {
        app = ApplicationBuilder.newManagedApp(TestApplication.class);
        managementContext = app.getManagementContext();
        scheduler = new BrooklynSchedulerImpl(managementContext);
        scheduler.start();
    }
    
    @AfterMethod(alwaysRun=true)
    public void tearDown() throws Exception {
        if (scheduler != null) scheduler.stop();
        if (managementContext != null) Entities.destroyAll(managementContext);
    }
    
    @Test
    public void testToQuartzCronString() throws Exception {
        assertEquals(scheduler.toQuartzCronString(CronSchedule.fromParts("*", "*", "*", "*", "*", "*")), "* * * * * ?");
        assertEquals(scheduler.toQuartzCronString(CronSchedule.fromParts("1", "*", "*", "*", "*", "*")), "1 * * * * ?");
        assertEquals(scheduler.toQuartzCronString(CronSchedule.fromParts("*", "*", "*", "1", "*", "*")), "* * * 1 * ?");
        assertEquals(scheduler.toQuartzCronString(CronSchedule.fromParts("*", "*", "*", "*", "*", "1")), "* * * ? * 1");
        assertEquals(scheduler.toQuartzCronString(CronSchedule.fromParts("*", "*", "*", "1", "*", "2")), "* * * 1 * 2");
    }
    
    @Test(groups="Integration") // because slow
    public void testCallsEverySecond() throws Exception {
        final RecordingJob job = new RecordingJob();
        scheduler.schedule(CronSchedule.fromParts("*", "*", "*", "*", "*", "*"), job, app);
        Asserts.succeedsEventually(new Runnable() {
            public void run() {
                long expectedTimeDiff = 1000;
                String errmsg = "calls="+job.calls;
                job.assertNumCallsAtLeast(2, errmsg);
                job.assertFirstHasNoLastExecutedTimestamp(errmsg);
                job.assertSubsequentHasLastExecutedTimestamp(errmsg);
                job.assertCallInterval(expectedTimeDiff, errmsg);
                job.assertManagementContext(managementContext, errmsg);
            }});
    }
    
    @Test
    public void testMultipleCalls() throws Exception {
        final RecordingJob job = new RecordingJob();
        for (int i = 0; i < 10; i++) {
            ScheduleHandle handle = scheduler.schedule(CronSchedule.fromParts("*", "*", "*", "*", "*", "*"), job, app);
            assertNotNull(handle);
        }
    }
    
    @Test
    public void testIsEmpty() throws Exception {
        assertTrue(scheduler.isEmpty());
        final RecordingJob job = new RecordingJob();
        ScheduleHandle handle = scheduler.schedule(CronSchedule.fromParts("*", "*", "*", "*", "*", "*"), job, app);
        assertFalse(scheduler.isEmpty());
        handle.cancel(true);
        assertTrue(scheduler.isEmpty());
    }
    
    @Test(groups="Integration") // because slow
    public void testUsesExecFilter() throws Exception {
        final RecordingJob job = new RecordingJob();

        // Filter accepts every second, and records timestamps of those that execute
        final List<Long> allCallsToPredicate = Lists.newCopyOnWriteArrayList();
        final List<Long> filteredCalls = Lists.newCopyOnWriteArrayList();
        Predicate<? super Long> filter = new Predicate<Long>() {
            int callCount = 0;
            @Override public boolean apply(Long input) {
                allCallsToPredicate.add(input);
                if (callCount++ % 2 == 0) {
                    filteredCalls.add(input);
                    return true;
                } else {
                    return false;
                }
            }
        };
        
        scheduler.schedule(CronSchedule.fromParts("*", "*", "*", "*", "*", "*"), job, filter, app);
        
        // Wait for 2 calls
        Asserts.succeedsEventually(new Runnable() {
            public void run() {
                job.assertNumCallsAtLeast(2, "calls="+job.calls);
            }});
        
        assertTrue(filteredCalls.containsAll(job.getCallTimestamps()));
        assertTrue(filteredCalls.size() < allCallsToPredicate.size());
    }
    
    @Test(groups="Integration") // because slow
    public void testCallsAreSequential() throws Exception {
        final BlockingJob job = new BlockingJob();
        try {
            scheduler.schedule(CronSchedule.fromParts("*", "*", "*", "*", "*", "*"), job, app);
            
            Asserts.succeedsEventually(new Runnable() {
                @Override public void run() {
                    assertEquals(job.callCounter.get(), 1);
                }});
            
            // wait long enough that expect additional jobs to execute (if it didn't do sequentially)
            Thread.sleep(3000);
        } finally {
            job.latch.countDown();
        }
        
        Asserts.succeedsEventually(new Runnable() {
            @Override public void run() {
                assertTrue(job.callCounter.get() >= 2);
                assertEquals(job.maxConcurrentCallCounter.get(), 1);
            }});
    }
    
    @Test
    public void testNextExecutionTime() throws Exception {
        Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        calendar.set(Calendar.MILLISECOND, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.DAY_OF_MONTH, 1);
        calendar.set(Calendar.MONTH, 0);
        calendar.set(Calendar.YEAR, 2020); // 1st Jan 2020, at 0:00
        long fromTime = calendar.getTimeInMillis();
        LOG.info("fromTime="+new Date(fromTime));
        
        assertNextExecutionTime(CronSchedule.fromParts("*", "*", "*", "*", "*", "*"), fromTime, fromTime + 1000);
        assertNextExecutionTime(CronSchedule.fromParts("*", "1", "*", "*", "*", "*"), fromTime, fromTime + 1000*60);
        assertNextExecutionTime(CronSchedule.fromParts("*", "*", "1", "*", "*", "*"), fromTime, fromTime + 1000*60*60);
        assertNextExecutionTime(CronSchedule.fromParts("*", "*", "*", "2", "*", "*"), fromTime, fromTime + 1000*60*60*24);
    }
    
    private void assertNextExecutionTime(CronSchedule cron, long fromTime, long expected) {
        long actual = scheduler.getNextExecutionTimeAfter(cron, fromTime);
        long diff = (actual - expected);
        assertEquals(actual, expected, "diff="+diff+"; actual="+new Date(actual)+"; expected="+new Date(expected));
    }

    public static class BlockingJob implements Job {
        public volatile CountDownLatch latch = new CountDownLatch(1);
        public final AtomicInteger callCounter = new AtomicInteger();
        public final AtomicInteger concurrentCallCounter = new AtomicInteger();
        public final AtomicInteger maxConcurrentCallCounter = new AtomicInteger();
        
        @Override
        public void run(ManagementContext managementContext, long currentTime, long timeLastExecuted) {
            concurrentCallCounter.incrementAndGet();
            maxConcurrentCallCounter.set(Math.max(maxConcurrentCallCounter.get(), concurrentCallCounter.get()));
            try {
                callCounter.incrementAndGet();
                if (latch != null)
                    try {
                        latch.await();
                    } catch (InterruptedException e) {
                        throw Exceptions.propagate(e);
                    }
                return;
            } finally {
                concurrentCallCounter.decrementAndGet();
            }
        }
    }
    
    public static class RecordingJob implements Job {
        public final List<List<Object>> calls = Lists.newCopyOnWriteArrayList();
        
        @Override
        public void run(ManagementContext managementContext, long currentTime, long timeLastExecuted) {
            ImmutableList<Object> details = ImmutableList.of(System.currentTimeMillis(), managementContext, currentTime, timeLastExecuted);
            LOG.info("Scheduled job called: "+details);
            calls.add(details);
        }
        public Collection<?> getCallTimestamps() {
            List<Long> result = Lists.newArrayList();
            for (List<Object> call : calls) {
                result.add((Long)call.get(2));
            }
            return result;
        }
        public void assertNumCallsAtLeast(int num, String errmsg) {
            assertTrue(calls.size() >= num, errmsg);
        }
        public void assertFirstHasNoLastExecutedTimestamp(String errmsg) {
            assertTrue(calls.size() >= 1, errmsg);
            assertEquals(calls.get(0).get(3), Long.valueOf(-1L), errmsg);
        }
        public void assertSubsequentHasLastExecutedTimestamp(String errmsg) {
            long prevTime = (Long) calls.get(0).get(2);
            for (int i = 1; i < calls.size(); i++) {
                assertEquals(calls.get(i).get(3), prevTime, errmsg);
                prevTime = (Long) calls.get(i).get(2);
            }
        }
        public void assertCallInterval(long interval, String errmsg) {
            long prevTime = (Long) calls.get(0).get(2);
            for (int i = 1; i < calls.size(); i++) {
                long nextTime = (Long) calls.get(i).get(2);
                long diffTime = nextTime - prevTime;
                assertTrue(diffTime >= interval - OVERHEAD_MS && diffTime <= interval + OVERHEAD_MS, errmsg);
                prevTime = nextTime;
            }
        }
        public void assertManagementContext(ManagementContext expected, String errmsg) {
            for (List<?> call : calls) {
                assertEquals(call.get(1), expected, errmsg);
            }
        }
    }
}
