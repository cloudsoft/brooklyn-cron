package brooklyn.management.schedule;

import brooklyn.entity.Entity;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;

/**
 * For scheduling jobs, e.g. using a cron-style format.
 * 
 * @author aled
 */
public interface BrooklynScheduler {

    void start();
    
    void stop();
    
    boolean isEmpty();
    
    /**
     * @see {@link #schedule(CronSchedule, Job, Predicate, Entity)}; equivalent of using {@link Predicates#alwaysTrue()}.
     */
    ScheduleHandle schedule(CronSchedule schedule, Job operation, Entity context);
    
    /**
     * Schedules an operation to be run, bases on the cron semantics.
     * 
     * The function must be serializable.
     * 
     * @param schedule Indicates the timing for execution the job. 
     * @param job      The job to execute.
     * @param filter   A filter to be applied, passing in {@link System#currentTimeMillis()}, to determine if job should be executed.
     * @param context  The entity whose execution context should be used.
     */
    ScheduleHandle schedule(CronSchedule schedule, Job job, Predicate<? super Long> filter, Entity context);

    /**
     * Returns the next time, after the given time in UTC millis, that the given cron schedule would be executed (if scheduled).
     */
    long getNextExecutionTimeAfter(CronSchedule cron, long fromTime);
}
