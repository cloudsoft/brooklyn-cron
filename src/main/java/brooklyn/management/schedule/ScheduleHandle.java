package brooklyn.management.schedule;

/**
 * The handle returned when a job is scheduled, allowing the job to be subsequently cancelled.
 * 
 * @see {@link BrooklynScheduler#schedule(CronSchedule, Job, brooklyn.entity.Entity)})
 * 
 * @author aled
 */
public interface ScheduleHandle {
    public void cancel(boolean interrupt);
}