package brooklyn.management.schedule.impl;

import static com.google.common.base.Preconditions.checkNotNull;

import java.text.ParseException;
import java.util.Date;
import java.util.Map;
import java.util.Set;

import org.quartz.CronExpression;
import org.quartz.CronScheduleBuilder;
import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.JobKey;
import org.quartz.ScheduleBuilder;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.TriggerKey;
import org.quartz.impl.DirectSchedulerFactory;
import org.quartz.impl.matchers.GroupMatcher;
import org.quartz.simpl.RAMJobStore;
import org.quartz.simpl.SimpleThreadPool;
import org.quartz.spi.JobStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import brooklyn.entity.Entity;
import brooklyn.entity.basic.Entities;
import brooklyn.entity.basic.EntityInternal;
import brooklyn.management.ExecutionManager;
import brooklyn.management.ManagementContext;
import brooklyn.management.schedule.BrooklynScheduler;
import brooklyn.management.schedule.CronSchedule;
import brooklyn.management.schedule.Job;
import brooklyn.management.schedule.ScheduleHandle;
import brooklyn.util.collections.MutableMap;
import brooklyn.util.exceptions.Exceptions;
import brooklyn.util.task.BasicExecutionManager;
import brooklyn.util.task.SingleThreadedScheduler;
import brooklyn.util.text.Identifiers;
import brooklyn.util.text.Strings;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

public class BrooklynSchedulerImpl implements BrooklynScheduler {

    /*
     * FIXME Need to support rebind:
     *  - TODO On re-start, how do we detect if there were scheduled events that were missed; and ensure they are 
     *         kicked off now correctly?
     *  - TODO Make xstream-compliant
     */

    private static final Logger LOG = LoggerFactory.getLogger(BrooklynSchedulerImpl.class);

    static class JobState {
        final CronSchedule schedule;
        final Job job;
        final Entity context;
        final TriggerKey triggerKey;
        final String jobId;
        long lastExecutionTime = -1;
        
        public JobState(CronSchedule schedule, Job job, Entity context, TriggerKey triggerKey, String jobId) {
            this.schedule = checkNotNull(schedule, "schedule");
            this.job = checkNotNull(job, "job");
            this.context = checkNotNull(context, "context");
            this.triggerKey = checkNotNull(triggerKey, "triggerKey");
            this.jobId = checkNotNull(jobId, "jobId");
        }
        public void setLastExecutionTime(long newVal) {
            lastExecutionTime = newVal;
        }
    }
    
    private final Map<String, JobState> state = Maps.newConcurrentMap();
    private final ManagementContext managementContext;
    private final String nameSuffix;
    
    private SimpleThreadPool threadPool;
    private Scheduler scheduler;
    
    public BrooklynSchedulerImpl(ManagementContext managementContext) {
        this(managementContext, "");
    }

    public BrooklynSchedulerImpl(ManagementContext managementContext, String nameSuffix) {
        this.managementContext = managementContext;
        this.nameSuffix = nameSuffix;
    }

    @Override
    public boolean isEmpty() {
        try {
            Set<JobKey> jobs = scheduler.getJobKeys(GroupMatcher.<JobKey>anyGroup());
            return jobs.isEmpty();
        } catch (SchedulerException e) {
            throw Exceptions.propagate(e);
        }
    }
    
    @Override
    public void start() {
        threadPool = new SimpleThreadPool();
        threadPool.setThreadCount(1);
        threadPool.setThreadNamePrefix("brooklyn-"+managementContext.getManagementNodeId()+"-scheduler"+(Strings.isBlank(nameSuffix) ? "" : "-"+nameSuffix));
        threadPool.setMakeThreadsDaemons(false);
        
        JobStore jobStore = new RAMJobStore();
        String schedulerName = "brooklyn-"+managementContext.getManagementNodeId()+"-scheduler"+(Strings.isBlank(nameSuffix) ? "" : "-"+nameSuffix);
        try {
            DirectSchedulerFactory.getInstance().createScheduler(
                    schedulerName,
                    schedulerName,
                    threadPool,
                    jobStore);
            scheduler = DirectSchedulerFactory.getInstance().getScheduler(schedulerName);
            scheduler.start();
        } catch (Exception e) {
            stop();
            throw Exceptions.propagate(e);
        }
    }
    
    @Override
    public void stop() {
        if (scheduler != null) {
            try {
                scheduler.shutdown();
            } catch (SchedulerException e) {
                LOG.warn("Problem shutting down scheduler (continuing)", e);
            }
        }
        if (threadPool != null) threadPool.shutdown(false);
    }
    
    @Override
    public long getNextExecutionTimeAfter(CronSchedule cron, long fromTime) {
        try {
            CronExpression quartzCron = new CronExpression(toQuartzCronString(cron));
            Date result = quartzCron.getNextValidTimeAfter(new Date(fromTime));
            return result.getTime();
        } catch (ParseException e) {
            throw Exceptions.propagate(e);
        }
    }
    @Override
    public ScheduleHandle schedule(CronSchedule schedule, Job job, Entity context) {
        return schedule(schedule, job, Predicates.alwaysTrue(), context);
    }
    
    @Override
    public ScheduleHandle schedule(CronSchedule schedule, Job job, Predicate<? super Long> filter, Entity context) {
        CronScheduleBuilder quartzSchedule = CronScheduleBuilder.cronSchedule(toQuartzCronString(schedule));
        return schedule(schedule, quartzSchedule, job, filter, context);
    }
    
    protected ScheduleHandle schedule(CronSchedule brooklynSchedule, ScheduleBuilder<? extends Trigger> quartzSchedule, Job job, Predicate<? super Long> filter, Entity context) {
        String jobId = Identifiers.makeRandomId(8);
        String triggerId = Identifiers.makeRandomId(8);
        String execTag = "scheduler-"+context.getId()+"-"+jobId;
        
        JobDataMap jobData = new JobDataMap(ImmutableMap.builder()
                .put("scheduler", this)
                .put("managementContext", managementContext)
                .put("jobId", jobId)
                .put("entityId", context.getId())
                .put("job", job)
                .put("filter", (filter == null ? Predicates.alwaysTrue() : filter))
                .put("execTag", execTag)
                .build());
        JobDetail jobDetail = JobBuilder.newJob(QuartzJobImpl.class)
                .setJobData(jobData)
                .withIdentity(jobId, "group1")
                .build();
        
        Trigger trigger = TriggerBuilder
                .newTrigger()
                .withIdentity(triggerId, "group1")
                .withSchedule(quartzSchedule)
                .build();
        
        ExecutionManager executionManager = managementContext.getExecutionManager();
        ((BasicExecutionManager) executionManager).setTaskSchedulerForTag(execTag, SingleThreadedScheduler.class);

        try {
            scheduler.scheduleJob(jobDetail, trigger);
        } catch (SchedulerException e) {
            throw Exceptions.propagate(e);
        }
        
        state.put(jobId, new JobState(brooklynSchedule, job, context, trigger.getKey(), jobId));

        return new ScheduleHandleImpl(jobId);
    }
    
    protected String toQuartzCronString(CronSchedule schedule) {
        // Needs to convert to quartz's non-standard "?"
        String dayOfWeek = schedule.getDayOfWeek();
        String dayOfMonth = schedule.getDayOfMonth();
        if ("*".equals(dayOfWeek) || "?".equals(dayOfWeek)) {
            dayOfWeek = "?";
        } else {
            if ("*".equals(dayOfMonth)) {
                dayOfMonth = "?";
            }
        }
        return schedule.getSecond() + " " + schedule.getMinute() + " " + schedule.getHour() + " " + 
                dayOfMonth + " " + schedule.getMonth() + " " + dayOfWeek;
    }
    
    protected Long getLastExecutionTimestamp(String jobId) {
        JobState jobState = state.get(jobId);
        return (jobState != null) ? jobState.lastExecutionTime : null;
    }
    
    protected void recordExecutionTimestamp(String jobId, long now) {
        JobState jobState = state.get(jobId);
        if (jobState != null) {
            jobState.setLastExecutionTime(now);
        }
    }
    
    protected void cancelJob(String jobId) {
        JobState jobState = state.get(jobId);
        if (jobState != null) {
            try {
                getQuartzScheduler().unscheduleJob(jobState.triggerKey);
            } catch (SchedulerException e) {
                throw Exceptions.propagate(e);
            }
        }
    }
    
    protected Scheduler getQuartzScheduler() {
        return scheduler;
    }
    
    // TODO Must be static for rebind?
    public class ScheduleHandleImpl implements ScheduleHandle {
        private final String jobId;
        
        public ScheduleHandleImpl(String jobId) {
            this.jobId = jobId;
        }
        @Override
        public void cancel(boolean interrupt) {
            // TODO how to interrupt?
            cancelJob(jobId);
        }
    }
    
    public static class QuartzJobImpl implements org.quartz.Job {
        public QuartzJobImpl() {
            // called automatically by scheduler
        }
        
        @Override
        public void execute(JobExecutionContext context) throws JobExecutionException {
            final long now = System.currentTimeMillis();
            JobDataMap jobDataMap = context.getJobDetail().getJobDataMap();
            final BrooklynSchedulerImpl scheduler = (BrooklynSchedulerImpl) jobDataMap.get("scheduler");
            final ManagementContext managementContext = (ManagementContext) jobDataMap.get("managementContext");
            final String jobId = (String) jobDataMap.get("jobId");
            final String entityId = (String) jobDataMap.get("entityId");
            final Job job = (Job) jobDataMap.get("job");
            final Predicate<? super Long> filter = (Predicate<? super Long>) jobDataMap.get("filter");
            final String execTag = (String) jobDataMap.get("execTag");
            final Entity entity = managementContext.getEntityManager().getEntity(entityId);
            
            if (!filter.apply(now)) {
                if (LOG.isDebugEnabled()) LOG.debug("Schedule filter does not match; not executing for entity {}, job {}", entity, job);
                return;
            }
            
            if (entity != null && Entities.isManaged(entity)) {
                MutableMap<String, String> flags = MutableMap.of("tag", execTag);
                ((EntityInternal)entity).getExecutionContext().submit(flags, new Runnable() {
                    @Override public void run() {
                        final Long lastTimeExecuted = scheduler.getLastExecutionTimestamp(jobId);
                        try {
                            job.run(managementContext, now, lastTimeExecuted == null ? -1 : lastTimeExecuted);
                        } catch (Exception e) {
                            LOG.warn("Problem executing scheduled job {} associated with entity {} (rethrowing)", job, entity);
                            throw Exceptions.propagate(e);
                        } finally {
                            scheduler.recordExecutionTimestamp(jobId, now);
                        }
                    }});
            } else {
                // TODO only log once per entity?
                LOG.warn("Entity "+entityId+" not managed; cannot execute associated scheduled job {}", job);
            }
        }
    }
}
