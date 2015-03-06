package brooklyn.management.schedule;

import brooklyn.management.ManagementContext;

/**
 * A job to be (repeatedly) executed by the BrooklynScheduler.
 * 
 * It is strongly recommended that implementations should be serializable (e.g. using xstream) 
 * to support persistance and server-restart.
 * 
 * @author aled
 */
public interface Job {
    
    /**
     * @param managementContext
     * @param currentTime       Normally equivalent to System.currentTimeMillis (i.e. utc)
     * @param timeLastExecuted  Time (utc) that job was last executed, or -1 if never
     */
    public void run(ManagementContext managementContext, long currentTime, long timeLastExecuted);
}