package csbase.azure;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

import javax.xml.parsers.ParserConfigurationException;

import org.xml.sax.SAXException;

import com.microsoft.windowsazure.Configuration;
import com.microsoft.windowsazure.exception.ServiceException;
import com.microsoft.windowsazure.management.compute.ComputeManagementClient;
import com.microsoft.windowsazure.management.compute.ComputeManagementService;
import com.microsoft.windowsazure.management.compute.models.HostedServiceCreateParameters;
import com.microsoft.windowsazure.management.compute.models.HostedServiceListResponse.HostedService;
import com.microsoft.windowsazure.management.configuration.ManagementConfiguration;

import sgaidl.ActionNotSupportedException;
import sgaidl.COMMAND_STATE;
import sgaidl.InvalidActionException;
import sgaidl.JobControlAction;
import sgaidl.ProcessState;
import csbase.sga.executor.JobData;
import csbase.sga.executor.JobExecutor;
import csbase.sga.executor.JobInfo;
import csbase.sga.executor.JobObserver;

public class AzureExecutor implements JobExecutor {
	
	private static final Logger logger = Logger.getLogger(AzureExecutor.class.getName());

	private static Executor threadpool = Executors.newCachedThreadPool();
	
	private static Map<AzureJobData, JobInfo> jobs = new ConcurrentHashMap<>();
	
	private Properties pluginProperties;
	
	public AzureExecutor(Properties pluginProperties) {
		System.out.println("AzureExecutor() : " + pluginProperties);

	}
	
	@Override
	public JobData executeJob(String jobCommand, Map<String, String> extraParams, final JobObserver observer) {
		System.out.println("AzureExecutor.executeJob()");
		System.out.println("* jobCommand: "+jobCommand);
		System.out.println("* extraParams: "+extraParams);

		final AzureJobData res = new AzureJobData(jobCommand, "oioioi", extraParams);
		
		/*
    jobParam.put(COMMAND_PID.value, "0");
    jobParam.put(COMMAND_PPID.value, "0");
    //    jobParam.put(COMMAND_STRING.value, defaulValue);
    jobParam.put(COMMAND_EXEC_HOST.value, "unknown");
    jobParam.put(COMMAND_STATE.value, ProcessState.WAITING.toString());
    jobParam.put(COMMAND_MEMORY_RAM_SIZE_MB.value, defaulValue);
    jobParam.put(COMMAND_MEMORY_SWAP_SIZE_MB.value, defaulValue);
    jobParam.put(COMMAND_CPU_PERC.value, defaulValue);
    jobParam.put(COMMAND_CPU_TIME_SEC.value, defaulValue);
    jobParam.put(COMMAND_WALL_TIME_SEC.value, defaulValue);
    jobParam.put(COMMAND_USER_TIME_SEC.value, defaulValue);
    jobParam.put(COMMAND_SYSTEM_TIME_SEC.value, defaulValue);
    jobParam.put(COMMAND_VIRTUAL_MEMORY_SIZE_MB.value, defaulValue);
    jobParam.put(COMMAND_BYTES_IN_KB.value, defaulValue);
    jobParam.put(COMMAND_BYTES_OUT_KB.value, defaulValue);
    jobParam.put(COMMAND_DISK_BYTES_READ_KB.value, defaulValue);
    jobParam.put(COMMAND_DISK_BYTES_WRITE_KB.value, defaulValue);
		 */
		
		final JobInfo info = new JobInfo();
		info.jobParam.put(COMMAND_STATE.value, ProcessState.WAITING.toString());
		jobs.put(res, info);
		
		try {Thread.sleep(1000);} catch (InterruptedException e) {}
		
		threadpool.execute(new Runnable() {
			@Override
			public void run() {
				
				info.jobParam.put(COMMAND_STATE.value, ProcessState.WAITING.toString());
				try {Thread.sleep(1000);} catch (InterruptedException e) {}
				info.jobParam.put(COMMAND_STATE.value, ProcessState.RUNNING.toString());
				observer.onJobStarted(res);
				try {Thread.sleep(5000);} catch (InterruptedException e) {}
				info.jobParam.put(COMMAND_STATE.value, ProcessState.FINISHED.toString());
				observer.onJobCompleted(info);
				
			}
		});
		
		return res;
	}
	
	@Override
	public void controlJob(JobData data, String child, JobControlAction action)	throws InvalidActionException, ActionNotSupportedException {
		System.out.println("AzureExecutor.controlJob()");
		System.out.println("* data: "+data);
		System.out.println("* child: "+child);
		System.out.println("* action: "+action);
		throw new ActionNotSupportedException();
	}

	@Override
	public JobInfo getJobInfo(JobData data) {
		System.out.println("AzureExecutor.getJobInfo()");
		System.out.println("* data: "+data);
		return jobs.get(data);
	}

	@Override
	public boolean recoveryJob(JobData data, JobObserver observer) {
		System.out.println("AzureExecutor.recoveryJob()");
		System.out.println("* data: "+data);
		return false;
	}
}
