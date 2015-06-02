package csbase.fogbow;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.fogbowcloud.cli.FogbowClient;
import org.fogbowcloud.cli.Instance;
import org.fogbowcloud.cli.Request;
import org.json.JSONObject;

import sgaidl.ActionNotSupportedException;
import sgaidl.COMMAND_STATE;
import sgaidl.InvalidActionException;
import sgaidl.JobControlAction;
import sgaidl.ProcessState;
import csbase.server.plugin.service.sgaservice.SGADataTransferException;
import csbase.sga.executor.JobData;
import csbase.sga.executor.JobExecutor;
import csbase.sga.executor.JobInfo;
import csbase.sga.executor.JobObserver;

public class FogbowExecutor implements JobExecutor {

	private static final Logger LOGGER = Logger.getLogger(FogbowExecutor.class.getName());
	private static final Executor EXECUTOR = Executors.newCachedThreadPool();
	
	private static final String REMOTE_COMMAND_LINE = 
			"function exec_in_bg { %s; echo $? > %s; }; exec_in_bg &";
	
	private static final String PROP_INPUT_FILES = "fogbow_input_files_path";
	private static final String PROP_OUTPUT_FILES = "fogbow_output_files_path";
	private static final String PROP_FEDERATION_TOKEN = "fogbow_federation_token";
	private static final String PROP_LOCAL_TOKEN = "fogbow_local_token";
	
	private static final String PROP_SSH_PRIVATE_KEY_PATH = "fogbow_ssh_private_key_path";
	private static final String PROP_SSH_PUBLIC_KEY_PATH = "fogbow_ssh_public_key_path";
	private static final String PROP_SSH_USER_NAME = "fogbow_ssh_user_name";
	private static final String PROP_MANAGER_HOST = "fogbow_manager_host";
	private static final String PROP_MANAGER_PORT = "fogbow_manager_port";
	private static final String PROP_IMAGE_NAME = "fogbow_image";
	private static final String PROP_REQUIREMENTS = "fogbow_requirements";
	
	private static final int RETRY_INTERVAL = 30000;
	private static final int MAX_RETRIES = 30;
	private static final String REMOTE_SANDBOX = "/tmp/sandbox/";
	
	private Properties pluginProperties;
	
	public FogbowExecutor(Properties pluginProperties) {
		this.pluginProperties = pluginProperties;
	}

	@Override
	public JobData executeJob(String jobCommand,
			Map<String, String> extraParams, JobObserver observer) {
		FogbowClient fogbowClient = new FogbowClient(pluginProperties.getProperty(PROP_MANAGER_HOST), 
				Integer.parseInt(pluginProperties.getProperty(PROP_MANAGER_PORT)), 
				extraParams.get(PROP_FEDERATION_TOKEN));
		
		String requestId = null;
		try {
			requestId = fogbowClient.createRequest(pluginProperties.getProperty(PROP_REQUIREMENTS), 
					pluginProperties.getProperty(PROP_IMAGE_NAME), 
					pluginProperties.getProperty(PROP_SSH_PUBLIC_KEY_PATH), 
					extraParams.get(PROP_LOCAL_TOKEN));
		} catch (Exception e) {
			observer.onJobLost();
			LOGGER.log(Level.WARNING, "Couldn't submit request to the Fogbow manager", e);
			return null;
		}
		
		int retries = 0;
		Request request = null;
		boolean shouldRetry = true;
		while (shouldRetry && (request = getRequestInfo(fogbowClient, requestId)) == null) {
			sleep();
			shouldRetry = retries++ < MAX_RETRIES;
		}
		Instance instance = null;
		while (shouldRetry && (instance = getInstanceInfo(fogbowClient, request)) == null) {
			sleep();
			shouldRetry = retries++ < MAX_RETRIES;
		}

		while (shouldRetry && !checkSSHConnectity(instance)) {
			sleep();
			shouldRetry = retries++ < MAX_RETRIES;
		}
		
		if (!shouldRetry) {
			terminate(request, extraParams);
			observer.onJobLost();			
			return null;
		}
		
		stageAndExecInBg(jobCommand, extraParams, request, instance, observer);
		
		return new FogbowJobData(request.getId(), 
				request.getInstanceId(), extraParams);
	}

	private void stageAndExecInBg(final String jobCommand,
			final Map<String, String> extraParams, final Request request, 
			final Instance instance, final JobObserver observer) {
		EXECUTOR.execute(new Runnable() {
			@Override
			public void run() {
				stageAndExec(jobCommand, extraParams, request, instance, observer);
			}
		});
	}
	
	private void stageAndExec(String jobCommand,
			Map<String, String> extraParams, Request request, 
			Instance instance, JobObserver observer) {
		
		String privKey = pluginProperties.getProperty(PROP_SSH_PRIVATE_KEY_PATH);
		String sshUserName = pluginProperties.getProperty(PROP_SSH_USER_NAME);
		try {
			stageFiles(new JSONObject(extraParams.get(PROP_INPUT_FILES)), 
					instance, privKey, sshUserName, true);
		} catch (SGADataTransferException e) {
			LOGGER.log(Level.WARNING, "Couldn't transfer files to remote VM", e);
			terminate(request, extraParams);
			observer.onJobLost();
			return;
		}
		
		try {
			exec(jobCommand, request, instance, privKey, sshUserName);
		} catch (Exception e) {
			LOGGER.log(Level.WARNING, "Failure during remote execution", e);
			terminate(request, extraParams);
			observer.onJobLost();
		}
	}

	private void exec(String jobCommand, Request request, Instance instance, 
			String privKey, String sshUserName) throws IOException {
		SSHClientWrapper wrapper = new SSHClientWrapper();
		wrapper.connect(instance.getSshHost(), instance.getSshPort(), 
				sshUserName, privKey);
		wrapper.doSshExecution(String.format(REMOTE_COMMAND_LINE, 
				jobCommand, getDoneFile(request.getId())));
		wrapper.disconnect();
	}

	private String getDoneFile(String requestId) {
		return REMOTE_SANDBOX + requestId + ".done";
	}

	private void stageFiles(JSONObject inputsJson, 
			Instance instance, String privKey, 
			String sshUserName, boolean in) throws SGADataTransferException {
		
		SSHSGADataTransfer dataTransfer = new SSHSGADataTransfer(
				instance.getSshHost(), instance.getSshPort(), 
				sshUserName, privKey);
		
		for (String localFilePath : inputsJson.keySet()) {
			String remoteFilePath = REMOTE_SANDBOX + inputsJson.optString(localFilePath);
			if (in) {
				dataTransfer.copyTo(split(localFilePath), split(remoteFilePath));
			} else {
				dataTransfer.copyFrom(split(remoteFilePath), split(localFilePath));
			}
		}
	}

	private static String[] split(String filePath) {
		return filePath.split("/");
	}
	
	private void terminate(Request request, Map<String, String> extraParams) {
		if (request == null) {
			return;
		}
		
		FogbowClient fogbowClient = new FogbowClient(
				pluginProperties.getProperty(PROP_MANAGER_HOST), 
				Integer.parseInt(pluginProperties.getProperty(PROP_MANAGER_PORT)), 
				extraParams.get(PROP_FEDERATION_TOKEN));
		try {
			fogbowClient.deleteRequest(request.getId());
		} catch (Exception e) {
			LOGGER.log(Level.WARNING, "Couldn't delete request", e);
			// Best effort here
		}
		if (request.getInstanceId() != null) {
			try {
				fogbowClient.deleteInstance(request.getInstanceId());
			} catch (Exception e) {
				LOGGER.log(Level.WARNING, "Couldn't delete instance", e);
				// Best effort here
			}
		}
	}

	protected boolean checkSSHConnectity(Instance instance) {
		SSHClientWrapper wrapper = new SSHClientWrapper();
    	try {
    		wrapper.connect(instance.getSshHost(), instance.getSshPort());
		} catch (Exception e) {
			return false;
		} finally {
			try {
				wrapper.disconnect();
			} catch (IOException e) {
			}
		}
		return true;
	}
	
	private Instance getInstanceInfo(FogbowClient fogbowClient, Request request) {
		Instance instance = null;
		try {
			instance = fogbowClient.getInstance(request.getInstanceId());
			if (instance.getSshHost() != null) {
				return instance;
			}
		} catch (Exception e) {
			LOGGER.log(Level.WARNING, "Couldn't retrieve Instance info from the Fogbow manager", e);
		}
		return null;
	}

	private void sleep() {
		try {
			Thread.sleep(RETRY_INTERVAL);
		} catch (InterruptedException e) {
		}
	}

	private Request getRequestInfo(FogbowClient fogbowClient, String requestId) {
		Request request = null;
		try {
			request = fogbowClient.getRequest(requestId);
			if (request.getInstanceId() != null) {
				return request;
			}
		} catch (Exception e) {
			LOGGER.log(Level.WARNING, "Couldn't retrieve Request info from the Fogbow manager", e);
		}
		return null;
	}

	@Override
	public boolean retrieveJob(JobData data, JobObserver observer) {
		
		FogbowJobData fogbowJobData = (FogbowJobData) data;
		Map<String, String> extraParams = fogbowJobData.getExtraParams();
		FogbowClient fogbowClient = new FogbowClient(
				pluginProperties.getProperty(PROP_MANAGER_HOST), 
				Integer.parseInt(pluginProperties.getProperty(PROP_MANAGER_PORT)), 
				extraParams.get(PROP_FEDERATION_TOKEN));
		Instance instance = null;
		try {
			instance = fogbowClient.getInstance(fogbowJobData.getInstanceId());
		} catch (Exception e) {
			terminate(fogbowJobData);
			return false;
		}
		if (instance.getSshHost() == null || 
				instance.getSshPort() == null) {
			terminate(fogbowJobData);
			return false;
		}
		
		String sshUserName = pluginProperties.getProperty(PROP_SSH_USER_NAME);
		String privKey = pluginProperties.getProperty(PROP_SSH_PRIVATE_KEY_PATH);
			
		SSHSGADataTransfer dataTransfer = new SSHSGADataTransfer(
				instance.getSshHost(), instance.getSshPort(), 
				sshUserName, privKey);
		boolean doneFileExists = false;
		try {
			doneFileExists = dataTransfer.checkExistence(getDoneFile(
					fogbowJobData.getRequestId()).split("/"));
		} catch (SGADataTransferException e) {
			terminate(fogbowJobData);
			return false;
		}
		
		if (doneFileExists) {
			
			try {
				stageFiles(new JSONObject(extraParams.get(PROP_OUTPUT_FILES)), 
						instance, privKey, sshUserName, false);
			} catch (SGADataTransferException e) {
				LOGGER.log(Level.WARNING, "Couldn't transfer files back from remote VM", e);
				terminate(fogbowJobData);
				return false;
			}
			
			terminate(fogbowJobData);
			JobInfo jobInfo = new JobInfo();
			jobInfo.jobParam.put(COMMAND_STATE.value, ProcessState.FINISHED.toString());
			observer.onJobCompleted(jobInfo);
		}
		
		return true;
	}

	private void terminate(FogbowJobData fogbowJobData) {
		terminate(new Request(fogbowJobData.getRequestId(), 
				fogbowJobData.getInstanceId()), fogbowJobData.getExtraParams());
	}

	@Override
	public void controlJob(JobData data, String child, JobControlAction action)
			throws InvalidActionException, ActionNotSupportedException {
		if (JobControlAction.TERMINATE.equals(action)) {
			FogbowJobData fogbowJobData = (FogbowJobData) data;
			terminate(fogbowJobData);
			return;
		}
		
		throw new ActionNotSupportedException();
	}

	@Override
	public JobInfo getJobInfo(JobData data) {
		JobInfo jobInfo = new JobInfo();
		jobInfo.jobParam.put(COMMAND_STATE.value, ProcessState.RUNNING.toString());
		return jobInfo;
	}
}
