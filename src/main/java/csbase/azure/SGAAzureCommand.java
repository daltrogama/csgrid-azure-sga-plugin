package csbase.azure;

import java.util.HashMap;
import java.util.Map;

import sgaidl.ActionNotSupportedException;
import sgaidl.InvalidActionException;
import sgaidl.InvalidTransitionException;
import sgaidl.JobControlAction;
import sgaidl.RunningCommandInfo;
import csbase.server.plugin.service.sgaservice.SGADaemonCommand;

@SuppressWarnings("serial")
public class SGAAzureCommand extends SGADaemonCommand{
	
	private final String command;
	
	private final String cmdid;
	
	private final Map<String, String> extraParams = new HashMap<>();
	
	private final ExecutionPool pool;
	
	/**
	 * Nome da máquina virtual que assumiu o job.
	 * Null indica que nenhuma ainda o assumiu.
	 */
	private String executingVMName = null;
	
	public SGAAzureCommand(String command, String cmdid, Map<String, String> extraParams, ExecutionPool pool) {
		super();
		this.command = command;
		this.cmdid = cmdid;
		this.extraParams.putAll(extraParams);
		this.pool = pool;
	}

	@Override
	public RunningCommandInfo getRunningCommandInfo() {
		return pool.getRunningCommandInfo(this);
	}

	@Override
	public void control(JobControlAction action, String childJobId)	throws InvalidActionException, ActionNotSupportedException,	InvalidTransitionException {
		pool.controlRunningJob(action, childJobId);		
	}
	

	public String getExecutingVMName() {
		return executingVMName;
	}

	public void setExecutingVMName(String executingVMName) {
		this.executingVMName = executingVMName;
	}

	public String getCommand() {
		return command;
	}

	public String getCmdid() {
		return cmdid;
	}

	public Map<String, String> getExtraParams() {
		return extraParams;
	}
	
}
