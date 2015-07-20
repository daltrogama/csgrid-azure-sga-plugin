package csbase.azure;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import sgaidl.ActionNotSupportedException;
import sgaidl.InvalidActionException;
import sgaidl.InvalidTransitionException;
import sgaidl.JobControlAction;
import sgaidl.RunningCommandInfo;
import sgaidl.SGACommand;

class ExecutionPool {

	/** Mapa com os identificadores de commando e suas referências */
	private final Map<String, SGAAzureCommand> commands = new HashMap<>();
	
	public ExecutionPool(Properties pluginProperties, AzureConnector azure) {
		// TODO Auto-generated constructor stub
	}

	public SGACommand executeCommand(String command, String cmdid, Map<String, String> convertDicToMap) {
		// TODO Auto-generated method stub
		return null;
	}

	public RunningCommandInfo getRunningCommandInfo(SGAAzureCommand sgaAzureCommand) {
		// TODO Auto-generated method stub
		return null;
	}

	public void controlRunningJob(JobControlAction action, String childJobId) throws InvalidActionException, ActionNotSupportedException,	InvalidTransitionException {
		// TODO Auto-generated method stub
		
	}

	
	
	
}
