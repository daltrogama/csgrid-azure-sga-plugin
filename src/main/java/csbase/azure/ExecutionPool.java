package csbase.azure;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.JSONObject;

import sgaidl.ActionNotSupportedException;
import sgaidl.COMMAND_STATE;
import sgaidl.CompletedCommandInfo;
import sgaidl.InvalidActionException;
import sgaidl.InvalidCommandException;
import sgaidl.InvalidSGAException;
import sgaidl.InvalidTransitionException;
import sgaidl.JobControlAction;
import sgaidl.NoPermissionException;
import sgaidl.ProcessState;
import sgaidl.SGACommand;
import csbase.server.plugin.service.sgaservice.ISGAService;

class ExecutionPool {

	protected Logger logger = Logger.getLogger(this.getClass().getName());

	/** Mapa com os identificadores de commando e suas referências */
	private final Map<String, SGAAzureCommand> commands = Collections.synchronizedMap(new HashMap<String, SGAAzureCommand>());
	
	private final AzureConnector azure;
	private final ISGAService sgaService;
	private final String sgaName;
	
	private final Executor asyncExecutors = Executors.newCachedThreadPool();
	
	private final Thread statusRetrieveThread = new Thread(){
		public void run() {
			try{
				while(!Thread.currentThread().isInterrupted()){
					try{
						JSONObject obj = azure.receiveStatus();
						if (obj == null)
							continue;
						
						String cmdId = obj.getString("cmdId");
						String status = obj.getString("status");
						String vmName = obj.getString("vmName");
						
						System.out.println("Mensagem de status: cmdId="+cmdId+", status="+status+", vmName="+vmName);
						
						SGAAzureCommand command = commands.get(cmdId);
						if (command == null){
							logger.log(Level.WARNING, "Status de comando não monitorado recebido: "+cmdId+": "+status);
							continue;
						}
						synchronized(command){
							command.setExecutingVMName(vmName);
							if ("Uploading".equals(status)){
								command.getJobInfo().jobParam.put(COMMAND_STATE.value, ProcessState.WAITING.toString());
							} else if ("Running".equals(status)){
								command.getJobInfo().jobParam.put(COMMAND_STATE.value, ProcessState.RUNNING.toString());
							} else if ("Downloading".equals(status)){
								command.getJobInfo().jobParam.put(COMMAND_STATE.value, ProcessState.RUNNING.toString());
							} else if ("Ended".equals(status)){
								command.getJobInfo().jobParam.put(COMMAND_STATE.value, ProcessState.FINISHED.toString());
								final String fCmdID = cmdId;
								final SGAAzureCommand fCmd = command;
								asyncExecutors.execute(new Runnable() {
									@Override
									public void run() {
										CompletedCommandInfo info = new CompletedCommandInfo();
										info.cpuTimeSec = -1;
										info.elapsedTimeSec = -1;
										info.userTimeSec = -1;
										commands.remove(fCmdID);
										System.out.println("Comando finalizado!");
										try {
											sgaService.commandCompleted(sgaName, fCmd, fCmdID, info);
										} catch (InvalidSGAException e) {
											// TODO Auto-generated catch block
											e.printStackTrace();
										} catch (NoPermissionException e) {
											// TODO Auto-generated catch block
											e.printStackTrace();
										} catch (InvalidCommandException e) {
											// TODO Auto-generated catch block
											e.printStackTrace();
										}
									}
								});
							}
						}
						
					}catch(Throwable e){
						logger.log(Level.INFO, "Falha ao recuperar status do tópico de status. Tentarei novamente.", e);
						Thread.sleep(10000);
					}
					Thread.sleep(1);
					
				}
			}
			catch(InterruptedException ie){
				
			}
		};
	};
	
	public ExecutionPool(Properties pluginProperties, AzureConnector azure, ISGAService sgaService, String sgaName) {
		this.azure = azure;
		this.sgaService = sgaService;
		this.sgaName = sgaName;
		this.statusRetrieveThread.start();
	}

	public SGACommand executeCommand(String command, String cmdid, Map<String, String> extraParams) {
		SGAAzureCommand newCommand = new SGAAzureCommand(command, cmdid, extraParams, this);
		System.out.println(newCommand.getJSONCommandDescription());
		
		commands.put(cmdid, newCommand);
		
		this.azure.sendNewCommand(newCommand.getJSONCommandDescription());
		
		return newCommand;
	}

	public void controlRunningJob(JobControlAction action, String childJobId) throws InvalidActionException, ActionNotSupportedException,	InvalidTransitionException {
		// TODO Auto-generated method stub
		
	}

}
