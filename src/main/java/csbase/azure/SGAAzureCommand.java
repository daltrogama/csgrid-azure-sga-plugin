package csbase.azure;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.json.JSONObject;

import sgaidl.ActionNotSupportedException;
import sgaidl.InvalidActionException;
import sgaidl.InvalidTransitionException;
import sgaidl.JobControlAction;
import sgaidl.Pair;
import sgaidl.RunningCommandInfo;
import csbase.server.plugin.service.sgaservice.SGADaemonCommand;

@SuppressWarnings("serial")
public class SGAAzureCommand extends SGADaemonCommand{
	
	private final String command;
	
	private final String cmdid;
	
	private final Map<String, String> extraParams = new HashMap<>();
	
	private final ExecutionPool pool;
		
	private final JobInfo jobInfo;
	
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
		this.jobInfo = new JobInfo();
	}

	@Override
	public RunningCommandInfo getRunningCommandInfo() {
		return convertJobInfoToRunningCommandInfo(this.jobInfo);
	}

	@Override
	public void control(JobControlAction action, String childJobId)	throws InvalidActionException, ActionNotSupportedException,	InvalidTransitionException {
		pool.controlRunningJob(action, childJobId);		
	}
	
	public JobInfo getJobInfo() {
		return jobInfo;
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
	
	public String getJSONCommandDescription(){
		/*
command=/bin/ksh /AzureBlobStorage/sandbox/admin/tester/admi_test_CBJ6LTHDWM/.cmds/admi_test_CBJ6LTHDWM/script.ksh
cmdid=admi@test.CBJ6LTHDWM
params={
  csbase_command_sandbox_paths.1=admi_test_CBJ6LTHDWM
  csbase_command_path=/AzureBlobStorage/algorithms/TATU/versions/v_001_000_000/bin/Linux26g4_64
  fogbow_output_files_path={
    "/Users/daltrogama/Documents/workspace.csbase/csgrid-trunk/_project/admin/tester/.cmds/admi_test_CBJ6LTHDWM/logs":"sandbox/admin/tester/admi_test_CBJ6LTHDWM/.cmds/admi_test_CBJ6LTHDWM/logs"
  }
  fogbow_input_files_path={
    "/Users/daltrogama/Documents/workspace.csbase/csgrid-trunk/_project/admin/tester/.cmds/admi_test_CBJ6LTHDWM/cmd.parameters":"sandbox/admin/tester/admi_test_CBJ6LTHDWM/.cmds/admi_test_CBJ6LTHDWM/cmd.parameters"
    "/Users/daltrogama/Documents/workspace.csbase/csgrid-trunk/_project/admin/tester/tatu-a-b-lab-times.pdf":"sandbox/admin/tester/admi_test_CBJ6LTHDWM/tatu-a-b-lab-times.pdf"
    "/Users/daltrogama/Documents/workspace.csbase/csgrid-trunk/_project/admin/tester/.cmds/admi_test_CBJ6LTHDWM/script.ksh":"sandbox/admin/tester/admi_test_CBJ6LTHDWM/.cmds/admi_test_CBJ6LTHDWM/script.ksh"
    "/Users/daltrogama/Documents/workspace.csbase/csgrid-trunk/_project/admin/tester/.cmds/admi_test_CBJ6LTHDWM/cmd.properties":"sandbox/admin/tester/admi_test_CBJ6LTHDWM/.cmds/admi_test_CBJ6LTHDWM/cmd.properties"
    "/Users/daltrogama/Documents/workspace.csbase/csgrid-trunk/algorithms/TATU/versions/v_001_000_000/bin/Linux26g4_64":"algorithms/TATU/versions/v_001_000_000/bin/Linux26g4_64"
  }
  csbase_command_output_path=/AzureBlobStorage/sandbox/admin/tester/admi_test_CBJ6LTHDWM/.cmds/admi_test_CBJ6LTHDWM/logs
}
		 */
		
		
		/*
{
"command_id": "admi@test.CBJ6LTHDWM",
"algorithm_prfx": "algorithms/TATU/versions/v_001_000_000/bin/Linux26g4_64",
"project_prfx": "/AzureBlobStorage/sandbox/admin/tester/admi_test_CBJ6LTHDWM",
"project_input_files": [".cmds/admi_test_CBJ6LTHDWM/cmd.parameters", "tatu-a-b-lab-times.pdf", ".cmds/admi_test_CBJ6LTHDWM/script.ksh", ".cmds/admi_test_CBJ6LTHDWM/cmd.properties"],
"algorithm_executable_name": "/bin/ksh",
"algorithm_parameters": ["/AzureBlobStorage/sandbox/admin/tester/admi_test_CBJ6LTHDWM/.cmds/admi_test_CBJ6LTHDWM/script.ksh"],
"sent_timestamp": "12/01/2015 10:00:00",
"machine_size": "*"
}
		 */
		
		StringBuilder res = new StringBuilder();
		
		res.append("{");
		res.append("\"command_id\": \"" +getCmdid() + "\", \n");
		res.append("\"algorithm_prfx\": \"" + getAlgorithmPrfx() + "\", \n");
		res.append("\"project_prfx\": \"" + getProjectPrfx() + "\", \n");
		res.append("\"project_input_files\": " + getProjectInputFiles() + ", \n");
		res.append("\"algorithm_executable_name\": \"" + getAlgorithmExecutableName() + "\", \n");
		res.append("\"algorithm_parameters\": " + getAlgorithmParameters() + ", \n");
		res.append("\"sent_timestamp\": \"" + getSentTimestamp() + "\", \n");
		res.append("\"machine_size\": \"*\" \n");
		res.append("}");

		return res.toString();
		
	}

	private String getSentTimestamp() {
		SimpleDateFormat fmt = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
		return fmt.format(new Date());
	}

	private String getAlgorithmParameters() {
		StringBuilder jsonStr = new StringBuilder();
		jsonStr.append("[");
		int k=0;
		for (String r : getCommand().split(" ")){
			if (k++ == 0)
				continue; // pula o primeiro
			
			if (jsonStr.length()>1)
				jsonStr.append(",");
			jsonStr.append("\"");
			jsonStr.append(r.replace("\"", "\\\""));
			jsonStr.append("\"");
		}
		jsonStr.append("]");
		return jsonStr.toString();
	}

	private String getAlgorithmExecutableName() {
		return getCommand().split(" ")[0];
	}

	private String getProjectInputFiles() {
		return getProjectInputFiles(getExtraParams().get("fogbow_input_files_path"));
	}
	
	private static String getProjectInputFiles(String fogbow_input_files_path) {
		String prfx = null;
		JSONObject rawInput = new JSONObject(fogbow_input_files_path);
		for (Object key : rawInput.keySet()){
			if (key.toString().contains("/.cmd")){
				prfx = key.toString().substring(0, key.toString().indexOf("/.cmd"));
				break;
			}
		}
		
		if (prfx != null){
			LinkedList<String> res = new LinkedList<>();
			for (Object key : rawInput.keySet()){
				if (key.toString().startsWith(prfx)){
					res.add(key.toString().substring(prfx.length()+1));
				}
			}
			
			StringBuilder jsonStr = new StringBuilder();
			jsonStr.append("[");
			for (String r : res){
				if (jsonStr.length()>1)
					jsonStr.append(",");
				jsonStr.append("\"");
				jsonStr.append(r.replace("\"", "\\\""));
				jsonStr.append("\"");
			}
			jsonStr.append("]");
			
			return jsonStr.toString();
		}
		
		return "[]";
	}

	private String getAlgorithmPrfx() {
		return getExtraParams().get("csbase_command_path").replace("/AzureBlobStorage/", "");
	}

	private String getProjectPrfx() {
		String commandOutputPath = getExtraParams().get("csbase_command_output_path"); 
		return commandOutputPath.substring(0, commandOutputPath.indexOf("/.cmd"));
	}

	private RunningCommandInfo convertJobInfoToRunningCommandInfo(JobInfo jobInfo) {
		List<Pair[]> processData = new LinkedList<Pair[]>();

		List<Pair> mainProcessDic = new LinkedList<Pair>();
		for (String key : jobInfo.jobParam.keySet()) {
			mainProcessDic.add(new Pair(key, jobInfo.jobParam.get(key)));
		}
		processData.add(mainProcessDic.toArray(new Pair[0]));

		for (JobInfo pInfo : jobInfo.children) {
			List<Pair> pDic = new LinkedList<Pair>();
			for (String key : pInfo.jobParam.keySet()) {
				pDic.add(new Pair(key, pInfo.jobParam.get(key)));
			}
			processData.add(pDic.toArray(new Pair[0]));
		}

		return new RunningCommandInfo(processData.toArray(new Pair[0][]), new Pair[0]);
	}


	public static void main(String[] args) {
		System.out.println(getProjectInputFiles("{     \"/Users/daltrogama/Documents/workspace.csbase/csgrid-trunk/_project/admin/tester/.cmds/admi_test_CBJ6LTHDWM/cmd.parameters\":\"sandbox/admin/tester/admi_test_CBJ6LTHDWM/.cmds/admi_test_CBJ6LTHDWM/cmd.parameters\", \"/Users/daltrogama/Documents/workspace.csbase/csgrid-trunk/_project/admin/tester/tatu-a-b-lab-times.pdf\":\"sandbox/admin/tester/admi_test_CBJ6LTHDWM/tatu-a-b-lab-times.pdf\" , \"/Users/daltrogama/Documents/workspace.csbase/csgrid-trunk/_project/admin/tester/.cmds/admi_test_CBJ6LTHDWM/script.ksh\":\"sandbox/admin/tester/admi_test_CBJ6LTHDWM/.cmds/admi_test_CBJ6LTHDWM/script.ksh\" , \"/Users/daltrogama/Documents/workspace.csbase/csgrid-trunk/_project/admin/tester/.cmds/admi_test_CBJ6LTHDWM/cmd.properties\":\"sandbox/admin/tester/admi_test_CBJ6LTHDWM/.cmds/admi_test_CBJ6LTHDWM/cmd.properties\"  , \"/Users/daltrogama/Documents/workspace.csbase/csgrid-trunk/algorithms/TATU/versions/v_001_000_000/bin/Linux26g4_64\":\"algorithms/TATU/versions/v_001_000_000/bin/Linux26g4_64\"   }"));
	}
	
}
