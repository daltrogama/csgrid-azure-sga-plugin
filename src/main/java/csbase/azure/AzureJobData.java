package csbase.azure;

import java.util.Map;

import csbase.sga.executor.JobData;

public class AzureJobData implements JobData {

	private static final long serialVersionUID = 1L;
	
	private String requestId;
	private String instanceId;
	
	private Map<String, String> extraParams;

	public AzureJobData(String requestId, String instanceId,
			Map<String, String> extraParams) {
		this.requestId = requestId;
		this.instanceId = instanceId;
		this.extraParams = extraParams;
	}

	public String getRequestId() {
		return requestId;
	}

	public void setRequestId(String requestId) {
		this.requestId = requestId;
	}

	public String getInstanceId() {
		return instanceId;
	}

	public void setInstanceId(String instanceId) {
		this.instanceId = instanceId;
	}

	public Map<String, String> getExtraParams() {
		return extraParams;
	}

	public void setExtraParams(Map<String, String> extraParams) {
		this.extraParams = extraParams;
	}

	
}
