package csbase.azure;

public class VMInstance {
	
	private final String vmName;
	private final String csName;
	private final String sizeName;
	public String getVmName() {
		return vmName;
	}
	public String getCsName() {
		return csName;
	}
	public String getSizeName() {
		return sizeName;
	}
	public VMInstance(String vmName, String csName, String sizeName) {
		super();
		this.vmName = vmName;
		this.csName = csName;
		this.sizeName = sizeName;
	}
	
	
	
	
	
}
