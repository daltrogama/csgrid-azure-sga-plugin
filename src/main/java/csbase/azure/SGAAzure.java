package csbase.azure;

import csbase.server.plugin.service.IServiceManager;
import csbase.sga.SGALocal;

public class SGAAzure extends SGALocal {

	public SGAAzure(IServiceManager serviceManager) {
		super(serviceManager);
	}

	@Override
	protected void init() {
		setExecutor(new AzureExecutor(pluginProperties));
		setMonitor(new AzureMonitor());
	}

}
