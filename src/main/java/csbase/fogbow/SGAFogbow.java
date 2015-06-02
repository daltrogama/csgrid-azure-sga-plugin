package csbase.fogbow;

import csbase.server.plugin.service.IServiceManager;
import csbase.sga.SGALocal;

public class SGAFogbow extends SGALocal {

	public SGAFogbow(IServiceManager serviceManager) {
		super(serviceManager);
	}

	@Override
	protected void init() {
		setExecutor(new FogbowExecutor(pluginProperties));
		setMonitor(new FogbowMonitor());
	}

}
