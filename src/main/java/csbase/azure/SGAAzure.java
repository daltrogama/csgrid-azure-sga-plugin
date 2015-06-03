package csbase.azure;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import javax.xml.parsers.ParserConfigurationException;

import org.xml.sax.SAXException;

import com.microsoft.windowsazure.Configuration;
import com.microsoft.windowsazure.core.DefaultBuilder;
import com.microsoft.windowsazure.exception.ServiceException;
import com.microsoft.windowsazure.management.compute.ComputeManagementClient;
import com.microsoft.windowsazure.management.compute.ComputeManagementService;
import com.microsoft.windowsazure.management.compute.models.HostedServiceListResponse.HostedService;
import com.microsoft.windowsazure.management.configuration.ManagementConfiguration;

import csbase.server.plugin.service.IServiceManager;
import csbase.sga.SGALocal;

public class SGAAzure extends SGALocal {

	public SGAAzure(IServiceManager serviceManager) {
		super(serviceManager);
	}

	@Override
	protected void init() {
		// Get current context class loader
		ClassLoader contextLoader = Thread.currentThread().getContextClassLoader();
		System.out.println("Classloader: "+contextLoader);
		// Change context classloader to class context loader   
		Thread.currentThread().setContextClassLoader(ComputeManagementService.class.getClassLoader()); 
		
		setExecutor(new AzureExecutor(pluginProperties));
		setMonitor(new AzureMonitor());
		
//		System.out.println("SGAAzure.init!");
//		try{
//			Configuration azureConfig;
//			try {
//				azureConfig = ManagementConfiguration.configure(
//						new URI(pluginProperties.getProperty("managementURI")), 
//						pluginProperties.getProperty("subscriptionId"), 
//						pluginProperties.getProperty("keyStorePath"), 
//						pluginProperties.getProperty("keyStorePassword"));
//				
//			} catch (IOException | URISyntaxException e) {
//				throw new IllegalStateException("Não foi possível configurar Azure!", e);
//			}
//			
//			ComputeManagementClient service = ComputeManagementService.create(azureConfig);
//			
//			try {
//				for (HostedService hs : service.getHostedServicesOperations().list().getHostedServices()){
//					System.out.println("CloudService: "+hs.getServiceName()+"\t "+hs.getProperties());
//				}
//			} catch (IOException | ServiceException | ParserConfigurationException
//					| SAXException | URISyntaxException e) {
//				throw new IllegalStateException("Não foi possível listar cloud services", e);
//			}
//	
//		}
//		catch(Throwable tte){
//			tte.printStackTrace(System.err);
//		}
	}

}
