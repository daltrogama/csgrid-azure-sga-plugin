package csbase.azure;

import java.io.IOException;
import java.io.StringWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.codec.binary.Base64;
import org.xml.sax.SAXException;

import com.microsoft.windowsazure.Configuration;
import com.microsoft.windowsazure.core.OperationResponse;
import com.microsoft.windowsazure.core.OperationStatusResponse;
import com.microsoft.windowsazure.exception.ServiceException;
import com.microsoft.windowsazure.management.compute.ComputeManagementClient;
import com.microsoft.windowsazure.management.compute.ComputeManagementService;
import com.microsoft.windowsazure.management.compute.models.ConfigurationSet;
import com.microsoft.windowsazure.management.compute.models.ConfigurationSetTypes;
import com.microsoft.windowsazure.management.compute.models.HostedServiceCreateParameters;
import com.microsoft.windowsazure.management.compute.models.HostedServiceGetDetailedResponse;
import com.microsoft.windowsazure.management.compute.models.HostedServiceListResponse;
import com.microsoft.windowsazure.management.compute.models.OSVirtualHardDisk;
import com.microsoft.windowsazure.management.compute.models.Role;
import com.microsoft.windowsazure.management.compute.models.VirtualHardDiskHostCaching;
import com.microsoft.windowsazure.management.compute.models.VirtualMachineCreateParameters;
import com.microsoft.windowsazure.management.compute.models.VirtualMachineGetResponse;
import com.microsoft.windowsazure.management.compute.models.VirtualMachineRoleType;
import com.microsoft.windowsazure.management.configuration.ManagementConfiguration;

class AzureConnector {
	
	/*
	 * 
	 * Inspiração para comandos Azure:
	 * https://github.com/Azure/azure-sdk-for-java/blob/master/management-compute/src/test/java/com/microsoft/windowsazure/management/compute/VirtualMachineOperationsTests.java
	 * 
	 */
	
	
	private final Logger logger = Logger.getLogger(this.getClass().getName());
	private static final Random rnd = new Random(System.currentTimeMillis());

	Configuration azureConfig;
	ComputeManagementClient vmService;
	
	// Configurações
	private String stuffLocation = "East US";
	private String adminuserPassword = "csgrid123*";
	private String adminUserName = "csgrid";
	
	//TODO: Preencher isso
	private String diskStorageAccountName = "";
	private String diskStorageContainer = "";
	
	private Map<String, VMInstance> virtualMachines = Collections.synchronizedMap(new HashMap<String, VMInstance>());

	public AzureConnector(Properties pluginProperties){

		System.out.println("SGAAzure.init!");
		try{
			azureConfig = ManagementConfiguration.configure(
					new URI(pluginProperties.getProperty("managementURI")), 
					pluginProperties.getProperty("subscriptionId"), 
					pluginProperties.getProperty("keyStorePath"), 
					pluginProperties.getProperty("keyStorePassword"));
			azureConfig.setProperty("management.keystore.type", "jks");

			vmService = ComputeManagementService.create(azureConfig);

		}
		catch(Throwable tte){
			throw new IllegalStateException("Não foi possível inicializar SGA Azure com as configurações fornecidas: "+pluginProperties.toString(), tte);
		}

	}

	public void createNewVirtualMachine(String size) throws InterruptedException, URISyntaxException{
		byte[] rndName = new byte[64];
		rnd.nextBytes(rndName);
		String name = "csgrid-sga"+Base64.encodeBase64String(rndName);
		
		// Criar primeiro o serviço de nuvem
		{
			boolean ok = false;
	        //hosted service required for vm deployment
	        HostedServiceCreateParameters createParameters = new HostedServiceCreateParameters(); 
	        //required
	        createParameters.setLabel(name);
	        //required
	        createParameters.setServiceName(name);
	        createParameters.setDescription("Auto-generated SGA from CSGrid");
	        //required
	        createParameters.setLocation(stuffLocation);
			for (int retry=0; retry<10; retry+=1){
				try{
			        OperationResponse hostedServiceOperationResponse = vmService.getHostedServicesOperations().create(createParameters);
			        
			        if (hostedServiceOperationResponse.getStatusCode() != 201){
			        	logger.log(Level.FINE, "Azure respondeu requisição de criação de serviço de nuvem "+name+" com código HTTP != 201: "+hostedServiceOperationResponse.getStatusCode()+".");
			        	ok=false;
			        	Thread.sleep(2000+rnd.nextInt(2000));
			        	continue; // retry
			        }
			        
				}
				catch(Throwable e){
					if (e instanceof InterruptedException)
						throw (InterruptedException)e;
					
					logger.log(Level.FINE, "Erro na requisição de criação de serviço de nuvem "+name, e);
		        	ok=false;
		        	Thread.sleep(2000+rnd.nextInt(2000));
		        	continue; // retry
				}
		        
		        ok=true;
		        break;
			}
			
			if (!ok){
				logger.log(Level.SEVERE, "Não foi possível criar novo serviço de nuvem "+name);
				return;
			}
		}
		
        URI mediaLinkUriValue =  new URI("http://"+ diskStorageAccountName + ".blob.core.windows.net/"+diskStorageContainer+ "/" + name + ".vhd");
        String osVHarddiskName = name + "oshd";
        String operatingSystemName ="Linux";

        //required
        ArrayList<ConfigurationSet> configlist = createConfigList(name, size, adminuserPassword, adminUserName);

        //required
        String sourceImageName = getOSSourceImage();
        OSVirtualHardDisk oSVirtualHardDisk = createOSVirtualHardDisk(osVHarddiskName, operatingSystemName, mediaLinkUriValue, sourceImageName);
        VirtualMachineCreateParameters createParameters = createVirtualMachineCreateParameter(name, configlist, oSVirtualHardDisk, null, size);
        boolean ok = false;
		for (int retry=0; retry<10; retry+=1){
			try{
		        //Act
		        OperationResponse operationResponse = vmService.getVirtualMachinesOperations().create(name, name, createParameters);
		        if (operationResponse.getStatusCode() != 200){
		        	logger.log(Level.FINE, "Azure respondeu requisição de criação de serviço de nuvem "+name+" com código HTTP != 200: "+operationResponse.getStatusCode()+".");
		        	ok=false;
		        	Thread.sleep(2000+rnd.nextInt(2000));
		        	continue; // retry
		        }
			}
			catch(Throwable e){
				if (e instanceof InterruptedException)
					throw (InterruptedException)e;
				logger.log(Level.FINE, "Erro na requisição de criação de serviço de nuvem "+name, e);
	        	ok=false;
	        	Thread.sleep(2000+rnd.nextInt(2000));
	        	continue; // retry
			}
	        
	        ok=true;
	        break;
		}
		if (!ok){
			logger.log(Level.SEVERE, "Não foi possível criar nova máquina virtual "+name+" com tamanho "+size);
			return;
		}

	}
	
	public void deleteVirtualMachine(String name) throws InterruptedException{
		
		boolean ok = false;
		OperationStatusResponse virtualMachinesGetResponse;
		for (int retry=0; retry<10; retry+=1){
			try{
		        //Act
				virtualMachinesGetResponse = vmService.getHostedServicesOperations().deleteAll(name);
		        if (virtualMachinesGetResponse.getStatusCode() != 200){
		        	logger.log(Level.FINE, "Azure respondeu requisição de recuperação de dados sobre máquina virtual "+name+" com código HTTP != 200: "+virtualMachinesGetResponse.getStatusCode()+".");
		        	ok=false;
		        	Thread.sleep(2000+rnd.nextInt(2000));
		        	continue; // retry
		        }
			}
			catch(Throwable e){
				if (e instanceof InterruptedException)
					throw (InterruptedException)e;
				logger.log(Level.FINE, "Erro na requisição de criação de serviço de nuvem "+name, e);
	        	ok=false;
	        	Thread.sleep(2000+rnd.nextInt(2000));
	        	continue; // retry
			}
		}
		
		if (!ok){
			logger.log(Level.FINE, "Erro na requisição de informações sobre máquina virtual "+name+". Máquina não foi excluída.");
			return;
		}

	}
	
	
    private VirtualMachineCreateParameters createVirtualMachineCreateParameter(String roleName, ArrayList<ConfigurationSet> configlist, OSVirtualHardDisk oSVirtualHardDisk, String availabilitySetNameValue, String size) {
        VirtualMachineCreateParameters createParameters = new VirtualMachineCreateParameters();
        //required       
        createParameters.setRoleName(roleName);
        createParameters.setRoleSize(size);
        createParameters.setProvisionGuestAgent(true);
        createParameters.setConfigurationSets(configlist);       
        createParameters.setOSVirtualHardDisk(oSVirtualHardDisk);
        createParameters.setAvailabilitySetName(availabilitySetNameValue);        
        return createParameters;
    }
    
    private String getOSSourceImage() {
    	// Sistema operacional:
    	// TODO: Usar imagem própria
    	return "b39f27a8b8c64d52b05eac6a62ebad85__Ubuntu-12_04_2-LTS-amd64-server-20130225-en-us-30GB";
    	
//        String sourceImageName = null;
//        VirtualMachineOSImageListResponse virtualMachineImageListResponse = vmService.getVirtualMachineOSImagesOperations().list();
//        ArrayList<VirtualMachineOSImageListResponse.VirtualMachineOSImage> virtualMachineOSImagelist = virtualMachineImageListResponse.getImages();
//        PrintWriter out = new PrintWriter(new File("PublicVMs.txt"));
//        //Assert.assertNotNull(virtualMachineOSImagelist);
//        for (VirtualMachineOSImageListResponse.VirtualMachineOSImage virtualMachineImage : virtualMachineOSImagelist) {
//        	out.println(ReflectionToStringBuilder.toString(virtualMachineImage));
//        }
//        out.flush();
//        out.close();
//        //Assert.assertNotNull(sourceImageName);
//        return sourceImageName;
    }
	
    private OSVirtualHardDisk createOSVirtualHardDisk(String osVHarddiskName, String operatingSystemName, URI mediaLinkUriValue, String sourceImageName)
    {
        OSVirtualHardDisk oSVirtualHardDisk = new OSVirtualHardDisk(); 
        //required
        oSVirtualHardDisk.setName(osVHarddiskName);
        oSVirtualHardDisk.setHostCaching(VirtualHardDiskHostCaching.READWRITE);
        oSVirtualHardDisk.setOperatingSystem(operatingSystemName);
        //required
        oSVirtualHardDisk.setMediaLink(mediaLinkUriValue);
        //required
        oSVirtualHardDisk.setSourceImageName(sourceImageName);
        return oSVirtualHardDisk;
    }

	
    private ArrayList<ConfigurationSet> createConfigList(String computerName, String size, String adminuserPassword, String adminUserName) {
        ArrayList<ConfigurationSet> configlist = new ArrayList<ConfigurationSet>();
        ConfigurationSet configset = new ConfigurationSet();
        configset.setConfigurationSetType(ConfigurationSetTypes.LINUXPROVISIONINGCONFIGURATION);
        //required
        configset.setComputerName(computerName);
        //required
        configset.setAdminPassword(adminuserPassword);
        //required
        configset.setAdminUserName(adminUserName);
        configset.setEnableAutomaticUpdates(false);
        
        // Parâmetros passados para a VM
        Properties customDataProps = new Properties();
        customDataProps.put("vmname", computerName);
        customDataProps.put("size", size);
        
        StringWriter propsOut = new StringWriter();
        try {
			customDataProps.store(propsOut, "Parâmetros gerados automaticamente pelo SGA CSgrid");
		} catch (IOException e) {
			throw new IllegalStateException(e);
		}
        
        configset.setCustomData(propsOut.toString());
        
        configlist.add(configset);
        return configlist;
    }
    
	
	
	private AtomicBoolean refreshAllVMsLock = new AtomicBoolean(false);
	
	public void refreshAllVMs() throws InterruptedException{
		
		if (refreshAllVMsLock.getAndSet(true)){
			// Há outra thread fazendo isso...
			while(refreshAllVMsLock.get()){
				Thread.sleep(10);
			}
			// Quando a outra thread terminar, considera esta terminada também.
			return;
		}
		try{
		
			List<String> discovered = new LinkedList<>();
	
			for (int retry=0; retry<10; retry+=1){
				discovered.clear();
				
				//there is no dedicated vm list methods, has to filter through hosted service, and deployment, rolelist to find out the vm list
				//role that has VirtualMachineRoleType.PersistentVMRole property is a vm
				try {
					HostedServiceListResponse hostedServiceListResponse;
					hostedServiceListResponse = vmService.getHostedServicesOperations().list();
					ArrayList<HostedServiceListResponse.HostedService> hostedServicelist = hostedServiceListResponse.getHostedServices();
					
					for (HostedServiceListResponse.HostedService hostedService : hostedServicelist) {
						if (hostedService.getServiceName().startsWith("csgrid-sga")) {
							
							String name = hostedService.getServiceName();
							
							discovered.add(name);
							
							VMInstance vmInstance = virtualMachines.get(name);
							
							if (vmInstance == null){
			
								HostedServiceGetDetailedResponse hostedServiceGetDetailedResponse 
									= vmService.getHostedServicesOperations().getDetailed(hostedService.getServiceName());
			
								ArrayList<HostedServiceGetDetailedResponse.Deployment> deploymentlist = hostedServiceGetDetailedResponse.getDeployments();
			
								for (HostedServiceGetDetailedResponse.Deployment deployment : deploymentlist) {
									ArrayList<Role> rolelist = deployment.getRoles();
			
									for (Role role : rolelist) {
										if ((role.getRoleType()!=null) && (role.getRoleType().equalsIgnoreCase(VirtualMachineRoleType.PersistentVMRole.toString()))) {
											virtualMachines.put(name, new VMInstance(name, name, role.getRoleSize()));
										}
									}
								}
								
							}
						}
					}
					
					for (String listedVM : virtualMachines.keySet()){
						if (!discovered.contains(listedVM))
							virtualMachines.remove(listedVM);
					}
					
					if (retry > 0)
						logger.info("A atualização das máquinas virtuais virtuais foi bem sucedida.");
					
					return; // Ok.
				} catch (IOException | ServiceException | ParserConfigurationException
						| SAXException | URISyntaxException e) {
					logger.log(Level.WARNING, "Falha ao recuperar a lista de todas as máquinas virtuais. Tentando de novo ("+retry+")", e);
				}
				Thread.sleep(5000);
			}
			
			logger.log(Level.SEVERE, "Falha ao recuperar a lista de todas as máquinas virtuais após dez tentativas.");
		}
		finally{
			refreshAllVMsLock.set(false);
		}
	}

	public Map<String, VMInstance> getAllVMs() {
		return Collections.unmodifiableMap(virtualMachines);
	}

}
