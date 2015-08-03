package csbase.azure;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.security.InvalidKeyException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.xml.sax.SAXException;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlobListingDetails;
import com.microsoft.azure.storage.blob.BlobRequestOptions;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlobDirectory;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.blob.ListBlobItem;
import com.microsoft.windowsazure.Configuration;
import com.microsoft.windowsazure.core.OperationResponse;
import com.microsoft.windowsazure.core.OperationStatusResponse;
import com.microsoft.windowsazure.exception.ServiceException;
import com.microsoft.windowsazure.management.compute.ComputeManagementClient;
import com.microsoft.windowsazure.management.compute.ComputeManagementService;
import com.microsoft.windowsazure.management.compute.models.ConfigurationSet;
import com.microsoft.windowsazure.management.compute.models.ConfigurationSetTypes;
import com.microsoft.windowsazure.management.compute.models.DeploymentSlot;
import com.microsoft.windowsazure.management.compute.models.HostedServiceCreateParameters;
import com.microsoft.windowsazure.management.compute.models.HostedServiceGetDetailedResponse;
import com.microsoft.windowsazure.management.compute.models.HostedServiceListResponse;
import com.microsoft.windowsazure.management.compute.models.OSVirtualHardDisk;
import com.microsoft.windowsazure.management.compute.models.Role;
import com.microsoft.windowsazure.management.compute.models.VirtualHardDiskHostCaching;
import com.microsoft.windowsazure.management.compute.models.VirtualMachineCreateDeploymentParameters;
import com.microsoft.windowsazure.management.compute.models.VirtualMachineRoleType;
import com.microsoft.windowsazure.management.configuration.ManagementConfiguration;
import com.microsoft.windowsazure.management.storage.StorageManagementClient;
import com.microsoft.windowsazure.management.storage.StorageManagementService;
import com.microsoft.windowsazure.management.storage.models.StorageAccountCreateParameters;
import com.microsoft.windowsazure.management.storage.models.StorageAccountGetKeysResponse;
import com.microsoft.windowsazure.management.storage.models.StorageAccountGetResponse;
import com.microsoft.windowsazure.services.servicebus.ServiceBusConfiguration;
import com.microsoft.windowsazure.services.servicebus.ServiceBusContract;
import com.microsoft.windowsazure.services.servicebus.ServiceBusService;
import com.microsoft.windowsazure.services.servicebus.models.ListQueuesResult;
import com.microsoft.windowsazure.services.servicebus.models.ListTopicsResult;
import com.microsoft.windowsazure.services.servicebus.models.QueueInfo;
import com.microsoft.windowsazure.services.servicebus.models.TopicInfo;

class AzureConnector {
	
	private AzureConnector(){}
	
	private final static AzureConnector instance = new AzureConnector();
	private static boolean configured = false;
	public static final AzureConnector getInstance(){return instance;}
	
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
	StorageManagementClient storageService;
	ServiceBusContract serviceBusService;
	
	// Configurações
	private String stuffLocation = "East US";
	private String adminuserPassword = "csgrid123*";
	private String adminUserName = "csgrid";
	private String storageAccountName = "portalvhdsj1wfschz5kp1n";
	private String diskStorageContainer = "vhds";
	private String algorithmStorageContainer = "csgridAlgorithms";
	private String projectsStorageContainer = "csgridProjects";
	private String commandQueuePath = "commands";
	private String statusTopicPath = "status";
	
	// Define the connection-string with your values
	private String storageConnectionString;
	private CloudStorageAccount storageAccount;
	private CloudBlobClient blobClient;
	private CloudBlobContainer vhdsConteiner;
	private CloudBlobContainer algorithmsConteiner;
	private CloudBlobContainer projectsConteiner;
	
	private String fakeBlobRootDir = "/AzureBlobStorage";
	
	private Map<String, VMInstance> virtualMachines = Collections.synchronizedMap(new HashMap<String, VMInstance>());

	public synchronized void configure(Properties pluginProperties){
		if (configured)
			return;
		configured = true;
		
		stuffLocation = pluginProperties.getProperty("stuffLocation");
		adminUserName = pluginProperties.getProperty("adminUserName");
		adminuserPassword = pluginProperties.getProperty("adminuserPassword");
		storageAccountName = pluginProperties.getProperty("storageAccountName");
		diskStorageContainer = pluginProperties.getProperty("diskStorageContainer");
		algorithmStorageContainer = pluginProperties.getProperty("algorithmStorageContainer");
		projectsStorageContainer = pluginProperties.getProperty("projectsStorageContainer");
		
		try{
			azureConfig = ManagementConfiguration.configure(
					new URI(pluginProperties.getProperty("managementURI")), 
					pluginProperties.getProperty("subscriptionId"), 
					pluginProperties.getProperty("keyStorePath"), 
					pluginProperties.getProperty("keyStorePassword"));
			azureConfig.setProperty("management.keystore.type", "jks");
			
			vmService = ComputeManagementService.create(instance.azureConfig);
			storageService = StorageManagementService.create(instance.azureConfig);
			
			Configuration serviceBusconfig = new Configuration();
			ServiceBusConfiguration.configureWithConnectionString(null, serviceBusconfig, 
					pluginProperties.getProperty("serviceBusConnectionString"));
			serviceBusService = ServiceBusService.create(serviceBusconfig);
			
		}
		catch(Throwable tte){
			throw new IllegalStateException("Não foi possível inicializar SGA Azure com as configurações fornecidas: "+pluginProperties.toString(), tte);
		}
		
		prepareStorageAccount();
		prepareServiceBus();
		
	}
	
	public void prepareServiceBus(){
		
		logger.log(Level.INFO, "Verificando fila de comandos no ServiceBus...");
		boolean found = false;
		try {
			ListQueuesResult queuesResult = serviceBusService.listQueues();
			
			for (QueueInfo queueInfo : queuesResult.getItems()){
				if (queueInfo.getPath().equals(commandQueuePath)){
					found = true;
					break;
				}
			}
		} catch (ServiceException e) {
			throw new IllegalStateException("Falha ao recuperar lista de filas do ServiceBus Azure", e);
		}

		if (!found){
			logger.log(Level.INFO, "Criando a fila de comandos no ServiceBus ("+commandQueuePath+")");
			QueueInfo commandQueueInfo = new QueueInfo(commandQueuePath);
			try {
				serviceBusService.createQueue(commandQueueInfo);
			} catch (ServiceException e) {
				throw new IllegalStateException("Falha ao criar fila de comandos " + commandQueuePath + " no ServiceBus Azure", e);
			}
		}

		logger.log(Level.INFO, "Verificando tópico de status no ServiceBus...");
		found = false;
		try {
			ListTopicsResult topicsResult = serviceBusService.listTopics();
			for (TopicInfo topicInfo : topicsResult.getItems()){
				if (topicInfo.getPath().equals(statusTopicPath)){
					found = true;
					break;
				}
			}
		} catch (ServiceException e) {
			throw new IllegalStateException("Falha ao recuperar lista de tópicos do ServiceBus Azure", e);
		}

		if (!found){
			logger.log(Level.INFO, "Criando o tópico de status no ServiceBus ("+statusTopicPath+")");
			TopicInfo statusTopicInfo = new TopicInfo(statusTopicPath);
			try {
				serviceBusService.createTopic(statusTopicInfo);
			} catch (ServiceException e) {
				throw new IllegalStateException("Falha ao criar tópico de status " + statusTopicPath + " no ServiceBus Azure", e);
			}
		}

	}
	
	
	public void prepareStorageAccount(){
		
		logger.log(Level.INFO, "Preparando serviço de armazenamento Azure");
		boolean exists = false;
		try {
			logger.log(Level.FINE, "Verificando existência da conta de armazenamento "+storageAccountName+"...");
			StorageAccountGetResponse response = storageService.getStorageAccountsOperations().get(storageAccountName);
			if (response.getStatusCode()==200){
				exists = true;
			}
			
		} catch (IOException | ServiceException | ParserConfigurationException
				| SAXException | URISyntaxException e) {
			if (e instanceof ServiceException && e.getMessage().contains("ResourceNotFound")){
				exists = false;
			}
			else
				throw new IllegalStateException("Falha ao recuperar conta de armazenamento para HDs de máquinas virtuais "+storageAccountName, e);
		}

		if (!exists){
			StorageAccountCreateParameters parameters = new StorageAccountCreateParameters();
			parameters.setLocation(stuffLocation);
			parameters.setLabel(storageAccountName);
			parameters.setName(storageAccountName);
			parameters.setDescription("Armazenamento de dados para SGAs CSGrid");
			parameters.setAccountType("Standard_LRS");
			
			try {
				logger.log(Level.INFO, "Criando conta de armazenamento na nuvem Azure "+storageAccountName);
				OperationStatusResponse resp = storageService.getStorageAccountsOperations().create(parameters);
				if (resp.getStatusCode() != 200){
					throw new IllegalStateException("Falha ao criar conta de armazenamento (HTTP STATUS "+resp.getStatusCode()+": "+resp.getError()+"): "+ReflectionToStringBuilder.toString(resp));
				}
			} catch (InterruptedException | ExecutionException | ServiceException | IOException e) {
				throw new IllegalStateException("Falha ao criar conta de armazenamento "+storageAccountName, e);
			}
			
		}
		
		try {
			logger.log(Level.FINE, "Recuperando chave de acesso para a conta de armazenamento "+storageAccountName+"...");
			StorageAccountGetKeysResponse keysResp = storageService.getStorageAccountsOperations().getKeys(storageAccountName);
			if (keysResp.getStatusCode()!=200)
				throw new IllegalStateException("Não foi possível recuperar as chaves de acesso à conta de armazenamento "+storageAccountName+" (HTTP STATUS "+keysResp.getStatusCode()+"): "+ReflectionToStringBuilder.toString(keysResp));
			storageConnectionString = 
				    "DefaultEndpointsProtocol=http;" + 
				    "AccountName="+storageAccountName+";" + 
				    "AccountKey=" + keysResp.getPrimaryKey();

		} catch (IOException | ServiceException | ParserConfigurationException
				| SAXException | URISyntaxException e) {
			throw new IllegalStateException("Falha ao recuperar chaves da conta de armazenamento "+storageAccountName, e);
		}
		
		logger.log(Level.FINE, "Criando referências para conteiners de VHDS, projetos e algoritmos...");
		try {
			storageAccount = CloudStorageAccount.parse(storageConnectionString);
		} catch (InvalidKeyException | URISyntaxException e) {
			throw new IllegalStateException("Falha ao instanciar serviço de armazenamento à partir da string de conexão: "+storageConnectionString, e);
		}
		
		blobClient = storageAccount.createCloudBlobClient();
		try {
			vhdsConteiner = blobClient.getContainerReference(diskStorageContainer);
			projectsConteiner = blobClient.getContainerReference(projectsStorageContainer);
			algorithmsConteiner = blobClient.getContainerReference(algorithmStorageContainer);
		} catch (URISyntaxException | StorageException e) {
			throw new IllegalStateException("Falha ao adquirir referência aos conteiners do serviço de armazenamento "+storageAccountName, e);
		}
		
		logger.log(Level.FINE, "Verificando existência dos conteiners de VHDS, projetos e algoritmos...");
		try {
			if (vhdsConteiner.createIfNotExists())
				logger.log(Level.INFO, "Criado novo conteiner para discos de máquinas virtuais "+diskStorageContainer+".");
			else
				logger.log(Level.FINER, "Conteiner para discos de máquinas virtuais "+diskStorageContainer+" ok.");
				
			if (projectsConteiner.createIfNotExists())
				logger.log(Level.INFO, "Criado novo conteiner para projetos "+projectsStorageContainer);
			else
				logger.log(Level.FINER, "Conteiner para discos para projetos "+projectsStorageContainer+" ok.");

			if (algorithmsConteiner.createIfNotExists())
				logger.log(Level.INFO, "Criado novo conteiner para algoritmos "+algorithmStorageContainer);
			else
				logger.log(Level.FINER, "Conteiner para algoritmos "+algorithmStorageContainer+" ok.");

		} catch (StorageException e) {
			throw new IllegalStateException("Falha criar conteiners do serviço de armazenamento "+storageAccountName+", caso não existissem.", e);
		}
		
		logger.log(Level.INFO, "Serviço de armazenamento Azure ok.");		
	}

	public String createNewVirtualMachine(String size) throws InterruptedException, URISyntaxException{
		byte[] rndName = new byte[20];
		rnd.nextBytes(rndName);
		String name = "csgrid-sga"+Hex.encodeHexString(rndName);
		
		// Criar primeiro o serviço de nuvem
		{
			boolean ok = false;
			Throwable error = null;
	        //hosted service required for vm deployment
	        HostedServiceCreateParameters createParameters = new HostedServiceCreateParameters(); 
	        //required
	        createParameters.setLabel(name);
	        //required
	        createParameters.setServiceName(name);
	        createParameters.setDescription("Auto-generated SGA from CSGrid");
	        //required
	        createParameters.setLocation(stuffLocation);
			if (logger.isLoggable(Level.FINEST))
				logger.log(Level.FINEST, "Requisitando criação de cloud service "+ReflectionToStringBuilder.toString(createParameters, ToStringStyle.MULTI_LINE_STYLE));
			for (int retry=0; retry<10; retry+=1){
				if (retry>0)
					logger.log(Level.FINE, "Retentativa "+retry);
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
					if (e.getMessage().contains("SubscriptionDisabled")){
						logger.log(Level.SEVERE, "A assinatura da Azure está inválida. Regularize a situação ou troque a assinatura na configuração do SGA.", e);
						return null;
					}
					error = e;
					logger.log(Level.FINE, "Erro na requisição de criação de serviço de nuvem "+name, e);
		        	ok=false;
		        	Thread.sleep(2000+rnd.nextInt(2000));
		        	continue; // retry
				}
		        
		        ok=true;
		        break;
			}
			
			if (!ok){
				logger.log(Level.SEVERE, "Não foi possível criar novo serviço de nuvem "+name, error);
				return null;
			}
		}
		
		long timer=0;
		// Depois cria a máquina virtual propriamente dita
		{
	        ArrayList<Role> rolelist = createRoleList(name, adminUserName, adminuserPassword, size); 
	        
	        VirtualMachineCreateDeploymentParameters deploymentParameters = new VirtualMachineCreateDeploymentParameters();
	        deploymentParameters.setDeploymentSlot(DeploymentSlot.Production);
	        deploymentParameters.setName(name); 
	        deploymentParameters.setLabel(name);        
	        deploymentParameters.setRoles(rolelist);
	        
	        Throwable error = null;
	        boolean ok = false;
			if (logger.isLoggable(Level.FINEST))
				logger.log(Level.FINEST, "Requisitando criação de máquina virtual "+ReflectionToStringBuilder.toString(deploymentParameters, ToStringStyle.MULTI_LINE_STYLE));
	
			for (int retry=0; retry<10; retry+=1){
				if (retry>0)
					logger.log(Level.FINE, "Retentativa "+retry);
	
				try{
			        //Act
					timer = System.currentTimeMillis();
			        OperationResponse operationResponse = vmService.getVirtualMachinesOperations().createDeployment(name, deploymentParameters);
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
		        	error = e;
		        	Thread.sleep(2000+rnd.nextInt(2000));
		        	continue; // retry
				}
		        
		        ok=true;
		        break;
			}
			if (!ok){
				logger.log(Level.SEVERE, "Não foi possível criar nova máquina virtual "+name+" com tamanho "+size, error);
				return null;
			}
		}

		logger.log(Level.FINE, "Máquina virtual criada com sucesso: "+name+" ("+size+") em "+(System.currentTimeMillis()-timer)+"ms");
		
		return name;
	}
	
    private ArrayList<Role> createRoleList(String virtualMachineName, String adminUserName, String adminUserPassword, String size) {
        ArrayList<Role> roleList = new ArrayList<Role>();
        Role role = new Role();
        String roleName = virtualMachineName;
        String computerName = virtualMachineName;
        URI mediaLinkUriValue;
		try {
			mediaLinkUriValue = new URI("http://"+ storageAccountName + ".blob.core.windows.net/"+diskStorageContainer+ "/" + virtualMachineName +".vhd");
		} catch (URISyntaxException e) {
			throw new IllegalStateException("Erro interno descabido... corra enquanto é tempo!", e);
		}
        String osVHarddiskName = virtualMachineName + "oshd";
        String operatingSystemName ="Linux";

        //required
        ArrayList<ConfigurationSet> configurationSetList = new ArrayList<ConfigurationSet>();
        ConfigurationSet configurationSet = new ConfigurationSet();
         configurationSet.setConfigurationSetType(ConfigurationSetTypes.LINUXPROVISIONINGCONFIGURATION);
        //required
        configurationSet.setComputerName(computerName);
        //required
        configurationSet.setAdminPassword(adminUserPassword);
        //required
        configurationSet.setAdminUserName(adminUserName);
        configurationSet.setUserName("u"+adminUserName);

        // TODO: Mesma senha.
        configurationSet.setUserPassword(adminUserPassword);
        
        
        //"Custom data" fica em /var/lib/waagent/ovf-env.xml
        //ou em                 /var/lib/waagent/CustomData
        //(codificado em base64)
        configurationSet.setCustomData(
        		com.microsoft.windowsazure.core.utils.Base64.encode(
        		("vmname="+virtualMachineName+"\nsize="+size).getBytes()
        		));
        
        
        configurationSet.setEnableAutomaticUpdates(false);
        configurationSet.setHostName(virtualMachineName + ".cloudapp.net");
        configurationSetList.add(configurationSet); 

        String sourceImageName = getOSSourceImage();
        OSVirtualHardDisk oSVirtualHardDisk = new OSVirtualHardDisk();
        //required
        oSVirtualHardDisk.setName(osVHarddiskName);
        oSVirtualHardDisk.setHostCaching(VirtualHardDiskHostCaching.READWRITE);
        oSVirtualHardDisk.setOperatingSystem(operatingSystemName);
        //required
        oSVirtualHardDisk.setMediaLink(mediaLinkUriValue);
        //required
        oSVirtualHardDisk.setSourceImageName(sourceImageName);

        //required        
        role.setRoleName(roleName);
        //required
        role.setRoleType(VirtualMachineRoleType.PersistentVMRole.toString());
        role.setRoleSize(size);
        role.setProvisionGuestAgent(true);
        role.setConfigurationSets(configurationSetList);
        role.setOSVirtualHardDisk(oSVirtualHardDisk);
        roleList.add(role);
        return roleList; 
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
					logger.finest("Requisitando lista completa de serviços de nuvem...");
					hostedServiceListResponse = vmService.getHostedServicesOperations().list();
					ArrayList<HostedServiceListResponse.HostedService> hostedServicelist = hostedServiceListResponse.getHostedServices();
					
					for (HostedServiceListResponse.HostedService hostedService : hostedServicelist) {
						if (hostedService.getServiceName().startsWith("csgrid-sga")) {
							
							String name = hostedService.getServiceName();
							
							discovered.add(name);
							
							VMInstance vmInstance = virtualMachines.get(name);
							
							if (vmInstance == null){
			
								logger.finest("Requisitando detalhes sobre o serviço de nuvem "+hostedService.getServiceName());
								HostedServiceGetDetailedResponse hostedServiceGetDetailedResponse 
									= vmService.getHostedServicesOperations().getDetailed(hostedService.getServiceName());
			
								ArrayList<HostedServiceGetDetailedResponse.Deployment> deploymentlist = hostedServiceGetDetailedResponse.getDeployments();
								
								int thisHostedServiceVMCount = 0;
			
								for (HostedServiceGetDetailedResponse.Deployment deployment : deploymentlist) {
									ArrayList<Role> rolelist = deployment.getRoles();
			
									for (Role role : rolelist) {
										if ((role.getRoleType()!=null) && (role.getRoleType().equalsIgnoreCase(VirtualMachineRoleType.PersistentVMRole.toString()))) {
											virtualMachines.put(name, new VMInstance(name, name, role.getRoleSize()));
											thisHostedServiceVMCount += 1;
										}
									}
								}
								
								// Se o serviço de nuvem estiver "vazio", termina de o excluir de uma vez por todas!
								if (thisHostedServiceVMCount == 0){
									discovered.remove(name);
									// Tenta uma só vez e, se der algo errado, ignora e segue em frente.
									// (em uma próxima iteração poderá ser tentado novamente)
									try{
										logger.log(Level.INFO, "O serviço de nuvem "+name+" será excluído pro não possuir nenhuma máquina virtual.");
										vmService.getHostedServicesOperations().deleteAll(name);
									}catch(Throwable e){
										logger.log(Level.FINE, "Erro ao excluir serviço de nuvem vazio "+name+" para limpeza."
												+ " Será retentado novamente no futuro, se necessário.", e);
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

	public void copy(Path source, Path target) throws URISyntaxException, StorageException, IOException {
		boolean sourceIsAzure = source.toFile().getAbsolutePath().startsWith(fakeBlobRootDir);
		boolean targetIsAzure = target.toFile().getAbsolutePath().startsWith(fakeBlobRootDir);
		if (!sourceIsAzure && targetIsAzure){
			CloudBlockBlob targetBlob = unAzureSinglePath(target.toFile().getAbsolutePath());
			logger.log(Level.FINE, "Copiando arquivo "+source+" para BLOB "+targetBlob.getName());
			if (!source.toFile().getParentFile().exists())
				source.toFile().getParentFile().mkdirs();
			targetBlob.uploadFromFile(source.toFile().getAbsolutePath());
		}
		else if (sourceIsAzure && !targetIsAzure){
			CloudBlockBlob sourceBlob = unAzureSinglePath(source.toFile().getAbsolutePath());
			logger.log(Level.FINE, "Copiando BLOB "+sourceBlob.getName()+" para arquivo "+target);
			if (!target.toFile().getParentFile().exists())
				target.toFile().getParentFile().mkdirs();
			sourceBlob.downloadToFile(target.toFile().getAbsolutePath());
		}
//		else if (!sourceIsAzure && !targetIsAzure){
//			
//			sourceBlob.downloadToFile(target.toFile().getAbsolutePath());
//		}
		else{
			throw new IllegalStateException("Só sei copiar de Azure para local ou de local para azure.");
		}
	}

	private CloudBlockBlob unAzureSinglePath(String path) throws URISyntaxException, StorageException {
		if (!path.startsWith(fakeBlobRootDir))
			throw new IllegalArgumentException("Espera-se que o diretório comece com "+fakeBlobRootDir);
		String blobName = path.replaceFirst(Pattern.quote(fakeBlobRootDir), "");
		
		if (path.startsWith("algorithms"))
			return algorithmsConteiner.getBlockBlobReference(blobName);
		else
			return projectsConteiner.getBlockBlobReference(blobName);
	}

	public void createDirectories(Path target) {
		logger.log(Level.FINE, "Criando diretório "+target);
		boolean isAzure = target.toFile().getAbsolutePath().startsWith(fakeBlobRootDir);
		if (!isAzure){
			target.toFile().mkdirs();
		}
	}

	public void deleteBlobs(Path target) throws URISyntaxException, StorageException {
		for (ListBlobItem i : unAzurePrefixTree(target.toFile().getAbsolutePath())){
			if (i instanceof CloudBlockBlob){
				CloudBlockBlob blob = (CloudBlockBlob)i;
				logger.log(Level.FINE, "Excluindo blob "+blob.getName());
				blob.delete();
			}
		}
	}

	private Iterable<ListBlobItem> unAzurePrefixTree(String path) throws URISyntaxException, StorageException {
		if (!path.startsWith(fakeBlobRootDir))
			throw new IllegalArgumentException("Espera-se que o diretório comece com "+fakeBlobRootDir);
		String blobName = path.replaceFirst(Pattern.quote(fakeBlobRootDir), "");
		
		EnumSet<BlobListingDetails> listingDetails = EnumSet.of(BlobListingDetails.METADATA);
		BlobRequestOptions options = new BlobRequestOptions();
		
		if (path.startsWith("algorithms"))
			return algorithmsConteiner.listBlobs(blobName, true, listingDetails, options, null);
		else
			return projectsConteiner.listBlobs(blobName, true, listingDetails, options, null);
	}
	
	public boolean exists(Path target) throws StorageException, URISyntaxException {
		boolean isAzure = target.toFile().getAbsolutePath().startsWith(fakeBlobRootDir);
		boolean result;
		if (isAzure){
			result = unAzureSinglePath(target.toFile().getAbsolutePath()).exists();
			// Se não existe como um blob único, tenta ver se existe como diretório
			if (!result){
				for (ListBlobItem i : unAzurePrefixTree(target.toFile().getAbsolutePath()+"/")){
					if (i instanceof CloudBlockBlob){
						result = true;
						break;
					}
				}
			}
		}
		else
			result = target.toFile().exists();
		logger.log(Level.FINE, "Verificando se o arquivo "+target+" existe: "+result);
		return result;
	}

	public Map<String[], Long> getTimestamps(Path rootDir) throws URISyntaxException, StorageException {
		
		Map<String[], Long> result = new HashMap<>();
		
		for (ListBlobItem i : unAzurePrefixTree(rootDir.toFile().getAbsolutePath())){
			if (i instanceof CloudBlockBlob){
				CloudBlockBlob blob = (CloudBlockBlob)i;
				//String blobName = k.getPrefix();
//				if (blobName.endsWith("/"))
//					blobName = blobName.substring(0, blobName.length()-1);
//				if (blobName.startsWith("algorithms"))
//					blob = algorithmsConteiner.getBlockBlobReference(blobName);
//				else
//					blob = projectsConteiner.getBlockBlobReference(blobName);
				
				logger.log(Level.FINE, "Recuperando metadados de "+blob.getName()+"...");
				try{
					blob.downloadAttributes();
				}
				catch(StorageException se){
					if (se.getMessage().contains("The specified blob does not exist"))
						continue;
				}
				Date blobResult = blob.getProperties().getLastModified();
				
				logger.log(Level.FINE, "Data de criação do blob "+blob.getName()+": "+blobResult);
				long time;
				if (blobResult == null)
					time = 0;
				else
					time = blobResult.getTime();
				result.put(splitPath(fakeBlobRootDir+"/"+blob.getName(), '/'), time);
			}
		}

		return result;
	}

	
	  /**
	   * Separa um caminho.
	   *
	   * @param path o camimnho
	   * @param separator o separador
	   *
	   * @return array contendo cada item do caminho
	   */
	  public static final String[] splitPath(String path, char separator) {
	    @SuppressWarnings("resource")
		Scanner scanner =
	      new Scanner(path).useDelimiter(Pattern.quote(String.valueOf(separator)));
	    List<String> pathAsList = new ArrayList<String>();
	    while (scanner.hasNext()) {
	      String dir = scanner.next();
	      /*
	       * FIXME o 'if' abaixo existe apenas para compatibilizar o comportamento
	       * com a implementação anterior (que não usava o Scanner). Ele faz com que
	       * paths vazios ("//") sejam ignorados, quando na talvez devessem retornar
	       * "" (que é o comportamento natural do Scanner para este caso).
	       */
	      if (!dir.isEmpty()) {
	        pathAsList.add(dir);
	      }
	    }
	    return pathAsList.toArray(new String[pathAsList.size()]);
	  }

}
