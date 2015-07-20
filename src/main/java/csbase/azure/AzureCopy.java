package csbase.azure;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import csbase.server.plugin.service.IServiceManager;
import csbase.server.plugin.service.sgaservice.ISGADataTransfer;
import csbase.server.plugin.service.sgaservice.SGADataTransferException;

public class AzureCopy implements ISGADataTransfer {

  private AzureConnector azure;
	
  /**
   * As propriedades que o SGA forneceu sobre o mecanismo a ser adotado.
   */
  private Properties sgaProperties;

  /**
   * O gerenciador de serviços.
   */
  private IServiceManager serviceManager;

  /**
   * Construtor.
   *
   * @param serviceManager gerente dos serviços
   */
  public AzureCopy(IServiceManager serviceManager) {
    this.serviceManager = serviceManager;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setSGAProperties(Properties sgaProperties) {
    this.sgaProperties = sgaProperties;
    System.out.println("AzureCopy.setSGAProperties("+this.sgaProperties.toString()+")");
    
    azure = new AzureConnector(sgaProperties);
    
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void copyTo(String[] sourcePath, String[] targetPath) throws SGADataTransferException {
	  
	  System.out.println("AzureCopy.copyTo()");
	  System.out.println("* SourcePath: "+Arrays.toString(sourcePath));
	  System.out.println("* TargetPath: "+Arrays.toString(targetPath));
  
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void copyFrom(String[] remoteFilePath, String[] localFilePath) throws SGADataTransferException {
	  
	  System.out.println("AzureCopy.copyFrom()");
	  System.out.println("* remoteFilePath: "+Arrays.toString(remoteFilePath));
	  System.out.println("* localFilePath: "+Arrays.toString(localFilePath));
  
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void createDirectory(String[] path) throws SGADataTransferException {
	  
	  System.out.println("AzureCopy.createDirectory()");
	  System.out.println("* path: "+Arrays.toString(path));
  
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void remove(String[] path) throws SGADataTransferException {

	  System.out.println("AzureCopy.remove()");
	  System.out.println("* path: "+Arrays.toString(path));
	  
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean checkExistence(String[] path) throws SGADataTransferException {

	  System.out.println("AzureCopy.checkExistence() == true");
	  System.out.println("* path: "+Arrays.toString(path));
	  return true;
	  
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String[] getProjectsRootPath() throws SGADataTransferException {
	System.out.println("AzureCopy.getProjectsRootPath() == azure/dummy/projects");
    return new String[] { "azure", "dummy", "projects" };
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String[] getAlgorithmsRootPath() throws SGADataTransferException {
		System.out.println("AzureCopy.getAlgorithmsRootPath() == azure/dummy/algorithms");
	    return new String[] { "azure", "dummy", "algorithms" };
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Map<String[], Long> getLocalTimestamps(String[] sandboxPath) throws SGADataTransferException {
    final Map<String[], Long> timestampsMap = new HashMap<>();
    
    timestampsMap.put(new String[]{"arq1"}, 100000l);
    timestampsMap.put(new String[]{"arq2"}, 100001l);
    timestampsMap.put(new String[]{"arq3"}, 100002l);
    timestampsMap.put(new String[]{"pasta1", "arq1"}, 101003l);
    timestampsMap.put(new String[]{"pasta1", "arq2"}, 102003l);
    timestampsMap.put(new String[]{"pasta1", "arq3"}, 103003l);
    
	System.out.println("AzureCopy.getLocalTimestamps()");
	System.out.println("* path: "+Arrays.toString(sandboxPath));
    System.out.println("* res: "+timestampsMap);
    
    return timestampsMap;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Map<String[], Long> getRemoteTimestamps(String[] sandboxPath) throws SGADataTransferException {
    final Map<String[], Long> timestampsMap = new HashMap<>();
    
    timestampsMap.put(new String[]{"arq1"}, 100000l);
    timestampsMap.put(new String[]{"arq2"}, 100101l); // Atualizado
    timestampsMap.put(new String[]{"arq3"}, 100002l);
    timestampsMap.put(new String[]{"arq4"}, 100003l); // Novo
    timestampsMap.put(new String[]{"pasta1", "arq1"}, 101003l);
    timestampsMap.put(new String[]{"pasta1", "arq2"}, 102003l);
    timestampsMap.put(new String[]{"pasta1", "arq3"}, 103023l); // Atualizado
    
	System.out.println("AzureCopy.getLocalTimestamps()");
	System.out.println("* path: "+Arrays.toString(sandboxPath));
    System.out.println("* res: "+timestampsMap);
    
    return timestampsMap;
  }

}
