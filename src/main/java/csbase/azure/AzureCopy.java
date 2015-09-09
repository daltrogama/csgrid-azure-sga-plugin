package csbase.azure;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;
import java.util.regex.Pattern;

import com.microsoft.azure.storage.StorageException;

import csbase.server.plugin.service.IServiceManager;
import csbase.server.plugin.service.algorithmservice.IAlgorithmService;
import csbase.server.plugin.service.projectservice.IProjectService;
import csbase.server.plugin.service.sgaservice.ISGADataTransfer;
import csbase.server.plugin.service.sgaservice.SGADataTransferException;

public class AzureCopy implements ISGADataTransfer {

  private AzureConnector azure = AzureConnector.getInstance();
	
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
    
    azure.configure(sgaProperties);
    
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void copyTo(String[] sourcePath, String[] targetPath) throws SGADataTransferException {
	  
	  System.out.println("AzureCopy.copyTo()");
	  System.out.println("* SourcePath: "+Arrays.toString(sourcePath));
	  System.out.println("* TargetPath: "+Arrays.toString(targetPath));
	  
	    String root = sgaProperties.getProperty(ROOT);
	    Path source = Paths.get(join(sourcePath, File.separator));
	    Path target = Paths.get(root + File.separator + join(targetPath, File.separator));

	    try {
	      if (!Files.isDirectory(source)) {
	        azure.copy(source, target);
	      }
	      if (Files.isDirectory(source)) {
	        Files.walkFileTree(source, new CopyFileVisitor(source, target));
	      }
	    }
	    catch (IOException | URISyntaxException | StorageException e) {
	      throw new SGADataTransferException(e);
	    }

  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void copyFrom(String[] remoteFilePath, String[] localFilePath) throws SGADataTransferException {
	  
	  System.out.println("AzureCopy.copyFrom()");
	  System.out.println("* remoteFilePath: "+Arrays.toString(remoteFilePath));
	  System.out.println("* localFilePath: "+Arrays.toString(localFilePath));

	    String root = sgaProperties.getProperty(ROOT);
	    Path remote =
	      Paths.get(root + File.separator + join(remoteFilePath, File.separator));
	    Path local = Paths.get(join(localFilePath, File.separator));

	    try {
	      if (!Files.isDirectory(local)) {
	        Path parent = local.getParent();
	        Files.createDirectories(parent);
	        azure.copy(remote, local);
	        return;
	      }
	      Files.walkFileTree(remote, new CopyFileVisitor(remote, local));
	    }
	    catch (IOException | URISyntaxException | StorageException e) {
	      throw new SGADataTransferException(e);
	    }
	  
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void createDirectory(String[] path) throws SGADataTransferException {
	  
	  System.out.println("AzureCopy.createDirectory()");
	  System.out.println("* path: "+Arrays.toString(path));
	  
	    String root = sgaProperties.getProperty(ROOT);
	    Path target = Paths.get(root + File.separator + join(path, File.separator));

	    try {
	      azure.createDirectories(target);
	    }
	    catch (Throwable e) {
	      throw new SGADataTransferException(e);
	    }
  
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void remove(String[] path) throws SGADataTransferException {

	  System.out.println("AzureCopy.remove()");
	  System.out.println("* path: "+Arrays.toString(path));
	    String root = sgaProperties.getProperty(ROOT);
	    Path target = Paths.get(root + File.separator + join(path, File.separator));
	    try {
			azure.deleteBlobs(target);
		} catch (URISyntaxException | StorageException e) {
			throw new SGADataTransferException(e);
		}

  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean checkExistence(String[] path) throws SGADataTransferException {

	  System.out.println("AzureCopy.checkExistence() == true");
	  System.out.println("* path: "+Arrays.toString(path));
	  
	    String root = sgaProperties.getProperty(ROOT);
	    Path target = Paths.get(root + File.separator + join(path, File.separator));
	    boolean exists;
		try {
			exists = azure.exists(target);
		} catch (StorageException | URISyntaxException e) {
			throw new SGADataTransferException(e);
		}

	    return exists;
	  
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String[] getProjectsRootPath() throws SGADataTransferException {
    IProjectService projectService =
      (IProjectService) serviceManager.getService("ProjectService");
    return new String[] { projectService.getProjectRepositoryPath() };
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String[] getAlgorithmsRootPath() throws SGADataTransferException {
    IAlgorithmService algorithmService =
      (IAlgorithmService) serviceManager.getService("AlgorithmService");
    return new String[] { algorithmService.getAlgorithmRepositoryPath() };
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Map<String[], Long> getLocalTimestamps(String[] sandboxPath) throws SGADataTransferException {
 
	System.out.println("AzureCopy.getLocalTimestamps()");
	System.out.println("* path: "+Arrays.toString(sandboxPath));
	
    final Map<String[], Long> timestampsMap = new HashMap<>();
    Path sandbox = Paths.get(join(sandboxPath, File.separator));

    try {
      Files.walkFileTree(sandbox, new SimpleFileVisitor<Path>() {
        @Override
        public FileVisitResult visitFile(Path path, BasicFileAttributes attrs)
          throws IOException {
          File file = path.toFile();
          if (!file.isDirectory()) {
            timestampsMap.put(splitPath(file.getPath().replace(".."+File.separatorChar, ""),
              File.separatorChar), file.lastModified());
          }

          return FileVisitResult.CONTINUE;
        }

      });
    }
    catch (IOException e) {
      throw new SGADataTransferException(e);
    }

    return timestampsMap;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Map<String[], Long> getRemoteTimestamps(String[] sandboxPath) throws SGADataTransferException {
    
	System.out.println("AzureCopy.getRemoteTimestamps()");
	System.out.println("* path: "+Arrays.toString(sandboxPath));
	
    final Map<String[], Long> timestampsMap = new HashMap<>();
    Path sandbox =
      Paths.get(sgaProperties.getProperty(ROOT) + File.separator
        + join(sandboxPath, File.separator));

    try {
    	timestampsMap.putAll(azure.getTimestamps(sandbox));
    }
    catch (URISyntaxException | StorageException e) {
      throw new SGADataTransferException(e);
    }
    
    System.out.println("* res: "+timestampsMap);
    
    return timestampsMap;
  }
  
  /**
   * Constroi uma string concatenando o array de strings usando o separador
   * informado.
   *
   * @param str o array de strings a serem concatenadas
   * @param separator o separador usado para concatenação
   * @return a string resultante da concatenação
   */
  private String join(String[] str, String separator) {
    String retval = "";
    for (String s : str) {
      retval += separator + s;
    }
    return retval.replaceFirst(separator, "");
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

  /**
   * Implementa um visitador sobre arquivos para fazer uma cópia.
   *
   * @author Tecgraf/PUC-Rio
   */
  public class CopyFileVisitor extends SimpleFileVisitor<Path> {
    /** Caminho destino */
    private final Path targetPath;
    /** Caminho origem */
    private final Path sourcePath;

    /**
     * Construtor.
     *
     * @param sourcePath caminho de origem
     * @param targetPath caminho destino
     */
    public CopyFileVisitor(Path sourcePath, Path targetPath) {
      this.sourcePath = sourcePath;
      this.targetPath = targetPath;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public FileVisitResult preVisitDirectory(final Path directory,
      final BasicFileAttributes attrs) throws IOException {
//      Path targetdir = targetPath.resolve(sourcePath.relativize(directory));
//      try {
//        try {
//			azure.copy(directory, targetdir);
//		} catch (URISyntaxException | StorageException e) {
//			throw new IOException(e);
//		}
//      }
//      catch (FileAlreadyExistsException e) {
//        if (!Files.isDirectory(targetPath)) {
//          throw e;
//        }
//      }
      return FileVisitResult.CONTINUE;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public FileVisitResult visitFile(final Path file,
      final BasicFileAttributes attrs) throws IOException {
      try {
		azure.copy(file, targetPath.resolve(sourcePath.relativize(file)));
	} catch (URISyntaxException | StorageException e) {
		throw new IOException(e);
	}
      return FileVisitResult.CONTINUE;
    }
  }


}
