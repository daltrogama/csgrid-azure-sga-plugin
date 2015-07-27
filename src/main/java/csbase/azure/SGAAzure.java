package csbase.azure;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.omg.CORBA.IntHolder;

import sgaidl.InvalidParameterException;
import sgaidl.InvalidPathException;
import sgaidl.InvalidSGAException;
import sgaidl.MissingParameterException;
import sgaidl.NoPermissionException;
import sgaidl.Pair;
import sgaidl.PathNotFoundException;
import sgaidl.SGAAlreadyRegisteredException;
import sgaidl.SGACommand;
import sgaidl.SGAControlAction;
import sgaidl.SGANotRegisteredException;
import sgaidl.SGAPath;
import sgaidl.SGAProperties;
import sgaidl.SystemException;
import csbase.server.plugin.service.IServiceManager;
import csbase.server.plugin.service.sgaservice.ISGADaemon;
import csbase.server.plugin.service.sgaservice.ISGAService;
import csbase.server.plugin.service.sgaservice.SGADaemonException;

public class SGAAzure implements ISGADaemon {

	/**
	 * Nome do serviço SGA.
	 */
	public static final String SGA_SERVICE_NAME = "SGAService";

	private static final String PROP_LOG_PATH = "csbase_log_path";
	private static final String PROP_MACHINE_TIME = "csbase_machine_time_seconds";
	private static final String PROP_LOG_LEVEL = "csbase_log_level";

	/** Logger usado pelo SGA */
	protected Logger logger = Logger.getLogger(this.getClass().getName());
	private static final Level DEFAULT_LOG_LEVEL = Level.INFO;

	private AzureConnector azure = AzureConnector.getInstance();
	
	private ExecutionPool executor;
	
	/** A interface SGAService com o qual o SGA se comunica. */
	private ISGAService sgaService;

	/** As propriedades do SGALocal. */
	protected Properties pluginProperties;

	private String sgaName;

	private Timer renewPropertiesTimer;

	private Timer renewTimer;

	public SGAAzure(IServiceManager serviceManager) {
		this.sgaService = ISGAService.class.cast(serviceManager.getService(SGA_SERVICE_NAME));
	}

	@Override
	public void setProperties(Properties pluginProps) {
		this.pluginProperties = pluginProps;
	}

	/**
	 * Carrega as propriedades do SGA.
	 *
	 * @return retorna as propriedades do SGA.
	 */
	private synchronized SGAProperties loadSGAProperties() {
		SGAProperties sgaProperties = new SGAProperties();
		sgaProperties.properties = new Pair[pluginProperties.size()];

		int i = 0;
		for (Object key : pluginProperties.keySet()) {
			sgaProperties.properties[i] = new Pair();
			sgaProperties.properties[i].key = (String) key;
			sgaProperties.properties[i].value =
					pluginProperties.getProperty((String) key);
			i++;
		}

		//TODO Buscar as infos dos nós pelo monitor
		sgaProperties.nodesProperties = new Pair[1][];
		sgaProperties.nodesProperties[0] = sgaProperties.properties;

		// Propriedades dos nós

		// Coloca no log as propriedades dos SGAs
//		logger.finest("Propriedades do SGA: ");
//		for (Pair p : sgaProperties.properties) {
//			logger.finest("  " + p.key + "=" + p.value);
//		}
//		logger.finest("  " + "Propriedades dos nós do SGA ("
//				+ sgaProperties.nodesProperties.length + ")");
//
//		int j = 0;
//		for (Pair[] nodes : sgaProperties.nodesProperties) {
//			logger.finest("  " + "Nó " + (j++));
//			for (Pair p : nodes) {
//				logger.finest("     " + p.key + "=" + p.value);
//			}
//		}
		return sgaProperties;
	}


	@Override
	public boolean start() throws SGADaemonException {
		if (this.sgaService == null) {
			throw new SGADaemonException("O serviço SGAService está nulo.");
		}
		// Valida as propriedades do plugin
		//validatePluginProperties();
		
		azure.configure(pluginProperties);
		
		executor = new ExecutionPool(pluginProperties, azure);
		
		sgaName = pluginProperties.getProperty(sgaidl.SGA_NAME.value);
		String logFile = "";
		try {
			if (pluginProperties.getProperty(PROP_LOG_PATH) != null) {
				logFile = pluginProperties.getProperty(PROP_LOG_PATH) + sgaName + ".%g.log";
			}

			FileHandler fh = new FileHandler(logFile, 10485760, 10);
			fh.setLevel(DEFAULT_LOG_LEVEL);
			logger.addHandler(fh);

			String logLevel = "";
			if (pluginProperties.getProperty(PROP_LOG_LEVEL) != null) {
				logLevel = pluginProperties.getProperty(PROP_LOG_LEVEL);
				logger.setLevel(Level.parse(logLevel));
				fh.setLevel(Level.parse(logLevel));
			}
			else {
				logger.setLevel(DEFAULT_LOG_LEVEL);
				fh.setLevel(DEFAULT_LOG_LEVEL);
			}
		}
		catch (NullPointerException | IllegalArgumentException e) {
			logger.setLevel(DEFAULT_LOG_LEVEL);
			logger.log(Level.INFO, "Erro ao identificar a propriedade "
					+ PROP_LOG_LEVEL + ". Usando nível de log padrão; "
					+ DEFAULT_LOG_LEVEL.getName(), e);
		}
		catch (SecurityException | IOException e) {
			throw new SGADaemonException("Erro na criação do log " + logFile, e);
		}

		SGAProperties sgaProps = loadSGAProperties();

		if (sgaProps != null && sgaProps.nodesProperties.length > 0) {
			registerSGA(sgaProps);
		}
		int updateTime =
				Integer.parseInt(pluginProperties.getProperty(PROP_MACHINE_TIME));


		renewPropertiesTimer = new Timer();
		renewPropertiesTimer.schedule(new SGAUpdatePropertiesTask(), updateTime * 1000, updateTime * 1000);

		logger.info("SGADaemon " + sgaName + " iniciado");


		initTestDemo();


		return true;
	}


	/**
	 * {@inheritDoc}
	 */
	@Override
	public void stop() {

		if (renewTimer != null) {
			renewTimer.cancel();
		}

		if (renewPropertiesTimer != null) {
			renewPropertiesTimer.cancel();
		}

		logger.info("Finaliza o plugin do SGA.");
	}

	/**
	 * Registra o SGA no servidor.
	 *
	 * @param sgaProps propriedades do SGA
	 */
	private synchronized void registerSGA(SGAProperties sgaProps) {
		IntHolder updateInterval = new IntHolder();
		try {
			sgaService.registerSGA(this, sgaName, sgaProps, updateInterval);
		}
		catch (InvalidParameterException e) {
			logger.log(Level.SEVERE, "Erro ao registrar o SGA: {0}: {1}",
					new Object[] { e, e.message });
			this.stop();
		}
		catch (NoPermissionException e) {
			logger.log(Level.SEVERE, "Erro ao registrar o SGA: {0}: {1}",
					new Object[] { e, e.message });
			this.stop();
		}
		catch (SGAAlreadyRegisteredException e) {
			logger.log(Level.SEVERE, "Erro ao registrar o SGA: {0}: {1}",
					new Object[] { e, e.message });
			this.stop();
		}

		//	    try {
		//	      sgaService.commandRetrieved(sgaName, infos.toArray(new RetrievedInfo[0]));
		//	    }
		//	    catch (InvalidSGAException e) {
		//	      logger.log(Level.SEVERE, "Erro ao recuperar comandos: {0}: {1}",
		//	        new Object[] { e, e.message });
		//	    }
		//	    catch (NoPermissionException e) {
		//	      logger.log(Level.SEVERE, "Erro ao recuperar comandos: {0}: {1}",
		//	        new Object[] { e, e.message });
		//	    }
		//	    catch (InvalidCommandException e) {
		//	      logger.log(Level.SEVERE, "Erro ao recuperar comandos: {0}: {1}",
		//	        new Object[] { e, e.message });
		//	    }

		renewTimer = new Timer();
		renewTimer.schedule(new SGARegisterRenewTask(), updateInterval.value * 1000, updateInterval.value * 1000);

	}


	protected void initTestDemo() {
		
//		try {
//			azure.refreshAllVMs();
//			System.out.println(azure.getAllVMs().toString());
//			System.out.println("Máquina virtual criada: "+azure.createNewVirtualMachine("Small"));
//		} catch (InterruptedException e) {
//			e.printStackTrace();
//		} catch (Exception e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		
//		//		// Get current context class loader
//		//		ClassLoader contextLoader = Thread.currentThread().getContextClassLoader();
//		//		System.out.println("Classloader: "+contextLoader);
//		//		// Change context classloader to class context loader   
//		//		Thread.currentThread().setContextClassLoader(ComputeManagementService.class.getClassLoader()); 
//		//
//		//		setExecutor(new AzureExecutor(pluginProperties));
//		//		setMonitor(new AzureMonitor());
//
//		//TODO: Remover esta terminação forçada.
//		System.out.println("Terminando provisoriamente, para testes.");
//		Runtime.getRuntime().exit(0);

	}

	/**
	 * Tarefa agendada que executa a chamada ao SGAService para avisar que o SGA
	 * se mantém registrado.
	 */
	private class SGARegisterRenewTask extends TimerTask {
		/**
		 * {@inheritDoc}
		 */
		@Override
		public void run() {
			//logger.finest("Renova o SGA " + sgaName);
			try {
				sgaService.isRegistered(SGAAzure.this, sgaName);
			}
			catch (InvalidSGAException e) {
				logger.log(Level.SEVERE, "Erro ao renovar o registro do SGA: {0}: {1}",
						new Object[] { e, e.message });
			}
			catch (NoPermissionException e) {
				logger.log(Level.SEVERE, "Erro ao renovar o registro do SGA: {0}: {1}",
						new Object[] { e, e.message });
			}
		}
	}

	/**
	 * Tarefa agendada que executa a chamada ao SGAService para atualizar as
	 * propriedades do SGA e seus nós de execução.
	 */
	private class SGAUpdatePropertiesTask extends TimerTask {
		/**
		 * {@inheritDoc}
		 */
		@Override
		public void run() {
			//logger.finest("Atualiza os dados do SGA  " + sgaName);
			SGAProperties sgaProps;
			//TODO monitor.update()
			sgaProps = loadSGAProperties();
			if (sgaProps.nodesProperties.length <= 0) {
				logger.log(Level.WARNING, "Não existem nós de SGAs");
			}
			else {
				try {
					if (sgaService.isRegistered(SGAAzure.this, sgaName)) {
						sgaService.updateSGAInfo(SGAAzure.this, sgaName, sgaProps);
					}
					else {
						registerSGA(sgaProps);
					}
				}
				catch (InvalidParameterException e) {
					logger.log(Level.SEVERE,
							"Erro ao atualizar as informações do SGA: {0}: {1}", new Object[] {
							e, e.message });
				}
				catch (NoPermissionException e) {
					logger.log(Level.SEVERE,
							"Erro ao atualizar as informações do SGA: {0}: {1}", new Object[] {
							e, e.message });
				}
				catch (SGANotRegisteredException e) {
					logger.log(Level.SEVERE,
							"Erro ao atualizar as informações do SGA: {0}: {1}", new Object[] {
							e, e.message });
				}
				catch (InvalidSGAException e) {
					logger.log(Level.SEVERE,
							"Erro ao atualizar as informações do SGA: {0}: {1}", new Object[] {
							e, e.message });
				}
			}
		}
	}

	@Override
	public void ping() {
		// Ok. Ping recebido. Estou vivo. Pode continuar trabalhando.
	}

	@Override
	public void setDefaultConfigKeys(String[] keys) {
		// SGALocal não implementa também.
		System.out.println("setDefaultConfigKeys ("+Arrays.toString(keys)+")");
	}

	@Override
	public void setDefaultInfoKeys(String[] keys) {
		// SGALocal não implementa também.
		System.out.println("setDefaultInfoKeys ("+Arrays.toString(keys)+")");
	}

	@Override
	public void control(SGAControlAction action) {
		logger.info("Solicitado " + action.value());
		if (action.equals(SGAControlAction.SHUTDOWN)) {
			try {
				sgaService.unregisterSGA(this, sgaName);
			}
			catch (NoPermissionException e) {
				logger.log(Level.SEVERE, "Erro ao desregistrar o SGA: {0}: {1}",
						new Object[] { e, e.message });
			}
			catch (SGANotRegisteredException e) {
				logger.log(Level.SEVERE, "Erro ao desregistrar o SGA: {0}: {1}",
						new Object[] { e, e.message });
			}
			stop();
		}
	}

	@Override
	public SGACommand executeCommand(String command, String cmdid, Pair[] extraParams) throws SystemException, MissingParameterException {
		return executor.executeCommand(command, cmdid, Utils.convertDicToMap(extraParams));
	}

	@Override
	public SGAPath getPath(String path) throws InvalidPathException, PathNotFoundException {
		
		System.out.println("SGAAzure.getPath("+path+")");
		
		return new SGAPath(path, 0, false, false, "", true, true, false, true);
	}

	@Override
	public SGAPath[] getPaths(String root) throws InvalidPathException, PathNotFoundException {
		
		System.out.println("SGAAzure.getPaths("+root+")");
		
		return new SGAPath[]{
				new SGAPath(root+"/1", 0, false, false, "", true, true, false, true),
				new SGAPath(root+"/2", 0, false, false, "", true, true, false, true)
		};
	}

}
