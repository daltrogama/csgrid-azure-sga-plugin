package csbase.azure;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

class AzureVMMaintanceThread extends Thread {

	private final Logger logger = Logger.getLogger(this.getClass().getName());

	private final AtomicBoolean demandRun = new AtomicBoolean(true);
	
	private final Executor asyncExecutor = Executors.newCachedThreadPool();
	private final Set<Thread> asyncExecThreads = Collections.synchronizedSet(new HashSet<Thread>());

	private static final long REFRESH_INTERVAL = 60000; // Um minuto
	
	private long lastRun = 0;
	private final AzureConnector connector;
	
	@Override
	public void run() {
		try {
			while (!Thread.currentThread().isInterrupted()){
				try{
					if (demandRun.getAndSet(false) || (System.currentTimeMillis() - lastRun) >=REFRESH_INTERVAL){
						lastRun = System.currentTimeMillis();
						
						connector.refreshAllVMs();
						long queuedMessages = connector.getCommandQueueSize();
						if (queuedMessages > 0){
							int vmDemand = (int)queuedMessages - connector.getAllVMs().size();
							if (vmDemand > 0){
								createVMs(vmDemand);
							}
						}
						
					}
				} catch (Throwable e){
					logger.log(Level.SEVERE, "Falha ao verificar as máquinas virtuais Azure. Tentarei de novo em breve.", e);
					Thread.sleep(10000);
				}
			
				Thread.sleep(500);
			}
		} catch (InterruptedException e) {
			logger.log(Level.INFO, "Thread de manutenção de máquinas virtuais Azure interrompida.", e);
		}
		for (Thread t : new ArrayList<Thread>(asyncExecThreads)){
			t.interrupt();
		}
	}

	private void createVMs(final int vmDemand) throws InterruptedException {
		
		final CountDownLatch latch = new CountDownLatch(vmDemand);
		
		logger.info("Disparando a criação de "+vmDemand+" máquina(s) virtual(is)...");
		long timer = System.currentTimeMillis();
		
		for (int i=0; i<vmDemand; i+=1){
			asyncExecutor.execute(new Runnable() {
				@Override
				public void run() {
					asyncExecThreads.add(Thread.currentThread());
					try{
						try {
							connector.createNewVirtualMachine("Standard_D1");
						} catch (Throwable e) {
							logger.log(Level.SEVERE, "Não foi possível criar máquina virtual", e);
						}
					}
					finally{
						asyncExecThreads.remove(Thread.currentThread());
						latch.countDown();
					}
				}
			});
		}
		
		latch.await();
		
		timer = System.currentTimeMillis() - timer;
		logger.info(vmDemand+" máquina(s) virtual(is) criada(s) em "+timer+"ms.");
		
	}

	public void demandRun(){
		demandRun.set(true);
	}

	public AzureVMMaintanceThread(AzureConnector connector) {
		super("SGAAzure VM Maintance Thread");
		setDaemon(true);
		this.connector = connector;
	}
	
}
