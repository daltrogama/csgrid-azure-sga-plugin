package csbase.azure;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;

import csbase.server.plugin.service.sgaservice.ISGADataTransfer;
import csbase.server.plugin.service.sgaservice.SGADataTransferException;

public class SSHSGADataTransfer implements ISGADataTransfer {

	private String host;
	private int port;
	private String privateKey;
	private String userName;

	public SSHSGADataTransfer(String host, int port, String userName, String privateKey) {
		this.host = host;
		this.port = port;
		this.userName = userName;
		this.privateKey = privateKey;
	}
	
	@Override
	public boolean checkExistence(String[] arg0)
			throws SGADataTransferException {
		SSHClientWrapper wrapper = new SSHClientWrapper();
		String filePath = StringUtils.join(arg0, "/");
		try {
			wrapper.connect(host, port, userName, privateKey);
			return wrapper.doSshExecution("stat " + filePath) == 0;
		} catch (Exception e) {
			return false;
		} finally {
			try {
				wrapper.disconnect();
			} catch (IOException e) {
			}
		}
	}

	@Override
	public void copyFrom(String[] arg0, String[] arg1)
			throws SGADataTransferException {
		SSHClientWrapper wrapper = new SSHClientWrapper();
		try {
			wrapper.connect(host, port, userName, privateKey);
			wrapper.doScpDownload(StringUtils.join(arg1, "/"), 
					StringUtils.join(arg0, "/"));
		} catch (Exception e) {
			throw new SGADataTransferException(e);
		} finally {
			try {
				wrapper.disconnect();
			} catch (IOException e) {
			}
		}
	}

	@Override
	public void copyTo(String[] arg0, String[] arg1)
			throws SGADataTransferException {
		SSHClientWrapper wrapper = new SSHClientWrapper();
		try {
			wrapper.connect(host, port, userName, privateKey);
			wrapper.doScpUpload(StringUtils.join(arg0, "/"), 
					StringUtils.join(arg1, "/"));
		} catch (Exception e) {
			throw new SGADataTransferException(e);
		} finally {
			try {
				wrapper.disconnect();
			} catch (IOException e) {
			}
		}
	}

	@Override
	public void createDirectory(String[] arg0) throws SGADataTransferException {
		// TODO Auto-generated method stub

	}

	@Override
	public String[] getAlgorithmsRootPath() throws SGADataTransferException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String[], Long> getLocalTimestamps(String[] arg0)
			throws SGADataTransferException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String[] getProjectsRootPath() throws SGADataTransferException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String[], Long> getRemoteTimestamps(String[] arg0)
			throws SGADataTransferException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void remove(String[] arg0) throws SGADataTransferException {
		// TODO Auto-generated method stub

	}

	@Override
	public void setSGAProperties(Properties arg0) {
		// TODO Auto-generated method stub

	}

}
