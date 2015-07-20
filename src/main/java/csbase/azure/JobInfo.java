/**
 * $Id: JobInfo.java 163602 2015-04-14 18:56:26Z fpina $
 */
package csbase.azure;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import sgaidl.COMMAND_BYTES_IN_KB;
import sgaidl.COMMAND_BYTES_OUT_KB;
import sgaidl.COMMAND_CPU_PERC;
import sgaidl.COMMAND_CPU_TIME_SEC;
import sgaidl.COMMAND_DISK_BYTES_READ_KB;
import sgaidl.COMMAND_DISK_BYTES_WRITE_KB;
import sgaidl.COMMAND_EXEC_HOST;
import sgaidl.COMMAND_MEMORY_RAM_SIZE_MB;
import sgaidl.COMMAND_MEMORY_SWAP_SIZE_MB;
import sgaidl.COMMAND_PID;
import sgaidl.COMMAND_PPID;
import sgaidl.COMMAND_STATE;
import sgaidl.COMMAND_SYSTEM_TIME_SEC;
import sgaidl.COMMAND_USER_TIME_SEC;
import sgaidl.COMMAND_VIRTUAL_MEMORY_SIZE_MB;
import sgaidl.COMMAND_WALL_TIME_SEC;
import sgaidl.ProcessState;

/**
 * informa��es sobre a execu��o do job no ambiente de execu��o.
 *
 * @author Tecgraf/PUC-Rio
 */
public class JobInfo {
  private String defaulValue = "-1";
  public Map<String, String> jobParam;
  public List<JobInfo> children;

  public JobInfo() {
    jobParam = new HashMap<>();
    children = new LinkedList<>();

    jobParam.put(COMMAND_PID.value, "0");
    jobParam.put(COMMAND_PPID.value, "0");
    //    jobParam.put(COMMAND_STRING.value, defaulValue);
    jobParam.put(COMMAND_EXEC_HOST.value, "unknown");
    jobParam.put(COMMAND_STATE.value, ProcessState.WAITING.toString());
    jobParam.put(COMMAND_MEMORY_RAM_SIZE_MB.value, defaulValue);
    jobParam.put(COMMAND_MEMORY_SWAP_SIZE_MB.value, defaulValue);
    jobParam.put(COMMAND_CPU_PERC.value, defaulValue);
    jobParam.put(COMMAND_CPU_TIME_SEC.value, defaulValue);
    jobParam.put(COMMAND_WALL_TIME_SEC.value, defaulValue);
    jobParam.put(COMMAND_USER_TIME_SEC.value, defaulValue);
    jobParam.put(COMMAND_SYSTEM_TIME_SEC.value, defaulValue);
    jobParam.put(COMMAND_VIRTUAL_MEMORY_SIZE_MB.value, defaulValue);
    jobParam.put(COMMAND_BYTES_IN_KB.value, defaulValue);
    jobParam.put(COMMAND_BYTES_OUT_KB.value, defaulValue);
    jobParam.put(COMMAND_DISK_BYTES_READ_KB.value, defaulValue);
    jobParam.put(COMMAND_DISK_BYTES_WRITE_KB.value, defaulValue);

    children = new LinkedList<JobInfo>();
  }
}
