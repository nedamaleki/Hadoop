package org.cloudbus.cloudsim.examples;

public class ReduceTaskInfo {

	
	String key;
	int vmId;
	double executionTime;
	
	//private List<ReduceTaskInfo> reduceTaskInfo;
	
	public ReduceTaskInfo(String key, int vmId, double executionTime){
		this.key=key;
		this.vmId=vmId;
		this.executionTime=executionTime;
		
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public int getVmId() {
		return vmId;
	}

	public void setVmvalue(int vmId) {
		this.vmId = vmId;
	}

	public double getExecutionTime() {
		return executionTime;
	}

	public void setExecutionTime(double executionTime) {
		this.executionTime = executionTime;
	}
	
	
	
	
	
}
