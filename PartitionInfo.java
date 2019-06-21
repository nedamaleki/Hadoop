package org.cloudbus.cloudsim.examples;

public class PartitionInfo {

	
	String key;
	int vmId;
	double executionTime;
	
	
	public PartitionInfo(String key, int vmId, double executionTime) {
		
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
	public void setVmId(int vmId) {
		this.vmId = vmId;
	}
	public double getExecutionTime() {
		return executionTime;
	}
	public void setExecutionTime(double executionTime) {
		this.executionTime = executionTime;
	}
	
	
	
	
	
}
