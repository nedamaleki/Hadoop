package org.cloudbus.cloudsim.examples;

public class TaskSchedule {

	int taskId;
	int vmId;
	double arrivalTime;
	double finishedTime;
	String partitionName;
	
	public String getPartitionName() {
		return partitionName;
	}
	public void setPartitionName(String partitionName) {
		this.partitionName = partitionName;
	}
	public int getTaskId() {
		return taskId;
	}
	public void setTaskId(int taskId) {
		this.taskId = taskId;
	}
	public int getVmId() {
		return vmId;
	}
	public void setVmId(int vmId) {
		this.vmId = vmId;
	}
	public double getArrivalTime() {
		return arrivalTime;
	}
	public void setArrivalTime(double arrivalTime) {
		this.arrivalTime = arrivalTime;
	}
	public double getFinishedTime() {
		return finishedTime;
	}
	public void setFinishedTime(double finishedTime) {
		this.finishedTime = finishedTime;
	}
	

	
}

