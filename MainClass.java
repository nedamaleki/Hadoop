package org.cloudbus.cloudsim.examples;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;

//public class MyBrokerPolicy {
//
//}

import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Scanner;
import java.util.stream.Collectors;


import org.cloudbus.cloudsim.CloudletSchedulerSpaceShared;
import org.cloudbus.cloudsim.Datacenter;
import org.cloudbus.cloudsim.DatacenterBroker;
import org.cloudbus.cloudsim.DatacenterCharacteristics;
import org.cloudbus.cloudsim.Host;
import org.cloudbus.cloudsim.Log;
import org.cloudbus.cloudsim.Pe;
import org.cloudbus.cloudsim.Storage;
import org.cloudbus.cloudsim.UtilizationModel;
import org.cloudbus.cloudsim.UtilizationModelFull;
import org.cloudbus.cloudsim.Vm;
import org.cloudbus.cloudsim.VmAllocationPolicySimple;
import org.cloudbus.cloudsim.VmSchedulerTimeShared;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.provisioners.BwProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.RamProvisionerSimple;

/**
 * A simple example showing how to create a datacenter with one host and run one
 * cloudlet on it.
 */
public class MainClass {

	static Scanner reader;
	/////////////////////////// All lists (variables) are defined here
	/** The cloudlet list. */
	private static List<CloudletWithOutputFile> cloudletList;
	/** The vmlist. */
	private static List<Vm> vmList;
	private static List<KeyValuePair> totalKeyValuePairList;
	private static List<TaskSchedule> mapTasksScheduleList;
	static List<PartitionInfo> partitionInfoList;
	static List<BandwidthInfo> bandwidthInfoList;
	static List<BandwidthInfo> transferTimeList;
	static List<BandwidthInfo> partitionToVmTransferTimeList;
	static List<TaskSchedule> reduceTasksScheduleList;
	static double baseAvailableTimeForReduceTasksScheduling;
	////////////////////////////////////////////////////////////////////////

	/**
	 * Creates main() to run this example.
	 *
	 * @param args the args
	 */
	@SuppressWarnings("unused")
	public static void main(String[] args) {

		Log.printLine("Starting Scheduling...");

		try {
			reader = new Scanner(System.in);
			// First step: Initialize the CloudSim package. It should be called
			// before creating any entities.
			int num_user = 1; // number of cloud users
			Calendar calendar = Calendar.getInstance();
			boolean trace_flag = false; // mean trace events

			// Initialize the CloudSim library
			CloudSim.init(num_user, calendar, trace_flag);

			// Second step: Create Datacenters
			// Datacenters are the resource providers in CloudSim. We need at
			// list one of them to run a CloudSim simulation
			Datacenter datacenter0 = createDatacenter("Datacenter_0");

			// Third step: Create Broker
			DatacenterBroker broker = createBroker();
			int brokerId = broker.getId();

			/**
			 * "cloudsim-3.0.3\examples\org\cloudbus\cloudsim\examples\csv\VMsCharacteristics.csv"
			 * cloudsim-3.0.3\examples\org\cloudbus\cloudsim\examples\csv\CloudletsCharacteristics.csv"
			 * cloudsim-3.0.3\examples\org\cloudbus\cloudsim\examples\csv\MapTasksOutput.csv"
			 * cloudsim-3.0.3\examples\org\cloudbus\cloudsim\examples\csv\Bandwidth.csv"
			 */

			// Fourth step: Create virtual machines
			vmList = InitializeVms(args[0], brokerId);

			// submit vm list to the broker
			broker.submitVmList(vmList);

			// Fifth step: Create Cloudlets
			cloudletList = InitializeCloudlets(args[1], brokerId);

			// here we make map output partitions from CSV file
			totalKeyValuePairList = CreateMapOutputDataFromCSV(args[2]);

			// submit cloudlet list to the broker
			broker.submitCloudletList(cloudletList);

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

			// Sixth step: Starts the simulation
			CloudSim.startSimulation();

			// Write your scheduler HERE

			CreateMapTaskExecutionTimeMatrix();
			mapTasksScheduleList = MapScheduling();
			reduceTasksScheduleList = ReduceScheduling(args[3]);

			CloudSim.stopSimulation();

			// Final step: Print results when simulation is over
			List<CloudletWithOutputFile> newList = broker.getCloudletReceivedList();
			// printCloudletList(newList);
			printCloudletList(mapTasksScheduleList);
			printCloudletList(reduceTasksScheduleList);

			Log.printLine("Scheduling finished!");

			generateCsvFile("c:\\MapTasksScheduling.csv", mapTasksScheduleList);
			//generateCsvFile("c:\\ReduceTasksScheduling.csv", reduceTasksScheduleList);

		} catch (Exception e) {
			e.printStackTrace();
			Log.printLine("Unwanted errors happen");
		}

		reader.close();
	}

	// ************************************************************************************************************************************
	/**
	 * Creates the datacenter.
	 *
	 * @param name the name
	 *
	 * @return the datacenter
	 */
	private static Datacenter createDatacenter(String name) {

		// Here are the steps needed to create a PowerDatacenter:
		// 1. We need to create a list to store
		// our machine
		List<Host> hostList = new ArrayList<Host>();

		// 2. A Machine contains one or more PEs or CPUs/Cores.
		// In this example, it will have only one core.
		List<Pe> peList1 = new ArrayList<Pe>();

		// int mips = 1000;

		// 3. Create PEs and add these into a list.

		peList1.add(new Pe(0, new PeProvisionerSimple(1000))); // need to store Pe id and MIPS Rating
		// peList1.add(new Pe(1, new PeProvisionerSimple(1000)));

		// Another list, for a dual-core machine
		List<Pe> peList2 = new ArrayList<Pe>();

		peList2.add(new Pe(0, new PeProvisionerSimple(500)));
		// peList2.add(new Pe(1, new PeProvisionerSimple(500)));

		// Another list, for a dual-core machine
		List<Pe> peList3 = new ArrayList<Pe>();

		peList3.add(new Pe(0, new PeProvisionerSimple(250)));
		// peList3.add(new Pe(1, new PeProvisionerSimple(250)));

		// 4. Create Hosts with its id and list of PEs and add them to the list of
		// machines
		int hostId = 0;
		int ram = 2048; // host memory (MB)
		long storage = 1000000; // host storage
		int bw = 10000;

		hostList.add(new Host(hostId, new RamProvisionerSimple(ram), new BwProvisionerSimple(bw), storage, peList1,
				new VmSchedulerTimeShared(peList1))); // This is our first machine

		hostId++;

		hostList.add(new Host(hostId, new RamProvisionerSimple(ram), new BwProvisionerSimple(bw), storage, peList2,
				new VmSchedulerTimeShared(peList2))); // Second machine

		hostId++;

		hostList.add(new Host(hostId, new RamProvisionerSimple(ram), new BwProvisionerSimple(bw), storage, peList3,
				new VmSchedulerTimeShared(peList3))); // Third machine

		////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

		// 5. Create a DatacenterCharacteristics object that stores the
		// properties of a data center: architecture, OS, list of
		// Machines, allocation policy: time- or space-shared, time zone
		// and its price (G$/Pe time unit).
		String arch = "x86"; // system architecture
		String os = "Linux"; // operating system
		String vmm = "Xen";
		double time_zone = 10.0; // time zone this resource located
		double cost = 3.0; // the cost of using processing in this resource
		double costPerMem = 0.05; // the cost of using memory in this resource
		double costPerStorage = 0.001; // the cost of using storage in this
										// resource
		double costPerBw = 0.0; // the cost of using bw in this resource
		LinkedList<Storage> storageList = new LinkedList<Storage>(); // we are not adding SAN
		// devices by now

		DatacenterCharacteristics characteristics = new DatacenterCharacteristics(arch, os, vmm, hostList, time_zone,
				cost, costPerMem, costPerStorage, costPerBw);

		// 6. Finally, we need to create a PowerDatacenter object.
		Datacenter datacenter = null;
		try {
			datacenter = new Datacenter(name, characteristics, new VmAllocationPolicySimple(hostList), storageList, 0);
		} catch (Exception e) {
			e.printStackTrace();
		}

		return datacenter;
	}

	// ************************************************************************************************************************************
	// We strongly encourage users to develop their own broker policies, to
	// submit vms and cloudlets according
	// to the specific rules of the simulated scenario
	/**
	 * Creates the broker.
	 *
	 * @return the datacenter broker
	 */
	private static DatacenterBroker createBroker() {
		DatacenterBroker broker = null;
		try {

			broker = new DatacenterBroker("Broker");

		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
		return broker;
	}

	// ************************************************************************************************************************************
	/**
	 * Prints the Cloudlet objects.
	 *
	 * @param list list of Cloudlets
	 */
//	private static void printCloudletList(List<CloudletWithOutputFile> list) {
//		int size = list.size();
//		CloudletWithOutputFile cloudlet;
//
//		String indent = "    ";
//		Log.printLine();
//		Log.printLine("========== OUTPUT ==========");
//		Log.printLine("Cloudlet ID" + indent + "STATUS" + indent + "Data center ID" + indent + "VM ID" + indent + "Time"
//				+ indent + "Start Time" + indent + "Finish Time");
//
//		DecimalFormat dft = new DecimalFormat("###.##");
//		for (int i = 0; i < size; i++) {
//			cloudlet = list.get(i);
//			Log.print(indent + cloudlet.getCloudletId() + indent + indent);
//
//			if (cloudlet.getCloudletStatus() == Cloudlet.SUCCESS) {
//				Log.print("SUCCESS");
//
//				Log.printLine(indent + indent + cloudlet.getResourceId() + indent + indent + indent + cloudlet.getVmId()
//						+ indent + indent + dft.format(cloudlet.getActualCPUTime()) + indent + indent
//						+ dft.format(cloudlet.getExecStartTime()) + indent + indent
//						+ dft.format(cloudlet.getFinishTime()));
//			}
//		}
//	}
	// ************************************************************************************************************************************

	private static void printCloudletList(List<TaskSchedule> list) {
		int size = list.size();
		TaskSchedule ts;

		String indent = "    ";
		Log.printLine();
		Log.printLine("========== OUTPUT ==========");
		Log.printLine("Cloudlet ID" + indent + "VM ID" + indent + "Start Time" + indent + "Finish Time");

		DecimalFormat dft = new DecimalFormat("###.##");
		for (int i = 0; i < size; i++) {
			ts = list.get(i);

			Log.printLine(indent + indent + dft.format(ts.getTaskId()) + indent + indent + dft.format(ts.getVmId())
					+ indent + indent + dft.format(ts.getArrivalTime()) + indent + indent + indent + indent
					+ dft.format(ts.getFinishedTime()));

		}
	}

	// ******************************************************************************************************************************
	private static void generateCsvFile(String sFileName, List<TaskSchedule> list) {
		try {
			int size = list.size();
			TaskSchedule ts;

			FileWriter writer = new FileWriter(sFileName);

			writer.append("Task ID");
			writer.append(',');
			writer.append("VM ID");
			writer.append(',');
			writer.append("Start Time");
			writer.append(',');
			writer.append("Finish Time");
			writer.append('\n');
			for (int i = 0; i < size; i++) {
				ts = list.get(i);
				String taskId = Integer.toString(ts.getTaskId());
				writer.append(taskId);
				writer.append(',');
				writer.append(Integer.toString(ts.getVmId()));
				writer.append(',');
				writer.append(Double.toString(ts.getArrivalTime()));
				writer.append(',');
				writer.append(Double.toString(ts.getFinishedTime()));
				writer.append('\n');

				
			}

			// generate whatever data you want

			writer.flush();
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	// *****************************************************************************************************************************
	public static List<Vm> InitializeVms(String addressOfCSVFile, int brokerId) throws FileNotFoundException {
		ArrayList<Vm> list = new ArrayList<Vm>();

		String line = "";
		String csvSplitBy = ",";

		// System.out.println(new java.io.File("").getAbsolutePath());
		// System.out.println(MainClass.class.getClassLoader().getResource("").getPath());

		try (BufferedReader br = new BufferedReader(new FileReader(new File(addressOfCSVFile)))) {

			line = br.readLine(); // this is required for skipping first line of csv file which is header
			while ((line = br.readLine()) != null) {

				String[] attributes = line.split(csvSplitBy);

				Vm vm = createVm(attributes, brokerId);

				// adding vm into ArrayList
				list.add(vm);

				// read next line before looping
				// if end of file reached, line would be null

			}

		} catch (IOException e) {
			e.printStackTrace();
		}

		return list;

	}

	// *****************************************************************************************************************************

	public static Vm createVm(String[] metadata, int brokerId) {

		// vm properties
		int vmId = Integer.parseInt(metadata[0]);
		int mips = Integer.parseInt(metadata[1]);
		int size = 10000; // image size (MB)
		int ram = 512; // vm memory (MB)
		int bw = 1000;
		String vmm = "Xen"; // VMM name
		int pesNumber = 1;
		Vm vm = new Vm(vmId, brokerId, mips, pesNumber, ram, bw, size, vmm, new CloudletSchedulerSpaceShared());
		return vm;

	}

	// *******************************************************************************************************************************
	public static List<CloudletWithOutputFile> InitializeCloudlets(String addressOfCSVFile, int brokerId) {

		List<CloudletWithOutputFile> list = new ArrayList<CloudletWithOutputFile>();
		String line = "";
		String csvSplitBy = ",";

		// System.out.println(new java.io.File("").getAbsolutePath());
		// System.out.println(MainClass.class.getClassLoader().getResource("").getPath());

		try (BufferedReader br = new BufferedReader(new FileReader(new File(addressOfCSVFile)))) {

			line = br.readLine(); // this is required for skipping first line of csv file which is header
			while ((line = br.readLine()) != null) {

				String[] attributes = line.split(csvSplitBy);

				CloudletWithOutputFile task = createTask(attributes, brokerId);

				// adding task into ArrayList
				list.add(task);

				// read next line before looping
				// if end of file reached, line would be null

			}

		} catch (IOException e) {
			e.printStackTrace();
		}

		return list;
	}

	// *****************************************************************************************************************************

	public static CloudletWithOutputFile createTask(String[] metadata, int brokerId) {

		// Cloudlet properties
		int id = Integer.parseInt(metadata[0]);
		int pesNumber = 1;
		long length = Integer.parseInt(metadata[1]);
		long fileSize = 128;
		long outputSize = 0;
		UtilizationModel utilizationModel = new UtilizationModelFull();
		boolean record = true;

		CloudletWithOutputFile cloudlet = new CloudletWithOutputFile(id, length, pesNumber, fileSize, outputSize,
				utilizationModel, utilizationModel, utilizationModel, record, 1);
		cloudlet.setUserId(brokerId);

		return cloudlet;

	}

	// *****************************************************************************************************************************

	public static List<KeyValuePair> CreateMapOutputDataFromCSV(String addressOfCSVFile) {
		// total list
		List<KeyValuePair> list = new ArrayList<KeyValuePair>();

		String line = "";
		String csvSplitBy = ",";
		KeyValuePair kv;

		try (BufferedReader br = new BufferedReader(new FileReader(new File(addressOfCSVFile)))) {

			line = br.readLine(); // this is required for skipping first line of csv file which is header
			while ((line = br.readLine()) != null) {

				String[] attributes = line.split(csvSplitBy);

				int taskId = Integer.parseInt(attributes[2]);

				kv = createMapOutput(attributes);

				for (int i = 0; i < cloudletList.size(); i++) {

					CloudletWithOutputFile cloudletListFilteredByTaskId = cloudletList.stream()
							.filter(p -> p.getCloudletId() == taskId).collect(Collectors.toList()).get(0);
					if (cloudletListFilteredByTaskId.mapOutputDataList == null) {
						cloudletListFilteredByTaskId.mapOutputDataList = new ArrayList<KeyValuePair>();

					} // end of if
					cloudletListFilteredByTaskId.AddToMapOutputDataList(kv);
					break;
				} // end of for

			} // end of while

			//////////////////////////////////////////////////////////////////////////////////////////////////////////

			// Calculate Total size of each partition

			boolean found;

			for (int i = 0; i < cloudletList.size(); i++) {
				for (int j = 0; j < cloudletList.get(i).getMapOutputDataList().size(); j++) {
					found = false;
					for (int k = 0; k < list.size(); k++) {
						if (list.get(k).getKey().equals(cloudletList.get(i).getMapOutputDataList().get(j).getKey())) {
							found = true;
							list.get(k).value += cloudletList.get(i).getMapOutputDataList().get(j).getValue();
							break;
						}
					}
					if (!found) {
						kv = new KeyValuePair(cloudletList.get(i).getMapOutputDataList().get(j).getKey(),
								cloudletList.get(i).getMapOutputDataList().get(j).getValue());
						list.add(kv);

					}
				}
			}
			return list;

		} catch (Exception e) {
			System.out.print(e);
			return null;
		}
	}
	// *****************************************************************************************************************************

	public static KeyValuePair createMapOutput(String[] metadata) {

		// KeyValue pair properties
		String partitionName = metadata[0];
		int partitionSize = Integer.parseInt(metadata[1]);

		KeyValuePair kv = new KeyValuePair(partitionName, partitionSize);

		return kv;

	}

	// *****************************************************************************************************************************
	public static double[][] CreateMapTaskExecutionTimeMatrix() {

		double[][] matrix = new double[cloudletList.size()][vmList.size()];

		// this for is used for filling the matrix (the execution time of each task on
		// each vm)
		for (int i = 0; i < cloudletList.size(); i++) {

			for (int j = 0; j < vmList.size(); j++) {

				matrix[i][j] = cloudletList.get(i).getCloudletLength() / vmList.get(j).getMips();

			} // end for

		} // end for

		// this for is used for printing a 2d matrix in matrix format in console
		for (int i = 0; i < matrix.length; i++) {
			for (int j = 0; j < matrix[i].length; j++) {
				System.out.print(matrix[i][j] + " ");
			}
			System.out.println();
		} // end for

		return matrix;
	}

	// *****************************************************************************************************************************

	public static List<TaskSchedule> MapScheduling() {

		List<TaskSchedule> _mapTasksScheduleList = new ArrayList<TaskSchedule>();
		TaskSchedule TS;
		for (int i = 0; i < cloudletList.size(); i++) {

			TS = new TaskSchedule();
			TS = DetectFastestVM(cloudletList.get(i), _mapTasksScheduleList);
			_mapTasksScheduleList.add(TS);

		} // end of for

		// This for is used for printing a 2d matrix in matrix format in console
		for (int i = 0; i < _mapTasksScheduleList.size(); i++) {
			System.out.print(_mapTasksScheduleList.get(i).getVmId() + " " + _mapTasksScheduleList.get(i).getTaskId()
					+ " " + _mapTasksScheduleList.get(i).getArrivalTime() + " "
					+ _mapTasksScheduleList.get(i).getFinishedTime() + " vmId=  "
					+ _mapTasksScheduleList.get(i).getVmId());
			System.out.println();
		} // end for
		return _mapTasksScheduleList;
	}
	// *****************************************************************************************************************************

	public static TaskSchedule DetectFastestVM(CloudletWithOutputFile currentTask,
			List<TaskSchedule> _mapTasksScheduleList) {

		TaskSchedule TS = new TaskSchedule();

		double availableTime = 0.0;
		double finishTime = 0.0;
		for (int i = 0; i < vmList.size(); i++) {

			availableTime = VMInMapTasksScheduleList(vmList.get(i).getId(), _mapTasksScheduleList);

			if (availableTime > 0.0) { // this means the vm is in matrix row

				finishTime = availableTime + (currentTask.getCloudletLength() / vmList.get(TS.getVmId()).getMips());
				if (TS.getFinishedTime() == 0.0 || finishTime < TS.getFinishedTime()) {
					TS.setVmId(_mapTasksScheduleList.get(i).getVmId());
					TS.setFinishedTime(finishTime);
					TS.setArrivalTime(availableTime);
					TS.setTaskId(currentTask.getCloudletId());
				}

			}

			else {

				double executionTime = currentTask.getCloudletLength() / vmList.get(i).getMips(); // this is equal to
																									// mapFinishTime
				if (TS.getFinishedTime() == 0.0 || executionTime < TS.getFinishedTime())

				{
					TS.setFinishedTime(executionTime);
					TS.setVmId(vmList.get(i).getId());
					TS.setArrivalTime(0.0);
					TS.setTaskId(currentTask.getCloudletId());

				}
			}

		}

		return TS;
	}

	// *****************************************************************************************************************************

	public static double VMInMapTasksScheduleList(int vmId, List<TaskSchedule> _mapTasksScheduleList) {

		double finishTime = 0.0; // this will be the vm available time
		// int selectedVMId;

		for (int i = 0; i < _mapTasksScheduleList.size(); i++) {

			if (_mapTasksScheduleList.get(i).getVmId() == vmId) {
				if (finishTime < _mapTasksScheduleList.get(i).getFinishedTime()) {
					finishTime = _mapTasksScheduleList.get(i).getFinishedTime();
				} // end of if
			} // end of if

		} // end of for

		return finishTime;
	}
	// ********************************************************************************************************************************

	public static List<TaskSchedule> ReduceScheduling(String addressOfCSVFile) {

		List<TaskSchedule> _reduceTasksScheduleList = new ArrayList<TaskSchedule>();

		CalculateReducersExecutionTime(totalKeyValuePairList);

		CalculateReduceStartTime(addressOfCSVFile);

		_reduceTasksScheduleList = CalculateReducersFinishTime();

		System.out.println();

		return _reduceTasksScheduleList;

	}

	// ********************************************************************************************************************************
	public static void CalculateReducersExecutionTime(List<KeyValuePair> totalKeyValuePairList) {

		List<ReduceTaskInfo> ReduceTaskInfoList = new ArrayList<ReduceTaskInfo>();
		// Reduce task execution time on vm1
		ReduceTaskInfo ReduceTaskInfo = new ReduceTaskInfo("R", 0, 5);
		ReduceTaskInfoList.add(ReduceTaskInfo);
		ReduceTaskInfo = new ReduceTaskInfo("R", 1, 10);
		ReduceTaskInfoList.add(ReduceTaskInfo);
		ReduceTaskInfo = new ReduceTaskInfo("R", 2, 20);
		ReduceTaskInfoList.add(ReduceTaskInfo);

		///////////////////////////////////////////////////////////////////////////////////////////////////////////////

		// This for is used for printing a 2d matrix in matrix format in console
		for (int i = 0; i < ReduceTaskInfoList.size(); i++) {

			System.out.print(ReduceTaskInfoList.get(i).getKey() + " Vm" + ReduceTaskInfoList.get(i).getVmId() + " "
					+ ReduceTaskInfoList.get(i).getExecutionTime());

			System.out.println();
		} // end for

		////////////////////////////////////////////////////////////////////////////////////////////////////////////////

		String partitionName;
		int numberOfReducers;
		double executionTime;
		partitionInfoList = new ArrayList<PartitionInfo>();

		for (int i = 0; i < totalKeyValuePairList.size(); i++) {

			partitionName = totalKeyValuePairList.get(i).getKey();
			numberOfReducers = (int) Math.ceil(totalKeyValuePairList.get(i).getValue() / 1000) + 1;
			for (int j = 0; j < ReduceTaskInfoList.size(); j++) {

				executionTime = (numberOfReducers / 1) * ReduceTaskInfoList.get(j).getExecutionTime();
				PartitionInfo partitionInfo = new PartitionInfo(partitionName, ReduceTaskInfoList.get(j).getVmId(),
						executionTime);
				partitionInfoList.add(partitionInfo);
				System.out.println();

			} // end of for

		} // end of for

		/////////////////////////////////////////////////////////////////////////////////////////////////////////////////

		// This for is used for printing the partition (is calculated using number of
		// reducers) execution time on each vm
		for (int i = 0; i < partitionInfoList.size(); i++) {

			System.out.print(partitionInfoList.get(i).getKey() + " " + partitionInfoList.get(i).getVmId() + " "
					+ partitionInfoList.get(i).getExecutionTime());
			System.out.println();

		}
		System.out.println();

	}

	// ********************************************************************************************************************************

	public static void CalculateReduceStartTime(String addressOfCSVFile) {

		bandwidthInfoList = new ArrayList<BandwidthInfo>();
		String line = "";
		String csvSplitBy = ",";

		try (BufferedReader br = new BufferedReader(new FileReader(new File(addressOfCSVFile)))) {

			line = br.readLine(); // This is required for skipping the csv file header (first line of csv file)
			while ((line = br.readLine()) != null) {

				String[] attributes = line.split(csvSplitBy);

				BandwidthInfo bandwidthInfo = createBandwidth(attributes);

				// adding task into ArrayList
				bandwidthInfoList.add(bandwidthInfo);

				// read next line before looping
				// if end of file reached, line would be null

			}

		} catch (IOException e) {
			e.printStackTrace();
		}

		/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

		// This for is used for printing bandwidth info between nodes
		for (int i = 0; i < bandwidthInfoList.size(); i++) {

			System.out.print(bandwidthInfoList.get(i).getSourceNodeId() + " "
					+ bandwidthInfoList.get(i).getDestinationNodeId() + " " + bandwidthInfoList.get(i).getBandwidth());
			System.out.println();

		} // end of for

		/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

		System.out.println();

		// Step2: find maximum time of column 4 of mapTasksScheduleList
		baseAvailableTimeForReduceTasksScheduling = 1;
		for (int i = 0; i < mapTasksScheduleList.size(); i++) {
			if (mapTasksScheduleList.get(i).getFinishedTime() > baseAvailableTimeForReduceTasksScheduling) {
				baseAvailableTimeForReduceTasksScheduling = mapTasksScheduleList.get(i).getFinishedTime();
			} // end of if
		} // end of for
		System.out.println("all map tasks finish at " + baseAvailableTimeForReduceTasksScheduling + " seconds");

		/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

		// Step3: transfer time of a partition from all other nodes into a node

		// String currentPartitionName;
		// Vm currentVm;
		
		double transferTime = 0.0;

		transferTimeList = new ArrayList<BandwidthInfo>();
		int dataVolume = 0;

		for (int i = 0; i < totalKeyValuePairList.size(); i++) {
			String currentPartitionName = totalKeyValuePairList.get(i).getKey();
			for (int j = 0; j < vmList.size(); j++) {
				Vm currentVm = vmList.get(j);
				for (int k = 0; k < vmList.size(); k++) {

					if (vmList.get(k) != currentVm) {
						List<TaskSchedule> mapTasksScheduleListFilteredByVMId;
						int m = k;
						// Filter mapTasksScheduleList Return: the Tasks belong to (exist
						// in/run on) this vm
						mapTasksScheduleListFilteredByVMId = mapTasksScheduleList.stream()
								.filter(p -> p.getVmId() == vmList.get(m).getId()).collect(Collectors.toList());
						dataVolume = 0;
						for (int r = 0; r < mapTasksScheduleListFilteredByVMId.size(); r++) {
							for (int q = 0; q < cloudletList.size(); q++) {
								if (mapTasksScheduleListFilteredByVMId.get(r).taskId == cloudletList.get(q)
										.getCloudletId()) {
									int w = r;
									List<CloudletWithOutputFile> cl = cloudletList.stream().filter(
											p -> p.getCloudletId() == mapTasksScheduleListFilteredByVMId.get(w).taskId)
											.collect(Collectors.toList());
									for (int idx = 0; idx < cl.get(0).getMapOutputDataList().size(); idx++) {
										if (cl.get(0).getMapOutputDataList().get(idx)
												.getKey() == currentPartitionName) {
											dataVolume += cl.get(0).getMapOutputDataList().get(idx).getValue();
										} // end of if

									} // end of for idx
								}// end of if
								
							} // end of for q
						} // end of for r

						if (dataVolume == 0) {
							transferTime = 0.0;

						} else {
							transferTime = CalculateTransferTime(dataVolume, currentVm, vmList.get(k));

						}
						System.out.println();

					} // end of if

					else {

						// System.out.println("Transfer Time is zero");
						transferTime = 0.0;

					}

					// create transferTimeList
					// BandwidthInfo transferTime=new BandwidthInfo(sourceNodeId, destinationNodeId,
					// transferTime, currentPartitionName)
					transferTimeList.add(new BandwidthInfo(vmList.get(k).getId(), currentVm.getId(), transferTime,
							currentPartitionName));

				} // end of for k

			} // end of for j
		} // end of for i

		// Create one (partition) to one (VM) Transfer Time List (each partition sizes
		// (e.g. of P1) on other VMs to a specific VM transfer time)

		partitionToVmTransferTimeList = CalculatePartitionToVmTransferTime(transferTimeList);

	}

	// ********************************************************************************************************************************

	public static BandwidthInfo createBandwidth(String[] metadata) {

		// Bandwidth between two nodes
		int sourceNodeId = Integer.parseInt(metadata[0]);
		int destinationNodeId = Integer.parseInt(metadata[1]);
		double bandwidth = Double.parseDouble(metadata[2]);

		BandwidthInfo bandwidthSD = new BandwidthInfo(sourceNodeId, destinationNodeId, bandwidth);

		return bandwidthSD;

	}
	// *********************************************************************************************************************************

	public static double CalculateTransferTime(int dataVolume, Vm sourceNode, Vm destinationNode) {

		double bandwidth = bandwidthInfoList.stream().filter(p -> (p.getSourceNodeId() == sourceNode.getId()
				&& p.destinationNodeId == destinationNode.getId())
				|| (p.getSourceNodeId() == destinationNode.getId() && p.destinationNodeId == sourceNode.getId()))
				.collect(Collectors.toList()).get(0).getBandwidth();

		return dataVolume / bandwidth;

	}

	// ***********************************************************************************************************************************
	public static List<BandwidthInfo> CalculatePartitionToVmTransferTime(List<BandwidthInfo> transferTimeList) {

		double maxOfTransferTime = 0.0;
		List<BandwidthInfo> _partitionToVmTransferTimeList = new ArrayList<BandwidthInfo>();

		for (int i = 0; i < vmList.size(); i++) {
			int m = i;
			for (int idx = 0; idx < totalKeyValuePairList.size(); idx++) {
				int n = idx;
				BandwidthInfo maxOfTransferTimeRecord = transferTimeList.stream()
						.filter(p -> p.getDestinationNodeId() == vmList.get(m).getId()
								&& p.getPartitionName() == totalKeyValuePairList.get(n).getKey())
						.max(Comparator.comparing(BandwidthInfo::getTransferTime))
						.orElseThrow(NoSuchElementException::new);
				maxOfTransferTime = maxOfTransferTimeRecord.getTransferTime();

				_partitionToVmTransferTimeList.add(new BandwidthInfo(-1, vmList.get(m).getId(), maxOfTransferTime,
						totalKeyValuePairList.get(n).getKey()));

			}
		}

		return _partitionToVmTransferTimeList;
	}
	// *************************************************************************************************************************************

	public static List<TaskSchedule> CalculateReducersFinishTime() {

		List<TaskSchedule> _reduceTasksScheduleList = new ArrayList<TaskSchedule>();
		TaskSchedule TS;
		for (int i = 0; i < totalKeyValuePairList.size(); i++) {

			TS = new TaskSchedule();
			TS = DetectFastestVMForPartitionProcessing(totalKeyValuePairList.get(i), _reduceTasksScheduleList);
			_reduceTasksScheduleList.add(TS);

		} // end of for

		// This for is used for printing a 2d matrix in matrix format in console
		for (int i = 0; i < _reduceTasksScheduleList.size(); i++) {
			System.out
					.print(_reduceTasksScheduleList.get(i).getVmId() + " " + _reduceTasksScheduleList.get(i).getTaskId()
							+ " " + _reduceTasksScheduleList.get(i).getArrivalTime() + " "
							+ _reduceTasksScheduleList.get(i).getFinishedTime() + " vmId=  "
							+ _reduceTasksScheduleList.get(i).getVmId());
			System.out.println();
		} // end for
		return _reduceTasksScheduleList;

	}
	// ****************************************************************************************************************************************

	public static TaskSchedule DetectFastestVMForPartitionProcessing(KeyValuePair currentPartition,
			List<TaskSchedule> _reduceTasksScheduleList) {

		TaskSchedule TS = new TaskSchedule();
		TS.setFinishedTime(baseAvailableTimeForReduceTasksScheduling);

		double availableTime = baseAvailableTimeForReduceTasksScheduling;
		double finishTime = 0.0;
		double transferTime;

		for (int i = 0; i < vmList.size(); i++) {

			availableTime = VMInReduceTasksScheduleList(vmList.get(i).getId(), _reduceTasksScheduleList);
			int idx = i;
			if (availableTime > baseAvailableTimeForReduceTasksScheduling) { // this means the vm is already in matrix
																				// row
				transferTime = partitionToVmTransferTimeList.stream()
						.filter(p -> p.getDestinationNodeId() == vmList.get(idx).getId()
								&& p.getPartitionName() == currentPartition.getKey())
						.collect(Collectors.toList()).get(0).getTransferTime();
				availableTime = Math.max(availableTime, transferTime);

				double executionTime = partitionInfoList.stream()
						.filter(p -> p.getVmId() == vmList.get(idx).getId() && p.getKey() == currentPartition.getKey())
						.collect(Collectors.toList()).get(0).getExecutionTime();
				finishTime = availableTime + executionTime;
				if (TS.getFinishedTime() == baseAvailableTimeForReduceTasksScheduling
						|| finishTime < TS.getFinishedTime()) {
					TS.setVmId(_reduceTasksScheduleList.get(i).getVmId());
					TS.setFinishedTime(finishTime);
					TS.setArrivalTime(availableTime);
					TS.setPartitionName(currentPartition.getKey());
				}

			}

			else {

				transferTime = partitionToVmTransferTimeList.stream()
						.filter(p -> p.getDestinationNodeId() == vmList.get(idx).getId()
								&& p.getPartitionName() == currentPartition.getKey())
						.collect(Collectors.toList()).get(0).getTransferTime();
				availableTime = Math.max(baseAvailableTimeForReduceTasksScheduling, transferTime);

				finishTime = partitionInfoList.stream()
						.filter(p -> p.getVmId() == vmList.get(idx).getId() && p.getKey() == currentPartition.getKey())
						.collect(Collectors.toList()).get(0).getExecutionTime() + availableTime; // this is equal to
																									// Reduce Finish
																									// Time

				if (TS.getFinishedTime() == baseAvailableTimeForReduceTasksScheduling
						|| finishTime < TS.getFinishedTime())

				{
					TS.setFinishedTime(finishTime);
					TS.setVmId(vmList.get(i).getId());
					TS.setArrivalTime(baseAvailableTimeForReduceTasksScheduling);
					TS.setPartitionName(currentPartition.getKey());

				}
			}

		}

		return TS;
	}

	// **************************************************************************************************************************************

	public static double VMInReduceTasksScheduleList(int vmId, List<TaskSchedule> _reduceTasksScheduleList) {

		double finishTime = baseAvailableTimeForReduceTasksScheduling; // this will be the vm available time

		for (int i = 0; i < _reduceTasksScheduleList.size(); i++) {

			if (_reduceTasksScheduleList.get(i).getVmId() == vmId) {
				if (finishTime < _reduceTasksScheduleList.get(i).getFinishedTime()) {
					finishTime = _reduceTasksScheduleList.get(i).getFinishedTime();

				} // end of if
			} // end of if

		} // end of for

		return finishTime;
	}

	// ****************************************************************************************************************************************

}// end of class
