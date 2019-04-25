package com.faker.exploratory.ACO;

// region Imports

import java.util.ArrayList;
import java.util.Comparator;
import java.util.DoubleSummaryStatistics;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;

import com.faker.exploratory.StatePowerModel;

import org.cloudbus.cloudsim.allocationpolicies.VmAllocationPolicy;
import org.cloudbus.cloudsim.allocationpolicies.migration.VmAllocationPolicyMigrationBestFitStaticThreshold;
import org.cloudbus.cloudsim.allocationpolicies.migration.VmAllocationPolicyMigrationInterQuartileRange;
import org.cloudbus.cloudsim.allocationpolicies.migration.VmAllocationPolicyMigrationLocalRegression;
import org.cloudbus.cloudsim.allocationpolicies.migration.VmAllocationPolicyMigrationMedianAbsoluteDeviation;
import org.cloudbus.cloudsim.brokers.DatacenterBroker;
import org.cloudbus.cloudsim.brokers.DatacenterBrokerSimple;
import org.cloudbus.cloudsim.cloudlets.Cloudlet;
import org.cloudbus.cloudsim.cloudlets.CloudletSimple;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.datacenters.Datacenter;
import org.cloudbus.cloudsim.datacenters.DatacenterSimple;
import org.cloudbus.cloudsim.hosts.Host;
import org.cloudbus.cloudsim.hosts.HostSimple;
import org.cloudbus.cloudsim.hosts.HostStateHistoryEntry;
import org.cloudbus.cloudsim.provisioners.PeProvisionerSimple;
import org.cloudbus.cloudsim.provisioners.ResourceProvisionerSimple;
import org.cloudbus.cloudsim.resources.Pe;
import org.cloudbus.cloudsim.resources.PeSimple;
import org.cloudbus.cloudsim.schedulers.cloudlet.CloudletSchedulerTimeShared;
import org.cloudbus.cloudsim.schedulers.vm.VmScheduler;
import org.cloudbus.cloudsim.schedulers.vm.VmSchedulerTimeSharedOverSubscription;
import org.cloudbus.cloudsim.selectionpolicies.power.PowerVmSelectionPolicyMinimumUtilization;
import org.cloudbus.cloudsim.utilizationmodels.UtilizationModel;
import org.cloudbus.cloudsim.utilizationmodels.UtilizationModelDynamic;
import org.cloudbus.cloudsim.vms.Vm;
import org.cloudbus.cloudsim.vms.VmSimple;
import org.cloudsimplus.builders.tables.CloudletsTableBuilder;
import org.cloudsimplus.slametrics.SlaContract;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// endregion

public class AntColonyVMP {
    private static final Logger LOGGER = LoggerFactory.getLogger(AntColonyVMP.class.getSimpleName());

    private static final int SCHEDULING_INTERVAL = 5;

    private static int HOSTS = 9;
    private static int HOST_PES = 4;

    private static final int VMS = 6;
    private static final int VM_PES = 2;

    private static final int VM_MIPS = 1200;
    private static final int HOST_MIPS = 1000;

    private static final int CLOUDLET_PES = 2;
    private static final int CLOUDLET_LENGTH = 20000;

    private static final int CLOUDLETS_BY_VM = 2;

    /**
     * Defines the speed (in percentage) that CPU usage of a cloudlet will increase
     * during the simulation time. (in scale from 0 to 1, where 1 is 100%).
     */
    private static final double CLOUDLET_CPU_USAGE_INCREMENT_PER_SECOND = 0.04;

    private final CloudSim simulation;
    private Datacenter datacenter;
    private DatacenterBroker broker;
    private VMAllocationACO vmAllocationACO;
    private List<Vm> vmList;
    private double totalConsumption = 0f;

    public static void main(String[] args) {
        new AntColonyVMP();
    }

    AntColonyVMP() {
        simulation = new CloudSim();
        VmAllocationPolicy vmAllocationPolicy;

        // ACO
        vmAllocationPolicy = new VMAllocationACO(new PowerVmSelectionPolicyMinimumUtilization());

        // Local Regression
        // vmAllocationPolicy = new VmAllocationPolicyMigrationLocalRegression(
        // new PowerVmSelectionPolicyMinimumUtilization());

        // Media Absolute Deviation
        // vmAllocationPolicy = new VmAllocationPolicyMigrationMedianAbsoluteDeviation(
        // new PowerVmSelectionPolicyMinimumUtilization());

        // Best Fit Static Threshold
        // vmAllocationPolicy = new VmAllocationPolicyMigrationBestFitStaticThreshold(
        // new PowerVmSelectionPolicyMinimumUtilization(), 0.7);

        // Inter Quartile Range
        // vmAllocationPolicy = new VmAllocationPolicyMigrationInterQuartileRange(
        // new PowerVmSelectionPolicyMinimumUtilization());

        datacenter = createDatacenter(vmAllocationPolicy);

        broker = new DatacenterBrokerSimple(simulation);

        vmList = createVms();
        broker.submitVmList(vmList);
        createAndSubmitCloudlets();

        // initSlaContract();

        simulation.start();

        final List<Cloudlet> finishedList = broker.getCloudletFinishedList();
        finishedList.sort(Comparator.comparingLong((Cloudlet c) -> c.getVm().getHost().getId())
                .thenComparingLong(c -> c.getVm().getId()));
        new CloudletsTableBuilder(finishedList).build();
        System.out.println(
                "\n    WHEN A HOST CPU ALLOCATED MIPS IS LOWER THAN THE REQUESTED, IT'S DUE TO VM MIGRATION OVERHEAD)\n");

        printHostsHistory(datacenter.getHostList());
    }

    private void initSlaContract() {
        SlaContract sla = SlaContract.getInstance("./CustomerSLA.json");
    }

    private void printHostsHistory(List<Host> hostList) {
        hostList.stream().forEach(host -> {
            totalConsumption += printHostHistory(host);
        });
        System.out.printf("Total consumption across all hosts: %6.2f Watt-Sec\n", totalConsumption);
    }

    private double printHostHistory(Host host) {
        if (shouldPrintHostStateHistory(host)) {
            return printHostCpuUsageAndPowerConsumption(host);
        }
        return 0;
    }

    /**
     * Prints Host state history
     * 
     * @param host the host to print information
     * @return true if the Host was powered on during simulation, false otherwise
     */
    private boolean shouldPrintHostStateHistory(Host host) {
        if (host.getStateHistory().stream().anyMatch(HostStateHistoryEntry::isActive)) {
            System.out.printf("\nHost: %6d State History\n", host.getId());
            System.out.println(
                    "-------------------------------------------------------------------------------------------");
            host.getStateHistory().forEach(System.out::print);
            System.out.println();
            return true;
        } else
            System.out.printf("Host: %6d was powered off during all the simulation\n", host.getId());
        return false;
    }

    /**
     * Shows Host CPU utilization history and power consumption. The history is
     * shown in the interval defined by {@link #SCHEDULING_INTERVAL}, which is the
     * interval in which simulation is updated and usage data is collected.
     *
     * <p>
     * The Host CPU Utilization History also is only computed if VMs utilization
     * history is enabled by calling {@code vm.getUtilizationHistory().enable()}
     * </p>
     *
     * @param host the Host to print information
     */
    private double printHostCpuUsageAndPowerConsumption(final Host host) {
        System.out.printf("Host: %6d | CPU Usage | Power Consumption in Watt-Second (Ws)\n", host.getId());
        System.out
                .println("-------------------------------------------------------------------------------------------");
        SortedMap<Double, Double> utilizationHistory = host.getUtilizationHistorySum();
        // The total power the Host consumed in the period (in Watt-Sec)
        double totalHostPowerConsumptionWattSec = 0;
        int index = 0;
        for (Map.Entry<Double, Double> entry : utilizationHistory.entrySet()) {
            boolean isActive = true;
            double hostCpuUsage = entry.getValue();
            try {
                isActive = host.getStateHistory().get(index).isActive();
                // hostCpuUsage = host.getStateHistory().get(index).getAllocatedMips() /
                // host.getTotalMipsCapacity();
            } catch (IndexOutOfBoundsException ignored) {
            }
            final double time = entry.getKey();
            // The sum of CPU usage of every VM which has run in the Host
            double powerConsumption = ((StatePowerModel) host.getPowerModel()).getPowerWithState(hostCpuUsage,
                    isActive);
            System.out.printf("Time: %6.1f | %9.2f | %.2f\n", time, hostCpuUsage, powerConsumption);
            totalHostPowerConsumptionWattSec += powerConsumption;
            index++;
        }
        System.out.printf("Total Host power consumption in the period: %.2f Watt-Sec\n",
                totalHostPowerConsumptionWattSec);
        System.out.println();
        return totalHostPowerConsumptionWattSec;
    }

    private Datacenter createDatacenter(VmAllocationPolicy allocationPolicy) {
        final List<Host> hostList = new ArrayList<>(HOSTS);
        for (int i = 0; i < HOSTS; i++) {
            Host host = createHost(HOST_PES + i);
            hostList.add(host);
        }

        return new DatacenterSimple(simulation, hostList, allocationPolicy).setSchedulingInterval(SCHEDULING_INTERVAL);
    }

    private Host createHost(int peSize) {
        final List<Pe> peList = new ArrayList<>(peSize);
        // List of Host's CPUs (Processing Elements, PEs)
        for (int i = 0; i < peSize; i++) {
            peList.add(new PeSimple(HOST_MIPS, new PeProvisionerSimple()));
        }

        final long ram = 500000; // in Megabytes
        final long bw = 100000000L; // in Megabits/s
        final long storage = 1000000; // in Megabytes

        /*
         * Uses ResourceProvisionerSimple by default for RAM and BW provisioning and
         * VmSchedulerTimeShared for VM scheduling.
         */
        Host host = new HostSimple(ram, bw, storage, peList);
        host.setVmScheduler(vmScheduler).setPowerModel(new StatePowerModel())
                .setRamProvisioner(new ResourceProvisionerSimple()).setBwProvisioner(new ResourceProvisionerSimple());
        host.enableStateHistory();
        return host;
    }

    /**
     * Creates a list of VMs.
     */
    private List<Vm> createVms() {
        final List<Vm> list = new ArrayList<>(VMS);
        for (int i = 0; i < VMS; i++) {
            // Uses a CloudletSchedulerTimeShared by default to schedule Cloudlets
            final Vm vm = new VmSimple(VM_MIPS, VM_PES);
            vm.getUtilizationHistory().enable();
            vm.setRam(10000).setBw(100000).setSize(1000).setCloudletScheduler(new CloudletSchedulerTimeShared());
            list.add(vm);
        }

        return list;
    }

    /**
     * Creates a list of Cloudlets.
     */
    private void createAndSubmitCloudlets() {
        double initialCpu = 0.4;
        for (int i = 0; i < VMS; i++) {
            createCloudletsForVm(vmList.get(i), initialCpu, i % (VMS - 1) == 0);
            initialCpu += 0.05;
        }
    }

    private void createCloudletsForVm(Vm vm, double initialCpu, boolean dynamicUsage) {
        List<Cloudlet> cloudletList = new ArrayList<>(CLOUDLETS_BY_VM);
        for (int i = 0; i < CLOUDLETS_BY_VM; i++) {
            long cloudletId = vm.getId() + i;
            final Cloudlet cloudlet = new CloudletSimple(cloudletId, CLOUDLET_LENGTH, CLOUDLET_PES)
                    .setUtilizationModelCpu(createUtilizationModel(initialCpu, .9, dynamicUsage)).setSizes(1024);
            cloudletList.add(cloudlet);
        }
        broker.submitCloudletList(cloudletList);

        for (Cloudlet c : cloudletList) {
            broker.bindCloudletToVm(c, vm);
        }
    }

    private UtilizationModel createUtilizationModel(double initialCpuUsagePercent, double maxCloudletCpuUsagePercent,
            final boolean progressiveCpuUsage) {
        initialCpuUsagePercent = Math.min(initialCpuUsagePercent, 1);
        maxCloudletCpuUsagePercent = Math.min(maxCloudletCpuUsagePercent, 1);
        final UtilizationModelDynamic um = new UtilizationModelDynamic(initialCpuUsagePercent);

        if (progressiveCpuUsage) {
            um.setUtilizationUpdateFunction(this::getCpuUtilizationIncrement);
        }

        um.setMaxResourceUtilization(maxCloudletCpuUsagePercent);
        return um;
    }

    /**
     * Increments the CPU resource utilization, that is defined in percentage
     * values.
     *
     * @return the new resource utilization after the increment
     */
    private double getCpuUtilizationIncrement(final UtilizationModelDynamic um) {
        return um.getUtilization() + um.getTimeSpan() * CLOUDLET_CPU_USAGE_INCREMENT_PER_SECOND;
    }

    private VmScheduler vmScheduler = new VmSchedulerTimeSharedOverSubscription();
}