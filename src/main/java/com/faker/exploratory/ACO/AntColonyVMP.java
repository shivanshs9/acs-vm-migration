package com.faker.exploratory.ACO;

import static java.util.Comparator.comparingLong;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;

import com.faker.exploratory.StatePowerModel;

import org.cloudbus.cloudsim.allocationpolicies.VmAllocationPolicy;
import org.cloudbus.cloudsim.allocationpolicies.VmAllocationPolicyBestFit;
import org.cloudbus.cloudsim.allocationpolicies.VmAllocationPolicySimple;
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
import org.cloudbus.cloudsim.schedulers.cloudlet.CloudletSchedulerSpaceShared;
import org.cloudbus.cloudsim.schedulers.cloudlet.CloudletSchedulerTimeShared;
import org.cloudbus.cloudsim.schedulers.vm.VmSchedulerSpaceShared;
import org.cloudbus.cloudsim.schedulers.vm.VmSchedulerTimeShared;
import org.cloudbus.cloudsim.schedulers.vm.VmSchedulerTimeSharedOverSubscription;
import org.cloudbus.cloudsim.selectionpolicies.VmSelectionPolicy;
import org.cloudbus.cloudsim.selectionpolicies.VmSelectionPolicyMinimumUtilization;
import org.cloudbus.cloudsim.util.SwfWorkloadFileReader;
import org.cloudbus.cloudsim.utilizationmodels.UtilizationModel;
import org.cloudbus.cloudsim.utilizationmodels.UtilizationModelDynamic;
import org.cloudbus.cloudsim.vms.Vm;
import org.cloudbus.cloudsim.vms.VmSimple;
import org.cloudsimplus.builders.tables.CloudletsTableBuilder;
import org.cloudsimplus.listeners.EventListener;
import org.cloudsimplus.listeners.VmHostEventInfo;
import org.cloudsimplus.slametrics.SlaContract;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AntColonyVMP {
    private static final Logger LOGGER = LoggerFactory.getLogger(AntColonyVMP.class.getSimpleName());

    private static final int SCHEDULING_INTERVAL = 500;

    private static int HOSTS = 9;
    private static int HOST_PES = 4;

    private static final int VMS = 6;
    private static final int VM_PES = 2;

    private static final int VM_MIPS = 10000;

    private static final int CLOUDLET_PES = 2;
    private static final int CLOUDLET_LENGTH = 20000;

    private static final int CLOUDLETS_BY_VM = 2;

    private static final int NUMBER_OF_VMS_PER_HOST = 2;

    private static final long VM_SIZE = 2000;
    private static final int VM_RAM = 1000;
    private static final long VM_BW = 50000;

    /**
     * The minimum number of PEs to be created for each host.
     */
    private final int MINIMUM_NUM_OF_PES_BY_HOST = 16;

    /**
     * The workload file to be read.
     */
    private static final String WORKLOAD_FILENAME = "NASA-iPSC-1993-3.1-cln.swf.gz";

    /**
     * The base dir inside the resource directory to get SWF workload files.
     */
    private static final String WORKLOAD_BASE_DIR = "workload/swf/";

    /**
     * Defines the maximum number of cloudlets to be created from the given workload
     * file. The value -1 indicates that every job inside the workload file will be
     * created as one cloudlet.
     */
    private static final int maximumNumberOfCloudletsToCreateFromTheWorkloadFile = 20;

    /**
     * Defines the speed (in percentage) that CPU usage of a cloudlet will increase
     * during the simulation time. (in scale from 0 to 1, where 1 is 100%).
     */
    private static final double CLOUDLET_CPU_USAGE_INCREMENT_PER_SECOND = 0.04;

    /**
     * A {@link Comparator} that sorts VMs submitted to a broker by the VM's
     * required PEs number in decreasing order. This way, VMs requiring more PEs are
     * created first.
     *
     * @see DatacenterBroker#setVmComparator(Comparator)
     */
    private static final Comparator<Vm> VM_COMPARATOR = comparingLong(Vm::getNumberOfPes).reversed();

    private final CloudSim simulation;
    private Datacenter datacenter;
    private DatacenterBroker broker;
    private List<Vm> vmList;
    private List<Cloudlet> cloudlets;
    private SlaContract slaContract;
    private double totalConsumption = 0f;

    public enum MigrationAlgo {
        NONE, ACO, LR, MAD, BFST, IQR
    }

    private MigrationAlgo usedAlgo = MigrationAlgo.NONE;

    public static void main(String[] args) {
        new AntColonyVMP();
    }

    AntColonyVMP() {
        usedAlgo = MigrationAlgo.MAD;

        simulation = new CloudSim();
        VmAllocationPolicy vmAllocationPolicy;
        VmSelectionPolicy vmSelectionPolicy = new VmSelectionPolicyMinimumUtilization();

        initSlaContract();

        switch (usedAlgo) {
        case ACO:
            vmAllocationPolicy = new VMAllocationACO(vmSelectionPolicy,
                    slaContract.getCpuUtilizationMetric().getMaxDimension().getValue(),
                    slaContract.getCpuUtilizationMetric().getMinDimension().getValue());
            break;
        case LR:
            vmAllocationPolicy = new VmAllocationPolicyMigrationLocalRegression(vmSelectionPolicy);
            break;
        case MAD:
            vmAllocationPolicy = new VmAllocationPolicyMigrationMedianAbsoluteDeviation(vmSelectionPolicy);
            break;
        case BFST:
            vmAllocationPolicy = new VmAllocationPolicyMigrationBestFitStaticThreshold(vmSelectionPolicy,
                    slaContract.getCpuUtilizationMetric().getMaxDimension().getValue());
            break;
        case IQR:
            vmAllocationPolicy = new VmAllocationPolicyMigrationInterQuartileRange(vmSelectionPolicy);
            break;
        default:
            vmAllocationPolicy = new VmAllocationPolicyBestFit();
            break;
        }

        broker = new DatacenterBrokerSimple(simulation);
        broker.setVmComparator(VM_COMPARATOR);
        /*
         * Indicates that idle VMs must be destroyed right away (0 delay). This forces
         * the Host to become idle
         */
        // broker.setVmDestructionDelay(2.0);

        createCloudletsFromWorkloadFile();
        createOneVmForEachCloudlet();

        datacenter = createDatacenterAndHostBasedOnVmRequirements(vmAllocationPolicy);

        broker.submitCloudletList(cloudlets);
        broker.submitVmList(vmList);
        // vmList = createVms();
        // createAndSubmitCloudlets();

        simulation.start();

        final List<Cloudlet> finishedList = broker.getCloudletFinishedList();
        finishedList.sort(
                comparingLong((Cloudlet c) -> c.getVm().getHost().getId()).thenComparingLong(c -> c.getVm().getId()));
        new CloudletsTableBuilder(finishedList).build();
        System.out.println(
                "\n    WHEN A HOST CPU ALLOCATED MIPS IS LOWER THAN THE REQUESTED, IT'S DUE TO VM MIGRATION OVERHEAD)\n");

        printHostsHistory(datacenter.getHostList());

        if (usedAlgo == MigrationAlgo.ACO)
            System.out.printf("===ACO===\nTotal Migrations: %d\nTotal Sleeping hosts: %d\n",
                    ((VMAllocationACO) vmAllocationPolicy).totalVmMigrations,
                    ((VMAllocationACO) vmAllocationPolicy).sleepingHosts);
    }

    private void initSlaContract() {
        slaContract = SlaContract.getInstance("./CustomerSLA.json");
    }

    private void printHostsHistory(List<Host> hostList) {
        hostList.stream().forEach(host -> {
            totalConsumption += printHostHistory(host);
        });
        System.out.printf("Total consumption across all hosts: %6.2f Watt-Sec\n", totalConsumption);
    }

    private double printHostHistory(Host host) {
        // if (shouldPrintHostStateHistory(host)) {
        return printHostCpuUsageAndPowerConsumption(host);
        // }
        // return 0;
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
            double powerConsumption = 0;
            try {
                powerConsumption = ((StatePowerModel) host.getPowerModel()).getPowerWithState(hostCpuUsage, isActive);
            } catch (Exception ignored) {
            }
            // System.out.printf("Time: %6.1f | %9.2f | %.2f\n", time, hostCpuUsage,
            // powerConsumption);
            totalHostPowerConsumptionWattSec += powerConsumption;
            index++;
        }
        System.out.printf("Total Host power consumption in the period: %.2f Watt-Sec\n",
                totalHostPowerConsumptionWattSec);
        System.out.println();
        return totalHostPowerConsumptionWattSec;
    }

    private Datacenter createDatacenterAndHostBasedOnVmRequirements(VmAllocationPolicy allocationPolicy) {
        final List<Host> hostList = createHostsAccordingToVmRequirements();

        return new DatacenterSimple(simulation, hostList, allocationPolicy).enableMigrations()
                .setSchedulingInterval(SCHEDULING_INTERVAL);
    }

    private List<Host> createHostsAccordingToVmRequirements() {
        List<Host> hostList = new ArrayList<>();
        Map<Long, Long> vmsPesCountMap = getMapWithNumberOfVmsGroupedByRequiredPesNumber();
        long numberOfPesRequiredByVms, numberOfVms, numberOfVmsRequiringUpToTheMinimumPesNumber = 0;
        long totalOfHosts = 0, totalOfPesOfAllHosts = 0;
        for (Entry<Long, Long> entry : vmsPesCountMap.entrySet()) {
            numberOfPesRequiredByVms = entry.getKey();
            numberOfVms = entry.getValue(); // TODO: Extra 1 Host
            /*
             * For VMs requiring MINIMUM_NUM_OF_PES_BY_HOST or less PEs, it will be created
             * a set of Hosts which all of them contain this number of PEs.
             */
            if (numberOfPesRequiredByVms <= MINIMUM_NUM_OF_PES_BY_HOST) {
                numberOfVmsRequiringUpToTheMinimumPesNumber += numberOfVms;
            } else {
                hostList.addAll(createHostsOfSameCapacity(numberOfVms, numberOfPesRequiredByVms));
                totalOfHosts += numberOfVms;
                totalOfPesOfAllHosts += numberOfVms * numberOfPesRequiredByVms;
            }
        }

        totalOfHosts += numberOfVmsRequiringUpToTheMinimumPesNumber;
        totalOfPesOfAllHosts += numberOfVmsRequiringUpToTheMinimumPesNumber * MINIMUM_NUM_OF_PES_BY_HOST;
        List<Host> subList = createHostsOfSameCapacity(numberOfVmsRequiringUpToTheMinimumPesNumber,
                MINIMUM_NUM_OF_PES_BY_HOST);
        hostList.addAll(subList);
        System.out.printf("# Total of created hosts: %d Total of PEs of all hosts: %d\n\n", totalOfHosts,
                totalOfPesOfAllHosts);

        return hostList;
    }

    /**
     * Creates a specific number of PM's with the same capacity.
     *
     * @param numberOfHosts number of hosts to create
     * @param numberOfPes   number of PEs of the host
     * @return the created host
     */
    private List<Host> createHostsOfSameCapacity(long numberOfHosts, long numberOfPes) {
        final long ram = VM_RAM * NUMBER_OF_VMS_PER_HOST;
        final long storage = VM_SIZE * NUMBER_OF_VMS_PER_HOST;
        final long bw = VM_BW * NUMBER_OF_VMS_PER_HOST;

        List<Host> list = new ArrayList<>();
        for (int i = 0; i < numberOfHosts; i++) {
            List<Pe> peList = createPeList(numberOfPes, VM_MIPS);

            Host host = new HostSimple(ram, bw, storage, peList, true).setPowerModel(new StatePowerModel())
                    .setVmScheduler(new VmSchedulerTimeShared(0)).setRamProvisioner(new ResourceProvisionerSimple())
                    .setBwProvisioner(new ResourceProvisionerSimple());// .setIdleShutdownDeadline(5);
            host.enableStateHistory();

            list.add(host);
        }

        System.out.printf("# Created %d hosts with %d PEs each one\n", numberOfHosts, numberOfPes);

        return list;
    }

    private List<Pe> createPeList(long numberOfPes, long mips) {
        List<Pe> peList = new ArrayList<>();
        for (int i = 0; i < numberOfPes; i++) {
            peList.add(new PeSimple(mips, new PeProvisionerSimple()));
        }

        return peList;
    }

    /**
     * Gets a map containing the number of PEs that existing VMs require and the
     * total of VMs that required the same number of PEs. This map is a way to know
     * how many PMs will be required to host the VMs.
     *
     * @return a map that counts the number of VMs that requires the same amount of
     *         PEs. Each map key is number of PEs and each value is the number of
     *         VMs that require that number of PEs. For instance, a key = 8 and a
     *         value = 5 means there is 5 VMs that require 8 PEs.
     */
    private Map<Long, Long> getMapWithNumberOfVmsGroupedByRequiredPesNumber() {
        Map<Long, Long> vmsPesCountMap = new HashMap<>();
        for (Vm vm : vmList) {
            final long pesNumber = vm.getNumberOfPes();
            // checks if the map already has an entry to the given pesNumber
            Long numberOfVmsWithGivenPesNumber = vmsPesCountMap.get(pesNumber);
            if (numberOfVmsWithGivenPesNumber == null) {
                numberOfVmsWithGivenPesNumber = 0L;
            }
            // updates the number of VMs that have the given pesNumber
            vmsPesCountMap.put(pesNumber, ++numberOfVmsWithGivenPesNumber);
        }

        System.out.println();
        long totalOfVms = 0, totalOfPes = 0;
        for (Entry<Long, Long> entry : vmsPesCountMap.entrySet()) {
            totalOfVms += entry.getValue();
            totalOfPes += entry.getKey() * entry.getValue();
            System.out.printf("# There are %d VMs requiring %d PEs\n", entry.getValue(), entry.getKey());
        }
        System.out.printf("# Total of VMs: %d Total of required PEs of all VMs: %d\n", totalOfVms, totalOfPes);
        return vmsPesCountMap;
    }

    private void createCloudletsFromWorkloadFile() {
        final String fileName = WORKLOAD_BASE_DIR + WORKLOAD_FILENAME;
        SwfWorkloadFileReader reader = SwfWorkloadFileReader.getInstance(fileName, VM_MIPS);
        reader.setMaxLinesToRead(maximumNumberOfCloudletsToCreateFromTheWorkloadFile);
        this.cloudlets = reader.generateWorkload();

        System.out.printf("# Created %d Cloudlets for %s\n", cloudlets.size(), broker);
    }

    private void createOneVmForEachCloudlet() {
        int vmId = -1;
        vmList = new ArrayList<>();
        for (Cloudlet cloudlet : this.cloudlets) {
            Vm vm = new VmSimple(++vmId, VM_MIPS, cloudlet.getNumberOfPes()).setRam(VM_RAM).setBw(VM_BW)
                    .setSize(VM_SIZE).setCloudletScheduler(new CloudletSchedulerSpaceShared());
            vm.getUtilizationHistory().enable();
            vmList.add(vm);
            cloudlet.setVm(vm);
        }

        System.out.printf("# Created %d VMs for the %s\n", vmList.size(), broker);
    }

    // region OLD
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
            peList.add(new PeSimple(VM_MIPS, new PeProvisionerSimple()));
        }

        final long ram = 500000; // in Megabytes
        final long bw = 100000000L; // in Megabits/s
        final long storage = 1000000; // in Megabytes

        /*
         * Uses ResourceProvisionerSimple by default for RAM and BW provisioning and
         * VmSchedulerTimeShared for VM scheduling.
         */
        Host host = new HostSimple(ram, bw, storage, peList);
        host.setVmScheduler(new VmSchedulerTimeShared()).setPowerModel(new StatePowerModel())
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
            vm.setRam(10000).setBw(100000).setSize(1000).setCloudletScheduler(new CloudletSchedulerSpaceShared());
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

    // endregion
}