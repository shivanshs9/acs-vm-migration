package com.faker.exploratory.ACO;

// region Imports

import java.util.ArrayList;
import java.util.List;

import org.cloudbus.cloudsim.brokers.DatacenterBroker;
import org.cloudbus.cloudsim.brokers.DatacenterBrokerSimple;
import org.cloudbus.cloudsim.cloudlets.Cloudlet;
import org.cloudbus.cloudsim.cloudlets.CloudletSimple;
import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.datacenters.Datacenter;
import org.cloudbus.cloudsim.datacenters.DatacenterSimple;
import org.cloudbus.cloudsim.hosts.Host;
import org.cloudbus.cloudsim.hosts.HostSimple;
import org.cloudbus.cloudsim.power.models.PowerModelLinear;
import org.cloudbus.cloudsim.provisioners.ResourceProvisionerSimple;
import org.cloudbus.cloudsim.resources.Pe;
import org.cloudbus.cloudsim.resources.PeSimple;
import org.cloudbus.cloudsim.schedulers.cloudlet.CloudletSchedulerTimeShared;
import org.cloudbus.cloudsim.schedulers.vm.VmSchedulerTimeShared;
import org.cloudbus.cloudsim.utilizationmodels.UtilizationModel;
import org.cloudbus.cloudsim.utilizationmodels.UtilizationModelDynamic;
import org.cloudbus.cloudsim.vms.Vm;
import org.cloudbus.cloudsim.vms.VmSimple;

// endregion

public class AntColonyVMP {
    private static int HOSTS = 10;
    private static int HOST_PES = 12;

    private static final int VMS = HOSTS;
    private static final int VM_PES = 5;

    private static final int CLOUDLET_PES = 2;
    private static final int CLOUDLET_LENGTH = 20000;

    private static final int CLOUDLETS_BY_VM = 4;

    /**
     * Defines the speed (in percentage) that CPU usage of a cloudlet will increase
     * during the simulation time. (in scale from 0 to 1, where 1 is 100%).
     */
    private static final double CLOUDLET_CPU_USAGE_INCREMENT_PER_SECOND = 0.05;

    private final CloudSim simulation;
    private Datacenter datacenter;
    private DatacenterBroker broker;
    private VMAllocationACO vmAllocationACO;
    private List<Vm> vmList;

    public static void main(String[] args) {
        new AntColonyVMP();
    }

    AntColonyVMP() {
        simulation = new CloudSim();
        datacenter = createDatacenter();

        broker = new DatacenterBrokerSimple(simulation);

        vmList = createVms();
        broker.submitVmList(vmList);
        createAndSubmitCloudlets();

        simulation.start();
    }

    private Datacenter createDatacenter() {
        final List<Host> hostList = new ArrayList<>(HOSTS);
        for (int i = 0; i < HOSTS; i++) {
            Host host = createHost();
            hostList.add(host);
        }

        vmAllocationACO = new VMAllocationACO();
        return new DatacenterSimple(simulation, hostList, vmAllocationACO);
    }

    private Host createHost() {
        final List<Pe> peList = new ArrayList<>(HOST_PES);
        // List of Host's CPUs (Processing Elements, PEs)
        for (int i = 0; i < HOST_PES; i++) {
            // Uses a PeProvisionerSimple by default to provision PEs for VMs
            peList.add(new PeSimple(1000));
        }

        final long ram = 500000; // in Megabytes
        final long bw = 100000000L; // in Megabits/s
        final long storage = 1000000; // in Megabytes

        /*
         * Uses ResourceProvisionerSimple by default for RAM and BW provisioning and
         * VmSchedulerTimeShared for VM scheduling.
         */
        Host host = new HostSimple(ram, bw, storage, peList);
        host.setVmScheduler(new VmSchedulerTimeShared()).setPowerModel(new PowerModelLinear(1000, 0.7))
                .setRamProvisioner(new ResourceProvisionerSimple()).setBwProvisioner(new ResourceProvisionerSimple());
        return host;
    }

    /**
     * Creates a list of VMs.
     */
    private List<Vm> createVms() {
        final List<Vm> list = new ArrayList<>(VMS);
        for (int i = 0; i < VMS; i++) {
            // Uses a CloudletSchedulerTimeShared by default to schedule Cloudlets
            final Vm vm = new VmSimple(1000, VM_PES);
            vm.setRam(10000).setBw(100000).setSize(1000).setCloudletScheduler(new CloudletSchedulerTimeShared());
            list.add(vm);
        }

        return list;
    }

    /**
     * Creates a list of Cloudlets.
     */
    private void createAndSubmitCloudlets() {
        double initialCpu = 0.6;
        for (int i = 0; i < VMS - 1; i++) {
            createCloudletsForVm(vmList.get(i), initialCpu, false);
            initialCpu += 0.05;
        }
        createCloudletsForVm(vmList.get(vmList.size() - 1), initialCpu, true);
    }

    private void createCloudletsForVm(Vm vm, double initialCpu, boolean dynamicUsage) {
        List<Cloudlet> cloudletList = new ArrayList<>(CLOUDLETS_BY_VM);
        for (int i = 0; i < CLOUDLETS_BY_VM; i++) {
            long cloudletId = vm.getId() + i;
            final Cloudlet cloudlet = new CloudletSimple(cloudletId, CLOUDLET_LENGTH, CLOUDLET_PES)
                    .setUtilizationModelCpu(createUtilizationModel(initialCpu, 0.9, dynamicUsage)).setSizes(1024);
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
}