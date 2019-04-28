package com.faker.exploratory.ACO;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Triple;
import org.apache.commons.math3.distribution.EnumeratedIntegerDistribution;
import org.cloudbus.cloudsim.allocationpolicies.migration.VmAllocationPolicyMigrationAbstract;
import org.cloudbus.cloudsim.hosts.Host;
import org.cloudbus.cloudsim.selectionpolicies.VmSelectionPolicy;
import org.cloudbus.cloudsim.vms.Vm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VMAllocationACO extends VmAllocationPolicyMigrationAbstract {
    private static final Logger LOGGER = LoggerFactory.getLogger(VMAllocationACO.class.getSimpleName());

    /**
     * A map between a VM and the host where it is placed.
     */
    private final Map<Vm, Host> savedAllocation;

    private final Map<Host, Integer> savedHostVmCount;

    private final int iterations = 2;
    private final int countAnts = 3;
    private final double exploitationParam = 0.9;
    private final double beta = 0.9;
    private final int gamma = 5;
    private final double rho = 0.1;
    private final double alpha = 0.1;
    private final Map<Triple<Host, Vm, Host>, Double> pheromoneMap;
    private final double initialPheromone = 10.0;

    private final double overUtilizationThreshold;
    private final double underUtilizationThreshold;

    public long totalVmMigrations = 0;
    public long sleepingHosts = 0;

    public VMAllocationACO(VmSelectionPolicy vmSelectionPolicy, double overUtilizationThreshold,
            double underUtilizationThreshold) {
        super(vmSelectionPolicy);
        this.setUnderUtilizationThreshold(0.4);
        this.pheromoneMap = new HashMap<>();
        this.savedAllocation = new HashMap<>();
        this.savedHostVmCount = new HashMap<>();
        this.overUtilizationThreshold = overUtilizationThreshold;
        this.underUtilizationThreshold = underUtilizationThreshold;
    }

    @Override
    public Map<Vm, Host> getOptimizedAllocationMap(List<? extends Vm> vmList) {
        if (vmList.isEmpty())
            return Collections.EMPTY_MAP;

        LOGGER.info("{}: Starting to create migration plan...", getDatacenter().getSimulation().clock());
        final Set<Host> overloadedHosts = getOverloadedHosts();
        LOGGER.debug("Overloads: {}", overloadedHosts);
        final Set<Host> underloadedHosts = getUnderloadedHosts();
        LOGGER.debug("Underloads: {}", underloadedHosts);

        final Set<Host> targetHosts = getTargetHosts(overloadedHosts);
        final Set<Host> sourceHosts = getSourceHosts(underloadedHosts, overloadedHosts);
        // targetHosts.removeAll(sourceHosts);

        final Set<Triple<Host, Vm, Host>> finalTuples = this.getTuplesAndInitializePheromones(vmList, targetHosts)
                .stream().filter((tuple) -> sourceHosts.contains(tuple.getLeft())).collect(Collectors.toSet());

        if (sourceHosts.isEmpty() || targetHosts.isEmpty())
            return Collections.EMPTY_MAP;

        saveAllocation();
        saveHostVmCounts();

        LOGGER.debug("Targets: {}", targetHosts);
        LOGGER.debug("Sources: {}", sourceHosts);

        Set<Triple<Host, Vm, Host>> globalMigrationPlan = null;
        double globalScore = -1;
        for (int i = 0; i < iterations; i++) {
            LOGGER.info("{} iteration.", i);
            for (int ant = 0; ant < countAnts; ant++) {
                Set<Triple<Host, Vm, Host>> availableTuples = new HashSet<>();
                availableTuples.addAll(finalTuples);
                Set<Triple<Host, Vm, Host>> localMigrationPlan = new HashSet<>();
                double localScore = 0;
                Set<Vm> availableVms = new HashSet<>();
                availableVms.addAll(vmList);
                Optional<Triple<Host, Vm, Host>> optional = chooseNextTriple(availableTuples);
                Triple<Host, Vm, Host> nextTriple;
                while (optional.isPresent()) {
                    if (availableVms.isEmpty())
                        break;
                    nextTriple = optional.get();
                    availableTuples.remove(nextTriple);
                    boolean isValidMigration = isNeededMigration(nextTriple);// updateUsedCapacityAndVerify(nextTriple);
                    if (!availableVms.contains(nextTriple.getMiddle()) || !isValidMigration) {
                        // System.out.printf("Skipped %s.\n", nextTriple.getRight());
                        optional = chooseNextTriple(availableTuples);
                        continue;
                    }
                    updateLocalPheromone(nextTriple);
                    localMigrationPlan.add(nextTriple);
                    nextTriple.getLeft().addVmMigratingOut(nextTriple.getMiddle());
                    double score = getRouteScore(localMigrationPlan);
                    if (score > localScore) {
                        localScore = score;
                        Vm vmMigrated = nextTriple.getMiddle();
                        availableVms.remove(vmMigrated);
                        // LOGGER.debug("Tuple: {}", nextTriple);
                        // LOGGER.debug("Score update: {}", localScore);
                        availableTuples.removeIf(tuple -> tuple.getMiddle() == vmMigrated);
                    } else {
                        // LOGGER.info("Ant {} removing {}", ant, nextTriple);
                        localMigrationPlan.remove(nextTriple);
                    }
                    // LOGGER.info("Migration plan: {}", localMigrationPlan);
                    optional = chooseNextTriple(availableTuples);
                }
                LOGGER.info("Ant {}: score = {}", ant, localScore);
                if (globalScore < 0 || localScore > globalScore) {
                    globalMigrationPlan = localMigrationPlan;
                    globalScore = localScore;
                }
                // LOGGER.debug("Global plan = {}", globalMigrationPlan);
                LOGGER.info("Global score = {}", globalScore);
                restoreVmMigratingOut();
                // restoreAllocation();
            }
            updateGlobalPheromones(globalMigrationPlan, globalScore);
        }

        // printPheromoneMap();
        restoreVmMigratingOut();
        // restoreAllocation();
        calculateMigrationStats(globalMigrationPlan);
        return convertMigrationPlan(globalMigrationPlan);
    }

    private boolean isNeededMigration(Triple<Host, Vm, Host> nextTriple) {
        // Target is underloaded and has CPU requested less than source, then its in
        // sleep mode.
        // No need to wake it up now.
        Host source = nextTriple.getLeft();
        Host target = nextTriple.getRight();
        return !(target.getVmList().size() == 0 && !_isHostOverloaded(source));
        // return !(nextTriple.getRight().getUtilizationOfCpu() <
        // this.underUtilizationThreshold / 2
        // && getHostCpuPercentRequested(nextTriple.getRight()) <
        // getHostCpuPercentRequested(
        // nextTriple.getLeft()));
    }

    private void calculateMigrationStats(Set<Triple<Host, Vm, Host>> migrationPlan) {
        Set<Triple<Host, Vm, Host>> removedTuples = new HashSet<>();
        for (Triple<Host, Vm, Host> triple : migrationPlan) {
            if (updateUsedCapacityAndVerify(triple)) {
                System.out.printf("%s\n", triple);
                triple.getLeft().addVmMigratingOut(triple.getMiddle());
            } else
                removedTuples.add(triple);
        }
        migrationPlan.removeAll(removedTuples);
        if (!migrationPlan.isEmpty())
            LOGGER.info("{}: Found a Migration plan!", getDatacenter().getSimulation().clock());
        int sleepingHostCount = countSleepingHosts(migrationPlan, true);
        sleepingHosts += sleepingHostCount;
        totalVmMigrations += migrationPlan.size();
        System.out.printf("Sleeping hosts: %d\n", sleepingHostCount);
    }

    private Map<Vm, Host> convertMigrationPlan(Set<Triple<Host, Vm, Host>> migrationPlan) {
        Map<Vm, Host> resultMap = new HashMap<>();
        for (Triple<Host, Vm, Host> tuple : migrationPlan)
            resultMap.put(tuple.getMiddle(), tuple.getRight());
        return resultMap;
    }

    private void updateGlobalPheromones(Set<Triple<Host, Vm, Host>> migrationPlan, double routeScore) {
        for (Triple<Host, Vm, Host> tuple : this.pheromoneMap.keySet()) {
            double initialVal = this.pheromoneMap.get(tuple);
            double deltaPheromone = migrationPlan.contains(tuple) ? routeScore : 0;
            double finalVal = (1 - this.alpha) * initialVal + this.alpha * deltaPheromone;
            this.pheromoneMap.put(tuple, finalVal);
        }
    }

    private double getRouteScore(Set<Triple<Host, Vm, Host>> migrationPlan) {
        int sleepingHostCount = countSleepingHosts(migrationPlan, false);
        return (Math.pow(sleepingHostCount, this.gamma)) + (1.0 / migrationPlan.size());
    }

    private int countSleepingHosts(Set<Triple<Host, Vm, Host>> migrationPlan, boolean isFinalCalc) {
        int sleepingHostCount = 0;
        for (Host host : getHostList()) {
            long countTargets = migrationPlan.stream().filter(triple -> triple.getRight() == host).count();
            int savedCount = savedHostVmCount.get(host);
            if (countTargets == 0 && savedCount > 0 && savedCount == host.getVmsMigratingOut().size())
                sleepingHostCount++;
            // if (migrationPlan.isEmpty() && isFinalCalc) {
            // System.out.printf("%s ----- %d VMS with %f\n", host, host.getVmList().size(),
            // host.getUtilizationOfcCpu());
            // }
        }
        return sleepingHostCount;
    }

    private Optional<Triple<Host, Vm, Host>> chooseNextTriple(final Set<Triple<Host, Vm, Host>> availableTuples) {
        int lenTuple = availableTuples.size();
        if (lenTuple < 1)
            return Optional.empty();
        List<Triple<Host, Vm, Host>> tupleList = List.copyOf(availableTuples);
        int[] indices = new int[lenTuple];
        double[] probabs = new double[lenTuple];
        int maxIndex = -1;
        double totalWeight = 0;
        for (int i = 0; i < lenTuple; i++) {
            double weight = this.getTargetSelectionValue(tupleList.get(i).getMiddle(), tupleList.get(i).getRight());
            if (maxIndex < 0 || weight > probabs[maxIndex]) {
                maxIndex = i;
            }
            indices[i] = i;
            probabs[i] = weight;
            totalWeight += weight;
        }
        for (int i = 0; i < lenTuple; i++) {
            probabs[i] = probabs[i] / totalWeight;
        }
        double q = new Random().nextDouble();
        if (totalWeight > 0 && q > exploitationParam) {
            int randIndex = new EnumeratedIntegerDistribution(indices, probabs).sample();
            return Optional.of(tupleList.get(randIndex));
        } else {
            return Optional.of(tupleList.get(maxIndex));
        }
    }

    private Set<Triple<Host, Vm, Host>> getTuplesAndInitializePheromones(List<? extends Vm> vmList,
            Set<Host> targetHosts) {
        final Set<Triple<Host, Vm, Host>> result = new HashSet<>();
        for (Vm vm : vmList) {
            Host source = vm.getHost();
            for (Host target : targetHosts) {
                if (source != target) {
                    result.add(Triple.of(source, vm, target));
                    this.pheromoneMap.put(Triple.of(source, vm, target), this.initialPheromone);
                }
            }
        }
        return result;
    }

    private boolean updateUsedCapacityAndVerify(Triple<Host, Vm, Host> tuple) {
        Vm vm = tuple.getMiddle();
        if (tuple.getRight().createTemporaryVm(vm) && !_isHostOverloaded(tuple.getRight())) {
            tuple.getLeft().destroyTemporaryVm(vm);
            return true;
        }
        return false;
    }

    private void restoreVmMigratingOut() {
        for (Host host : this.getHostList()) {
            Set<Vm> vms = Set.copyOf(host.getVmsMigratingOut());
            for (Vm vm : vms) {
                host.removeVmMigratingOut(vm);
            }
        }
    }

    private void printPheromoneMap() {
        for (Map.Entry<Triple<Host, Vm, Host>, Double> entry : this.pheromoneMap.entrySet()) {
            System.out.printf("%s: %f\n", entry.getKey(), entry.getValue());
        }
    }

    private void updateLocalPheromone(Triple<Host, Vm, Host> tuple) {
        double initialVal = this.pheromoneMap.get(tuple);
        double finalVal = (1 - this.rho) * initialVal + rho * this.initialPheromone;

        Triple<Host, Vm, Host> reverseTuple = Triple.of(tuple.getRight(), tuple.getMiddle(), tuple.getLeft());
        this.pheromoneMap.put(tuple, finalVal);
        this.pheromoneMap.put(reverseTuple, -finalVal);
    }

    private double getTargetSelectionValue(Vm ant, Host target) {
        Host source = ant.getHost();
        double tau = pheromoneMap.get(Triple.of(source, ant, target));
        double n = getHostHeuristicValue(ant, target);
        return tau * Math.pow(n, beta);
    }

    private double getHostHeuristicValue(Vm vm, Host target) {
        double futureUsedCPU = target.getUtilizationOfCpuMips() + vm.getCurrentRequestedTotalMips();
        double totalCPU = target.getTotalMipsCapacity();
        double diffCPU = Math.max(totalCPU - futureUsedCPU, 0);

        long futureUsedStorage = target.getStorage().getAllocatedResource() + vm.getStorage().getAllocatedResource();
        long totalStorage = target.getStorage().getCapacity();
        long diffStorage = Math.max(totalStorage - futureUsedStorage, 0);

        double cost = Math.sqrt((diffCPU * diffCPU) + (diffStorage * diffStorage));
        return cost > 0 ? 1 / cost : 0;
    }

    private Set<Host> getSourceHosts(Set<Host> underloadedHosts, Set<Host> overloadedHosts) {
        final Set<Host> sourceHosts = new HashSet<>();
        sourceHosts.addAll(underloadedHosts);
        sourceHosts.addAll(overloadedHosts);
        return sourceHosts;
    }

    private Set<Host> getTargetHosts(Set<Host> overloadedHosts) {
        final Set<Host> targetHosts = new HashSet<>();
        targetHosts.addAll(this.getHostList());
        targetHosts.removeAll(overloadedHosts);
        targetHosts.removeIf(host -> !host.isActive());
        targetHosts.removeIf(host -> host.getAvailableMips() <= 0);
        return targetHosts;
    }

    private boolean _isHostUnderloadAndActive(final Host host) {
        // if (getHostCpuPercentAllocated(host) <= 0) {
        // host.setActive(false);
        // }
        return host.isActive() && host.getUtilizationOfCpu() < underUtilizationThreshold;
    }

    private boolean _isHostOverloaded(final Host host) {
        if (host.getUtilizationOfCpu() <= 0) {
            // host.setActive(true);
        }
        return host.getUtilizationOfCpu() > overUtilizationThreshold;
    }

    @Override
    public double getOverUtilizationThreshold(Host host) {
        return overUtilizationThreshold;
    }

    /**
     * Gets the List of overloaded hosts. If a Host is overloaded but it has VMs
     * migrating out, then it's not included in the returned List because the VMs to
     * be migrated to move the Host from the overload state already are in
     * migration.
     *
     * @return the over utilized hosts
     */
    private Set<Host> getOverloadedHosts() {
        return this.getHostList().stream().filter(this::_isHostOverloaded)
                .filter(host -> host.getVmsMigratingOut().isEmpty()).collect(Collectors.toSet());
    }

    /**
     * Gets the List of underloaded hosts. If a Host is underloaded but it has VMs
     * migrating out, then it's not included in the returned List because the VMs to
     * be migrated to move the Host from the underload state already are in
     * migration.
     *
     * @return the under utilized hosts
     */
    private Set<Host> getUnderloadedHosts() {
        return this.getHostList().stream().filter(this::_isHostUnderloadAndActive)
                .filter(host -> host.getVmsMigratingOut().isEmpty()).collect(Collectors.toSet());
    }

    /**
     * Checks if a host will be over utilized after placing of a candidate VM.
     *
     * @param host the host to verify
     * @param vm   the candidate vm
     * @return true, if the host will be over utilized after VM placement; false
     *         otherwise
     */
    private boolean isHostOverloadedAfterAllocation(final Host host, final Vm vm) {
        // if (!host.createTemporaryVm(vm)) {
        // return true;
        // }

        final double usagePercent = getHostCpuPercentRequested(host);
        final boolean overloadedAfterAllocation = usagePercent >= getOverUtilizationThreshold(host);
        // host.destroyTemporaryVm(vm);
        return overloadedAfterAllocation;
    }

    @Override
    protected Optional<Host> defaultFindHostForVm(final Vm vm) {
        final Set<Host> excludedHosts = new HashSet<>();
        excludedHosts.add(vm.getHost());
        return findHostForVm(vm, excludedHosts);
    }

    private Optional<Host> findHostForVm(final Vm vm, final Set<? extends Host> excludedHosts) {
        // final Comparator<Map.Entry<Host, Long>> hostPowerConsumptionComparator =
        // Comparator.comparingDouble(
        // (Map.Entry<Host, Long> entry) ->
        // getPowerDifferenceAfterAllocation(entry.getKey(), vm));

        final Map<Host, Long> map = getHostFreePesMap();
        /*
         * Since it's being used the min operation, the active comparator must be
         * reversed so that we get active hosts with minimum number of free PEs.
         */
        final Comparator<Map.Entry<Host, Long>> activeComparator = Comparator
                .comparing((Map.Entry<Host, Long> entry) -> entry.getKey().isActive()).reversed();
        final Comparator<Map.Entry<Host, Long>> comparator = activeComparator.thenComparingLong(Map.Entry::getValue);

        return map.entrySet().stream().filter(entry -> entry.getKey().isSuitableForVm(vm)).min(comparator)
                .map(Map.Entry::getKey);
    }

    private double getHostCpuPercentRequested(final Host host) {
        return getHostTotalRequestedMips(host) / host.getTotalMipsCapacity();
    }

    /**
     * Gets the total MIPS that is currently being used by all VMs inside the Host.
     * 
     * @param host
     * @return
     */
    private double getHostTotalRequestedMips(final Host host) {
        return host.getVmList().stream().mapToDouble(Vm::getCurrentRequestedTotalMips).sum();
    }

    /**
     * Saves the current map between a VM and the host where it is place.
     *
     * @see #savedAllocation
     */
    private void saveAllocation() {
        savedAllocation.clear();
        for (final Host host : getHostList()) {
            for (final Vm vm : host.getVmList()) {
                if (!host.getVmsMigratingIn().contains(vm)) {
                    savedAllocation.put(vm, host);
                }
            }
        }
    }

    /**
     * Restore VM allocation from the allocation history.
     *
     * @see #savedAllocation
     */
    private void restoreAllocation() {
        for (final Host host : getHostList()) {
            host.destroyAllVms();
            // host.reallocateMigratingInVms();
        }

        for (final Vm vm : savedAllocation.keySet()) {
            final Host host = savedAllocation.get(vm);
            if (!host.createTemporaryVm(vm)) {
                LOGGER.error("Couldn't restore {} on {}", vm, host);
                System.out.printf("Had VMS: %d, NOW: %d", savedHostVmCount.get(host), host.getVmList().size());
            }
        }
    }

    private void saveHostVmCounts() {
        for (Host host : getHostList()) {
            savedHostVmCount.put(host, host.getVmList().size());
        }
    }

}