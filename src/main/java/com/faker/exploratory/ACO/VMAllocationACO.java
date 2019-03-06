package com.faker.exploratory.ACO;

// region Imports

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Triple;
import org.apache.commons.math3.distribution.EnumeratedIntegerDistribution;
import org.cloudbus.cloudsim.allocationpolicies.migration.VmAllocationPolicyMigrationAbstract;
import org.cloudbus.cloudsim.hosts.Host;
import org.cloudbus.cloudsim.selectionpolicies.power.PowerVmSelectionPolicyMinimumUtilization;
import org.cloudbus.cloudsim.vms.Vm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// endregion

public class VMAllocationACO extends VmAllocationPolicyMigrationAbstract {
    private static final Logger LOGGER = LoggerFactory.getLogger(VMAllocationACO.class.getSimpleName());

    /**
     * A map between a VM and the host where it is placed.
     */
    private final Map<Vm, Host> savedAllocation;

    private final int iterations = 2;
    private final int countAnts = 10;
    private final double exploitationParam = 0.9;
    private final double beta = 0.9;
    private final int gamma = 5;
    private final double rho = 0.1;
    private final double alpha = 0.1;
    private final Map<Triple<Host, Vm, Host>, Double> pheromoneMap;
    private final double initialPheromone = 1.0;

    public VMAllocationACO() {
        super(new PowerVmSelectionPolicyMinimumUtilization());
        this.setUnderUtilizationThreshold(0.5);
        this.pheromoneMap = new HashMap<>();
        this.savedAllocation = new HashMap<>();
    }

    @Override
    public Map<Vm, Host> getOptimizedAllocationMap(List<? extends Vm> vmList) {
        final Set<Host> overloadedHosts = getOverloadedHosts();
        LOGGER.debug("Overloads: {}", overloadedHosts);
        final Set<Host> underloadedHosts = getUnderloadedHosts();

        final Set<Host> targetHosts = getTargetHosts(overloadedHosts);
        final Set<Host> sourceHosts = getSourceHosts(underloadedHosts, overloadedHosts);
        LOGGER.debug("Targets: {}", targetHosts);
        LOGGER.debug("Sources: {}", sourceHosts);
        final Set<Triple<Host, Vm, Host>> finalTuples = this.getTuplesAndInitializePheromones(vmList, targetHosts)
                .stream().filter((tuple) -> sourceHosts.contains(tuple.getLeft())).collect(Collectors.toSet());

        saveAllocation();

        if (sourceHosts.isEmpty() || targetHosts.isEmpty() || vmList.isEmpty())
            return new HashMap<>();

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
                    if (!availableVms.contains(nextTriple.getMiddle())) {
                        optional = chooseNextTriple(availableTuples);
                        continue;
                    }
                    availableVms.remove(nextTriple.getMiddle());
                    updateLocalPheromone(nextTriple);
                    updateUsedCapacity(nextTriple);
                    localMigrationPlan.add(nextTriple);
                    double score = getRouteScore(localMigrationPlan);
                    if (score > localScore) {
                        localScore = score;
                    } else {
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
            }
            updateGlobalPheromones(globalMigrationPlan, globalScore);
        }

        restoreAllocation();
        return convertMigrationPlan(globalMigrationPlan);
    }

    private Map<Vm, Host> convertMigrationPlan(Set<Triple<Host, Vm, Host>> globalMigrationPlan) {
        Map<Vm, Host> resultMap = new HashMap<>();
        for (Triple<Host, Vm, Host> tuple : globalMigrationPlan)
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
        Map<Host, Integer> sourceMigrations = new HashMap<>();
        for (Triple<Host, Vm, Host> tuple : migrationPlan) {
            int initialVal = sourceMigrations.getOrDefault(tuple.getLeft(), 0);
            sourceMigrations.put(tuple.getLeft(), initialVal + 1);
        }
        int sleepingHostCount = 0;
        for (Host host : getHostList()) {
            if (sourceMigrations.get(host) == null)
                continue;
            if (host.getVmList().size() <= sourceMigrations.get(host))
                sleepingHostCount++;
        }
        return (Math.pow(sleepingHostCount, this.gamma)) + (1 / migrationPlan.size());
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
        if (q > exploitationParam) {
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
                if (source != target)
                    result.add(Triple.of(source, vm, target));
                this.pheromoneMap.put(Triple.of(source, vm, target), this.initialPheromone);
            }
        }
        return result;
    }

    private void updateUsedCapacity(Triple<Host, Vm, Host> tuple) {
        Vm vm = tuple.getMiddle();
        tuple.getLeft().destroyTemporaryVm(vm);
        tuple.getRight().createTemporaryVm(vm);
    }

    private void updateLocalPheromone(Triple<Host, Vm, Host> tuple) {
        double initialVal = this.pheromoneMap.get(tuple);
        double finalVal = (1 - this.rho) * initialVal + rho * this.initialPheromone;
        this.pheromoneMap.put(tuple, finalVal);
    }

    private double getTargetSelectionValue(Vm ant, Host target) {
        Host source = ant.getHost();
        double tau = pheromoneMap.get(Triple.of(source, ant, target));
        double n = getHostHeuristicValue(ant, target);
        return tau * Math.pow(n, beta);
    }

    private double getHostHeuristicValue(Vm ant, Host target) {
        double futureUsedCV = target.getUtilizationOfCpuMips() + ant.getCurrentRequestedTotalMips();
        double totalCV = target.getTotalMipsCapacity();
        return (futureUsedCV < totalCV) ? 1 / (totalCV - futureUsedCV) : 0;
    }

    private Set<Host> getSourceHosts(Set<Host> underloadedHosts, Set<Host> overloadedHosts) {
        Set<Host> sourceHosts = new HashSet<>();
        sourceHosts.addAll(underloadedHosts);
        sourceHosts.addAll(overloadedHosts);
        return sourceHosts;
    }

    private Set<Host> getTargetHosts(Set<Host> overloadedHosts) {
        Set<Host> targetHosts = new HashSet<>();
        targetHosts.addAll(this.getHostList());
        targetHosts.removeAll(overloadedHosts);
        return targetHosts;
    }

    private boolean _isHostUnderloadAndActive(final Host host) {
        return host.isActive() && host.getUtilizationOfCpu() < 0.2;
    }

    private boolean _isHostOverloaded(final Host host) {
        return host.getUtilizationOfCpu() > 0.5;
    }

    @Override
    public double getOverUtilizationThreshold(Host host) {
        return 0.9;
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
            host.reallocateMigratingInVms();
        }

        for (final Vm vm : savedAllocation.keySet()) {
            final Host host = savedAllocation.get(vm);
            if (!host.createTemporaryVm(vm)) {
                LOGGER.error("Couldn't restore {} on {}", vm, host);
                return;
            }
        }
    }
}