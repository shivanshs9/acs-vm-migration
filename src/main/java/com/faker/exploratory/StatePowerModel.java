package com.faker.exploratory;

import org.cloudbus.cloudsim.power.models.PowerModelLinear;
import org.cloudbus.cloudsim.power.models.PowerModelSpecPowerHpProLiantMl110G3PentiumD930;

public class StatePowerModel extends PowerModelSpecPowerHpProLiantMl110G3PentiumD930 {
    // private boolean isIdleOff = false;

    // /**
    // * Instantiates a linear power model.
    // *
    // * @param maxPower the max power that can be consumed in Watt-Second
    // * (Ws).
    // * @param staticPowerPercent the static power usage percentage between 0 and
    // 1.
    // */
    // public StatePowerModel(final double maxPower, final double
    // staticPowerPercent) {
    // /**
    // * Calls the super constructor passing a {@link #powerFunction} that indicates
    // * the base power consumption is linear to CPU utilization.
    // */
    // super(maxPower, staticPowerPercent, utilization -> utilization);
    // }

    // public void setIdleOff(boolean value) {
    // isIdleOff = value;
    // }

    public double getPowerWithState(double utilization, boolean isActive) {
        return isActive ? super.getPowerInternal(utilization) : 0;
    }

    // @Override
    // protected double getPowerInternal(double utilization) throws
    // IllegalArgumentException {
    // return (this.isIdleOff && utilization == 0) ? 0 :
    // super.getPowerInternal(utilization);
    // }
}