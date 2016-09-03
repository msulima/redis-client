package pl.msulima.queues.benchmark;

import org.apache.commons.math3.distribution.GammaDistribution;

interface DistributionGenerator {

    long next();
}

class ConstantDistributionGenerator implements DistributionGenerator {

    private final long value;

    ConstantDistributionGenerator(long value) {
        this.value = value;
    }

    @Override
    public long next() {
        return value;
    }
}

class GammaDistributionGenerator implements DistributionGenerator {

    private final GammaDistribution distribution;
    public static final int MULTIPLIER = 1;

    GammaDistributionGenerator(int shape, int scale) {
        distribution = new GammaDistribution(shape, scale);
        System.out.println(distribution.getNumericalMean() * MULTIPLIER);
        System.out.println("****");
    }

    @Override
    public long next() {
        return (long) (distribution.sample() * MULTIPLIER);
    }
}
