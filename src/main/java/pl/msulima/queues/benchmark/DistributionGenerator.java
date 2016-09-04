package pl.msulima.queues.benchmark;

import com.google.common.base.MoreObjects;
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

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("value", value)
                .toString();
    }
}

class GammaDistributionGenerator implements DistributionGenerator {

    private final GammaDistribution distribution;
    public static final int MULTIPLIER = 1;
    private final int shape;
    private final int scale;

    GammaDistributionGenerator(int shape, int scale) {
        this.shape = shape;
        this.scale = scale;
        distribution = new GammaDistribution(shape, scale);
        System.out.println(distribution.getNumericalMean() * MULTIPLIER);
        System.out.println("****");
    }

    @Override
    public long next() {
        return (long) (distribution.sample() * MULTIPLIER);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("shape", shape)
                .add("scale", scale)
                .toString();
    }
}
