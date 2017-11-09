package org.apache.ignite.internal.processors.cache;

import java.util.Collection;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.TopologyValidator;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lifecycle.LifecycleAware;
import org.apache.ignite.resources.IgniteInstanceResource;

public class SimpleAwareTopologyValidator implements TopologyValidator, LifecycleAware {
    /** */
    @IgniteInstanceResource
    private transient Ignite ignite;

    private static final String SPLIT_BRAIN_ATTR = "split-brain";

    @Override public boolean validate(Collection<ClusterNode> nodes) {
        IgniteKernal kernal = (IgniteKernal)ignite;

        ClusterNode crd = kernal.context().discovery().discoCache().oldestAliveServerNode();

        if (ignite.cluster().localNode().equals(crd)) {
            System.out.println("change coordinator");
            ((IgniteKernal)ignite).context().config().setUserAttributes(F.asMap(SPLIT_BRAIN_ATTR, System.currentTimeMillis()));
        }

        System.out.println("Coordinate attr: " + crd.attribute(SPLIT_BRAIN_ATTR));

        return true;
    }

    @Override public void start() throws IgniteException {

    }

    @Override public void stop() throws IgniteException {

    }
}
