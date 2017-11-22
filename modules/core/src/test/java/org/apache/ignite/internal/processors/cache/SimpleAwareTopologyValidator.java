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

    @Override public boolean validate(Collection<ClusterNode> nodes) {
        IgniteKernal kernal = (IgniteKernal)ignite;

        ClusterNode crd = kernal.context().discovery().discoCache().oldestAliveServerNode();

        System.out.println("++++++++++ " + ignite.cluster().localNode().id()+ " coord " + crd.id() + " time " + System.currentTimeMillis());

        long topologyVersion = kernal.cluster().topologyVersion();

        long topologyVersion2 = kernal.context().discovery().topologyVersion();

        System.out.println("==== 1: " + topologyVersion + "  " + "2: " + topologyVersion2 + " n1: " + nodes.size() + " n2: " + kernal.cluster().nodes().size());

        return true;
    }

    @Override public void start() throws IgniteException {

    }

    @Override public void stop() throws IgniteException {

    }
}
