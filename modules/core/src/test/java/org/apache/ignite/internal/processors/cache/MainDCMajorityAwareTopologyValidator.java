package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.TopologyValidator;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lifecycle.LifecycleAware;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;

import java.util.Collection;

/**
 * Allows cache operations only if current topology contains the majority of nodes from main DC and there is no lost
 * partitions
 */
class MainDCMajorityAwareTopologyValidator implements TopologyValidator, LifecycleAware {
    /** */
    static final String DC_NODE_ATTR = "dc";

    /** */
    static final String ACTIVATOR_NODE_ATTR = "seg.activator";

    /** */
    private static final long serialVersionUID = 0L;

    /** */
    @IgniteInstanceResource
    private transient Ignite ignite;

    /** */
    @LoggerResource
    private transient IgniteLogger log;

    /** */
    private volatile int mainDCNodeCnt;

    /** */
    private volatile String mainDCAttrValue = "megaCOD";

    /**
     * {@inheritDoc}
     */
    @Override public boolean validate(Collection<ClusterNode> nodes) {
        int curMainDCNodeCnt = F.view(nodes, new IgnitePredicate<ClusterNode>() {
            @Override
            public boolean apply(ClusterNode node) {
                return !node.isClient() && node.attribute(DC_NODE_ATTR).equals(mainDCAttrValue);
            }
        }).size();

        boolean hasMajority = false;
        boolean partitionLost = false;

        //Check if we have majority in main DC
        if (curMainDCNodeCnt > mainDCNodeCnt / 2)
            hasMajority = true;

        //Check if we lost any partitions
        IgniteKernal kernal = (IgniteKernal)ignite;

        for (CacheGroupContext grp : kernal.context().cache().cacheGroups()) {
            if (grp.isLocal())
                continue;

            if (!grp.topology().lostPartitions().isEmpty()) {
                partitionLost = true;
                break;
            }
        }

        if (!hasMajority || partitionLost) {
            boolean resolved = F.view(nodes, new IgnitePredicate<ClusterNode>() {
                @Override
                public boolean apply(ClusterNode node) {
                    return isMarkerNode(node);
                }
            }).size() > 0;

            if (!resolved)
                log.info("Grid segmentation is detected, switching to inoperative state.");

            return resolved;
        }
        else
            return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override public void start() throws IgniteException {
        String mainDCAttrValue = System.getProperty("maindc.attrvalue");
        if (mainDCAttrValue != null && mainDCAttrValue.trim().length() > 0)
            this.mainDCAttrValue = mainDCAttrValue;

        try {
            mainDCNodeCnt = Integer.parseInt(System.getProperty("maindc.totalnodescount"));
        }
        catch (NumberFormatException ex) {

        }
    }

    /**
     * @param node Node.
     * @return {@code True} if this is marker node.
     */
    private boolean isMarkerNode(ClusterNode node) {
        return node.isClient() && node.attribute(ACTIVATOR_NODE_ATTR) != null;
    }

    /**
     * {@inheritDoc}
     */
    @Override public void stop() {
        // No-op.
    }
}