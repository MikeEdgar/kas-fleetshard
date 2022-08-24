package org.bf2.operator.controllers;

import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ControllerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceInitializer;
import io.javaoperatorsdk.operator.api.reconciler.Reconciler;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import io.javaoperatorsdk.operator.processing.event.source.EventSource;
import io.micrometer.core.annotation.Counted;
import io.micrometer.core.annotation.Timed;
import org.bf2.common.ConditionUtils;
import org.bf2.operator.events.AgentResourceEventSource;
import org.bf2.operator.events.ControllerEventFilter;
import org.bf2.operator.managers.CapacityManager;
import org.bf2.operator.managers.InformerManager;
import org.bf2.operator.managers.ObservabilityManager;
import org.bf2.operator.managers.StrimziManager;
import org.bf2.operator.resources.v1alpha1.ManagedKafkaAgent;
import org.bf2.operator.resources.v1alpha1.ManagedKafkaAgentStatus;
import org.bf2.operator.resources.v1alpha1.ManagedKafkaAgentStatusBuilder;
import org.bf2.operator.resources.v1alpha1.ManagedKafkaCondition;
import org.bf2.operator.resources.v1alpha1.ManagedKafkaCondition.Status;
import org.bf2.operator.resources.v1alpha1.ManagedKafkaCondition.Type;
import org.bf2.operator.resources.v1alpha1.ProfileCapacity;
import org.bf2.operator.resources.v1alpha1.StrimziVersionStatus;
import org.jboss.logging.Logger;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The controller for {@link ManagedKafkaAgent}.  However there is currently
 * nothing for this to control.  There is just a scheduled job to update the status
 * which will create the singleton resource if needed.
 *
 * An alternative to this approach would be to have the ManagedKafkaControl make status
 * updates directly based upon the changes it sees in the ManagedKafka instances.
 */
@ApplicationScoped
@ControllerConfiguration(
        generationAwareEventProcessing = false,
        onUpdateFilter = ControllerEventFilter.class)
public class ManagedKafkaAgentController implements Reconciler<ManagedKafkaAgent>, EventSourceInitializer<ManagedKafkaAgent> {

    @Inject
    Logger log;

    @Inject
    AgentResourceEventSource eventSource;

    @Inject
    ObservabilityManager observabilityManager;

    @Inject
    StrimziManager strimziManager;

    @Inject
    CapacityManager capacityManager;

    @Inject
    InformerManager informerManager;

    @Override
    public Map<String, EventSource> prepareEventSources(EventSourceContext<ManagedKafkaAgent> context) {
        return Map.of("agentEvents", eventSource);
    }

    @Timed(value = "controller.update", extraTags = {"resource", "ManagedKafkaAgent"}, description = "Time spent processing createOrUpdate calls")
    @Counted(value = "controller.update", extraTags = {"resource", "ManagedKafkaAgent"}, description = "The number of createOrUpdate calls processed")
    @Override
    public UpdateControl<ManagedKafkaAgent> reconcile(ManagedKafkaAgent resource, Context<ManagedKafkaAgent> context) {
        capacityManager.getOrCreateResourceConfigMap(resource);
        observabilityManager.createOrUpdateObservabilitySecret(resource.getSpec().getObservability(), resource);
        // since we don't know the prior state, we have to just reconcile everything
        // in case the spec profile information has changed
        informerManager.resyncManagedKafka();
        boolean statusModified = updateStatus(resource);

        if (statusModified) {
            return UpdateControl.updateStatus(resource);
        }

        return UpdateControl.noUpdate();
    }

    private boolean updateStatus(ManagedKafkaAgent resource) {
        ManagedKafkaAgentStatus originalStatus = null;

        if (resource.getStatus() != null) {
            originalStatus = new ManagedKafkaAgentStatusBuilder(resource.getStatus()).build();
        }

        ManagedKafkaAgentStatus status = Objects.requireNonNullElse(resource.getStatus(),
                new ManagedKafkaAgentStatusBuilder().build());

        ManagedKafkaCondition readyCondition = ConditionUtils.findManagedKafkaCondition(status.getConditions(), Type.Ready)
                .orElseGet(() -> ConditionUtils.buildCondition(Type.Ready, Status.Unknown));

        List<StrimziVersionStatus> strimziVersions = this.strimziManager.getStrimziVersions();
        log.debugf("Strimzi versions %s", strimziVersions);

        // consider the fleetshard operator ready when observability is running and a Strimzi bundle is installed (aka at least one available version)
        Status statusValue = this.observabilityManager.isObservabilityRunning() && !strimziVersions.isEmpty() ?
                ManagedKafkaCondition.Status.True : ManagedKafkaCondition.Status.False;
        ConditionUtils.updateConditionStatus(readyCondition, statusValue, null, null);
        if (!this.observabilityManager.isObservabilityRunning()) {
            ConditionUtils.updateConditionStatus(readyCondition, statusValue,null, "Observability secret not yet accepted");
        }

        Map<String, ProfileCapacity> capacity = capacityManager.buildCapacity(resource);

        status.setConditions(Arrays.asList(readyCondition));
        status.setStrimzi(strimziVersions);
        status.setCapacity(capacity);

        if (!Objects.equals(originalStatus, status)) {
            status.setUpdatedTimestamp(ConditionUtils.iso8601Now());
            resource.setStatus(status);
            return true;
        }

        return false;
    }

}
