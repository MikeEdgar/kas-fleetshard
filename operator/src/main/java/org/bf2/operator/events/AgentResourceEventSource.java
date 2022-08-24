package org.bf2.operator.events;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.client.informers.ResourceEventHandler;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import io.javaoperatorsdk.operator.processing.event.source.AbstractEventSource;
import io.javaoperatorsdk.operator.processing.event.source.controller.ResourceAction;
import io.javaoperatorsdk.operator.processing.event.source.controller.ResourceEvent;
import org.jboss.logging.Logger;

import javax.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class AgentResourceEventSource extends AbstractEventSource implements ResourceEventHandler<HasMetadata> {

    private static Logger log = Logger.getLogger(AgentResourceEventSource.class);

    @Override
    public void onAdd(HasMetadata resource) {
        log.infof("Add event received for %s %s/%s", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName());
        handleEvent(resource, ResourceAction.UPDATED);
    }

    @Override
    public void onUpdate(HasMetadata oldResource, HasMetadata newResource) {
        log.infof("Update event received for %s %s/%s", oldResource.getKind(), oldResource.getMetadata().getNamespace(), oldResource.getMetadata().getName());
        handleEvent(newResource, ResourceAction.UPDATED);
    }

    @Override
    public void onDelete(HasMetadata resource, boolean deletedFinalStateUnknown) {
        log.infof("Delete event received for %s %s/%s with deletedFinalStateUnknown %s", resource.getKind(),
                resource.getMetadata().getNamespace(), resource.getMetadata().getName(), deletedFinalStateUnknown);
        handleEvent(resource, ResourceAction.UPDATED);
    }

    protected void handleEvent(HasMetadata resource, ResourceAction action) {
        // the operator may not have inited yet
        if (getEventHandler() != null) {
            ResourceID.fromFirstOwnerReference(resource)
                .ifPresentOrElse(
                    ownerId ->
                        getEventHandler().handleEvent(new ResourceEvent(action, ownerId, null)),
                    () ->
                        log.warnf("%s %s/%s does not have OwnerReference", resource.getKind(), resource.getMetadata().getNamespace(), resource.getMetadata().getName()));
        }
    }
}
