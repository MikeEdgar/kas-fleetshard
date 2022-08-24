package org.bf2.operator.managers;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.informers.cache.Cache;
import io.javaoperatorsdk.operator.processing.event.source.EventSource;
import org.bf2.common.OperandUtils;
import org.bf2.common.ResourceInformer;
import org.bf2.common.ResourceInformerFactory;
import org.bf2.operator.events.AgentResourceEventSource;
import org.bf2.operator.resources.v1alpha1.ObservabilityConfiguration;
import org.jboss.logging.Logger;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

@ApplicationScoped
public class ObservabilityManager {
    static final String OBSERVABILITY_REPOSITORY = "repository";
    static final String OBSERVABILITY_CHANNEL = "channel";
    static final String OBSERVABILITY_ACCESS_TOKEN = "access_token";
    static final String OBSERVABILITY_TAG = "tag";
    public static final String OBSERVABILITY_SECRET_NAME = "fleetshard-observability";
    public static final String OBSERVABILITY_OPERATOR_STATUS = "observability-operator/status";
    public static final String ACCEPTED = "accepted";

    @Inject
    Logger log;

    @Inject
    KubernetesClient client;

    @Inject
    ResourceInformerFactory informerFactory;

    @Inject
    AgentResourceEventSource eventSource;

    ResourceInformer<Secret> observabilitySecretInformer;

    AtomicBoolean secretAccepted = new AtomicBoolean(false);

    static Base64.Encoder encoder = Base64.getEncoder();

    static void createObservabilitySecret(String namespace, ObservabilityConfiguration observability, SecretBuilder builder) {
        Map<String, String> data = new HashMap<>(2);
        data.put(OBSERVABILITY_ACCESS_TOKEN, encoder.encodeToString(observability.getAccessToken().getBytes(StandardCharsets.UTF_8)));
        data.put(OBSERVABILITY_REPOSITORY, encoder.encodeToString(observability.getRepository().getBytes(StandardCharsets.UTF_8)));
        if (observability.getChannel() != null) {
            data.put(OBSERVABILITY_CHANNEL, encoder.encodeToString(observability.getChannel().getBytes(StandardCharsets.UTF_8)));
        }
        if (observability.getTag() != null) {
            data.put(OBSERVABILITY_TAG, encoder.encodeToString(observability.getTag().getBytes(StandardCharsets.UTF_8)));
        }
        builder.editOrNewMetadata()
                   .withNamespace(namespace)
                   .withName(OBSERVABILITY_SECRET_NAME)
                   .addToLabels("configures", "observability-operator")
                   .addToLabels(OperandUtils.getDefaultLabels())
               .endMetadata()
               .addToData(data);
    }

    static boolean isObservabilityStatusAccepted(Secret cm) {
        Map<String, String> annotations = Objects.requireNonNullElse(cm.getMetadata().getAnnotations(), Collections.emptyMap());
        String status = annotations.get(OBSERVABILITY_OPERATOR_STATUS);
        return ACCEPTED.equalsIgnoreCase(status);
    }

    @PostConstruct
    void initialize() {
        observabilitySecretInformer = informerFactory.create(Secret.class, observabilitySecretResource(), eventSource);
    }

    public EventSource getEventSource() {
        return eventSource;
    }

    Resource<Secret> observabilitySecretResource() {
        return this.client.secrets().inNamespace(this.client.getNamespace()).withName(OBSERVABILITY_SECRET_NAME);
    }

    Secret cachedObservabilitySecret() {
        return observabilitySecretInformer.getByKey(Cache.namespaceKeyFunc(this.client.getNamespace(),
                ObservabilityManager.OBSERVABILITY_SECRET_NAME));
    }

    public void createOrUpdateObservabilitySecret(ObservabilityConfiguration observability, HasMetadata owner) {
        Secret cached = cachedObservabilitySecret();
        if (cached == null) {
            log.info("Creating Observability secret");
        }
        SecretBuilder builder = Optional.ofNullable(cached).map(s -> new SecretBuilder(s)).orElse(new SecretBuilder());
        createObservabilitySecret(this.client.getNamespace(), observability, builder);
        Secret secret = builder.build();
        OperandUtils.setAsOwner(owner, secret);
        OperandUtils.createOrUpdate(this.client.secrets(), secret);
    }

    public boolean isObservabilityRunning() {
        Secret secret = cachedObservabilitySecret();
        boolean accepted = secret != null && isObservabilityStatusAccepted(secret);

        if (!accepted) {
            secretAccepted.set(false);
            log.info("Observability secret not yet accepted");
        } else if (secretAccepted.compareAndSet(false, true)) {
            log.info("Observability secret has been accepted");
        }

        return accepted;
    }
}
