package org.bf2.operator.operands;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.networking.v1.NetworkPolicyPeerBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.openshift.api.model.Route;
import io.fabric8.openshift.client.OpenShiftClient;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.strimzi.api.kafka.model.CertAndKeySecretSource;
import io.strimzi.api.kafka.model.CertAndKeySecretSourceBuilder;
import io.strimzi.api.kafka.model.CertSecretSource;
import io.strimzi.api.kafka.model.CertSecretSourceBuilder;
import io.strimzi.api.kafka.model.GenericSecretSource;
import io.strimzi.api.kafka.model.GenericSecretSourceBuilder;
import io.strimzi.api.kafka.model.Kafka;
import io.strimzi.api.kafka.model.KafkaBuilder;
import io.strimzi.api.kafka.model.KafkaFluent.MetadataNested;
import io.strimzi.api.kafka.model.KafkaResources;
import io.strimzi.api.kafka.model.StrimziPodSet;
import io.strimzi.api.kafka.model.listener.KafkaListenerAuthentication;
import io.strimzi.api.kafka.model.listener.KafkaListenerAuthenticationOAuthBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListener;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerConfigurationBootstrapBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerConfigurationBroker;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerConfigurationBrokerBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.GenericKafkaListenerConfigurationBuilder;
import io.strimzi.api.kafka.model.listener.arraylistener.KafkaListenerType;
import io.strimzi.api.kafka.model.status.Condition;
import io.strimzi.api.kafka.model.status.KafkaStatus;
import org.bf2.common.OperandUtils;
import org.bf2.operator.ManagedKafkaKeys.Annotations;
import org.bf2.operator.clients.KafkaResourceClient;
import org.bf2.operator.managers.InformerManager;
import org.bf2.operator.managers.OperandOverrideManager;
import org.bf2.operator.managers.SecuritySecretManager;
import org.bf2.operator.managers.StrimziManager;
import org.bf2.operator.resources.v1alpha1.ManagedKafka;
import org.bf2.operator.resources.v1alpha1.ManagedKafkaAuthenticationOAuth;
import org.bf2.operator.resources.v1alpha1.ManagedKafkaCondition.Reason;
import org.bf2.operator.resources.v1alpha1.ManagedKafkaCondition.Status;
import org.jboss.logging.Logger;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

public abstract class AbstractKafkaCluster implements Operand<ManagedKafka> {

    public static final String EXTERNAL_LISTENER_NAME = "external";

    @Inject
    Logger log;

    @Inject
    protected KafkaResourceClient kafkaResourceClient;

    @Inject
    protected KubernetesClient kubernetesClient;

    @Inject
    protected InformerManager informerManager;

    @Inject
    protected SecuritySecretManager secretManager;

    @Inject
    protected KafkaInstanceConfigurations configs;

    @Inject
    protected OperandOverrideManager overrideManager;

    public static String kafkaClusterName(ManagedKafka managedKafka) {
        return managedKafka.getMetadata().getName();
    }

    public static String kafkaClusterNamespace(ManagedKafka managedKafka) {
        return managedKafka.getMetadata().getNamespace();
    }

    public boolean isStrimziUpdating(ManagedKafka managedKafka) {
        Kafka kafka = cachedKafka(managedKafka);
        if (kafka == null) {
            return false;
        }
        Map<String, String> annotations = Objects.requireNonNullElse(kafka.getMetadata().getAnnotations(), Collections.emptyMap());
        return StrimziManager.isPauseReasonStrimziUpdate(annotations) && isConditionReconciliationPausedTrue(cachedKafka(managedKafka));
    }

    public boolean isKafkaUpdating(ManagedKafka managedKafka) {
        if (managedKafka.isSuspended() && !isKafkaReady(managedKafka)) {
            return isKafkaUpgradeIncomplete(managedKafka);
        }
        return this.isKafkaAnnotationUpdating(managedKafka, "strimzi.io/kafka-version", kafka -> kafka.getSpec().getKafka().getVersion());
    }

    public boolean isKafkaIbpUpdating(ManagedKafka managedKafka) {
        return this.isKafkaAnnotationUpdating(managedKafka, "strimzi.io/inter-broker-protocol-version", kafka -> {
            Object interBrokerProtocol = kafka.getSpec().getKafka().getConfig().get("inter.broker.protocol.version");
            return interBrokerProtocol != null ? interBrokerProtocol.toString() : AbstractKafkaCluster.getKafkaIbpVersion(kafka.getSpec().getKafka().getVersion());
        });
    }

    private boolean isKafkaAnnotationUpdating(ManagedKafka managedKafka, String annotation, Function<Kafka, String> valueSupplier) {
        Kafka kafka = cachedKafka(managedKafka);
        if (kafka == null) {
            return false;
        }
        List<Pod> kafkaPods = kubernetesClient.pods()
                .inNamespace(kafka.getMetadata().getNamespace())
                .withLabel("strimzi.io/name", kafka.getMetadata().getName() + "-kafka")
                .list()
                .getItems();
        boolean isKafkaAnnotationUpdating = false;
        String expectedValue = valueSupplier.apply(kafka);
        for (Pod kafkaPod : kafkaPods) {
            String annotationValueOnPod = Optional.ofNullable(kafkaPod.getMetadata().getAnnotations())
                    .map(annotations -> annotations.get(annotation))
                    .orElse(null);
            if (annotationValueOnPod == null) {
                log.errorf("Kafka pod [%s] is missing annotation '%s'", kafkaPod.getMetadata().getName(), annotation);
                throw new RuntimeException();
            }
            log.tracef("Kafka pod [%s] annotation '%s' = %s [expected value %s]", kafkaPod.getMetadata().getName(), annotation, annotationValueOnPod, expectedValue);
            isKafkaAnnotationUpdating |= !annotationValueOnPod.equals(expectedValue);
            if (isKafkaAnnotationUpdating) {
                break;
            }
        }
        return isKafkaAnnotationUpdating;
    }

    public boolean isKafkaUpgradeStabilityChecking(ManagedKafka managedKafka) {
        Kafka kafka = cachedKafka(managedKafka);
        Optional<String> kafkaUpgradeStartTimestampAnnotation = OperandUtils.getAnnotation(kafka, Annotations.KAFKA_UPGRADE_START_TIMESTAMP);
        Optional<String> kafkaUpgradeEndTimestampAnnotation = OperandUtils.getAnnotation(kafka, Annotations.KAFKA_UPGRADE_END_TIMESTAMP);

        return kafkaUpgradeStartTimestampAnnotation.isPresent() && kafkaUpgradeEndTimestampAnnotation.isPresent();
    }

    boolean isKafkaReady(ManagedKafka managedKafka) {
        return hasKafkaCondition(managedKafka, c -> "Ready".equals(c.getType()) && "True".equals(c.getStatus()));
    }

    boolean isKafkaUpgradeIncomplete(ManagedKafka managedKafka) {
        Kafka kafka = cachedKafka(managedKafka);
        return OperandUtils.getAnnotation(kafka, Annotations.KAFKA_UPGRADE_START_TIMESTAMP).isPresent()
                && OperandUtils.getAnnotation(kafka, Annotations.KAFKA_UPGRADE_END_TIMESTAMP).isEmpty();
    }

    public boolean isReadyNotUpdating(ManagedKafka managedKafka) {
        OperandReadiness readiness = getReadiness(managedKafka);
        return Status.True.equals(readiness.getStatus()) && !Reason.StrimziUpdating.equals(readiness.getReason());
    }

    @Override
    public OperandReadiness getReadiness(ManagedKafka managedKafka) {
        Kafka kafka = cachedKafka(managedKafka);

        if (kafka == null) {
            return new OperandReadiness(Status.False, Reason.Installing, String.format("Kafka %s does not exist", kafkaClusterName(managedKafka)));
        }

        Optional<Condition> notReady = kafkaCondition(kafka, c -> "NotReady".equals(c.getType()));

        if (notReady.filter(c -> "True".equals(c.getStatus())).isPresent()) {
            Condition c = notReady.get();
            return new OperandReadiness(Status.False, "Creating".equals(c.getReason()) ? Reason.Installing : Reason.Error, c.getMessage());
        }

        if (isStrimziUpdating(managedKafka)) {
            // the status here is actually unknown
            return new OperandReadiness(Status.True, Reason.StrimziUpdating, "Updating Strimzi version");
        }

        if (isKafkaUpdating(managedKafka) || isKafkaUpgradeStabilityChecking(managedKafka)) {
            return new OperandReadiness(Status.True, Reason.KafkaUpdating, "Updating Kafka version");
        }

        if (isKafkaIbpUpdating(managedKafka)) {
            return new OperandReadiness(Status.True, Reason.KafkaIbpUpdating, "Updating Kafka IBP version");
        }

        Optional<Condition> ready = kafkaCondition(kafka, c -> "Ready".equals(c.getType()));

        if (ready.filter(c -> "True".equals(c.getStatus())).isPresent()) {
            return new OperandReadiness(Status.True, null, null);
        }

        if (isReconciliationPaused(managedKafka)) {
            if (managedKafka.isSuspended()) {
                return new OperandReadiness(Status.False, Reason.Suspended, null);
            }
            // strimzi may in the future report the status even when paused, but for now we don't know
            return new OperandReadiness(Status.Unknown, Reason.Paused, String.format("Kafka %s is paused for an unknown reason", kafkaClusterName(managedKafka)));
        }

        return new OperandReadiness(Status.False, Reason.Installing, String.format("Kafka %s is not providing status", kafkaClusterName(managedKafka)));
    }

    public boolean isReconciliationPaused(ManagedKafka managedKafka) {
        Kafka kafka = cachedKafka(managedKafka);
        boolean isReconciliationPaused = kafka != null
                && hasAnnotationValue(kafka, StrimziManager.STRIMZI_PAUSE_RECONCILE_ANNOTATION, "true")
                && isConditionReconciliationPausedTrue(kafka);
        log.tracef("KafkaCluster isReconciliationPaused = %s", isReconciliationPaused);
        return isReconciliationPaused;
    }

    boolean isConditionReconciliationPausedTrue(Kafka kafka) {
        boolean isReconciliationPaused = kafka != null
                && kafka.getStatus() != null
                && hasKafkaCondition(kafka, c -> c.getType() != null && "ReconciliationPaused".equals(c.getType())
                && "True".equals(c.getStatus()));
        log.tracef("KafkaCluster isConditionReconciliationPausedTrue = %s", isReconciliationPaused);
        return isReconciliationPaused;
    }

    @Override
    public boolean isDeleted(ManagedKafka managedKafka) {
        boolean isDeleted = cachedKafka(managedKafka) == null;
        log.tracef("KafkaCluster isDeleted = %s", isDeleted);
        return isDeleted;
    }

    protected boolean hasAnnotationValue(Kafka kafka, String annotationName, String value) {
        return Optional.ofNullable(kafka.getMetadata().getAnnotations())
                .map(annotations -> annotations.get(annotationName))
                .map(value::equals)
                .orElse(false);
    }

    protected boolean hasKafkaCondition(ManagedKafka managedKafka, Predicate<Condition> predicate) {
        Kafka kafka = cachedKafka(managedKafka);
        return kafka != null && hasKafkaCondition(kafka, predicate);
    }

    protected boolean hasKafkaCondition(Kafka kafka, Predicate<Condition> predicate) {
        return kafkaCondition(kafka, predicate).isPresent();
    }

    protected Optional<Condition> kafkaCondition(Kafka kafka, Predicate<Condition> predicate) {
        return Optional.ofNullable(kafka.getStatus()).map(KafkaStatus::getConditions).flatMap(l -> l.stream().filter(predicate).findFirst());
    }

    protected Kafka cachedKafka(ManagedKafka managedKafka) {
        return informerManager.getLocalKafka(kafkaClusterNamespace(managedKafka), kafkaClusterName(managedKafka));
    }

    public boolean hasKafkaBeenReady(ManagedKafka managedKafka) {
        Kafka kafka = cachedKafka(managedKafka);
        // if we don't yet have a clusterId, then we've never been ready
        return hasClusterId(kafka);
    }

    static boolean hasClusterId(Kafka kafka) {
        return kafka != null && kafka.getStatus() != null && kafka.getStatus().getClusterId() != null;
    }

    @Override
    public void createOrUpdate(ManagedKafka managedKafka) {
        Kafka current = cachedKafka(managedKafka);

        MetadataNested<KafkaBuilder> editMetadata = moveAnnotation(managedKafka, Annotations.KAFKA_UPGRADE_START_TIMESTAMP, current, null);
        editMetadata = moveAnnotation(managedKafka, Annotations.KAFKA_UPGRADE_END_TIMESTAMP, current, editMetadata);

        if (editMetadata != null) {
            createOrUpdate(editMetadata.endMetadata().build());
            return; // just move the annotations - don't do any further reasoning.  We'll wait for the kafka event.
        }

        Kafka kafka = kafkaFrom(managedKafka, current);
        createOrUpdate(kafka);

        boolean reconciliationPaused = isReconciliationPaused(managedKafka);

        if (managedKafka.isSuspended() && reconciliationPaused && !updatesInProgress(managedKafka)) {
            suspend(managedKafka);
        }
    }

    private MetadataNested<KafkaBuilder> moveAnnotation(ManagedKafka managedKafka, String annotation,
            Kafka current, MetadataNested<KafkaBuilder> editMetadata) {
        if (managedKafka.getAnnotation(annotation).isPresent()) {
            String value = managedKafka.getMetadata().getAnnotations().remove(annotation);
            if (current != null) {
                if (editMetadata == null) {
                    editMetadata = new KafkaBuilder(current).editMetadata();
                }
                editMetadata.addToAnnotations(annotation, value);
            }
        }
        return editMetadata;
    }

    public abstract Kafka kafkaFrom(ManagedKafka managedKafka, Kafka current);

    @Override
    public void delete(ManagedKafka managedKafka, Context context) {
        kafkaResourceClient.delete(kafkaClusterNamespace(managedKafka), kafkaClusterName(managedKafka));
    }

    protected void createOrUpdate(Kafka kafka) {
        kafkaResourceClient.createOrUpdate(kafka);
    }

    protected void suspend(ManagedKafka managedKafka) {
        String namespace = kafkaClusterNamespace(managedKafka);
        String instanceName = kafkaClusterName(managedKafka);

        kubernetesClient.apps()
            .deployments()
            .inNamespace(namespace)
            .withName(instanceName + "-kafka-exporter")
            .delete();

        // Remove when migration to StrimziPodSets complete
        kubernetesClient.apps()
            .statefulSets()
            .inNamespace(namespace)
            .withName(KafkaResources.kafkaStatefulSetName(instanceName))
            .delete();

        kubernetesClient.resources(StrimziPodSet.class)
            .inNamespace(namespace)
            .withName(KafkaResources.kafkaStatefulSetName(instanceName))
            .delete();

        // Remove when migration to StrimziPodSets complete
        kubernetesClient.apps()
            .statefulSets()
            .inNamespace(namespace)
            .withName(KafkaResources.zookeeperStatefulSetName(instanceName))
            .delete();

        kubernetesClient.resources(StrimziPodSet.class)
            .inNamespace(namespace)
            .withName(KafkaResources.zookeeperStatefulSetName(instanceName))
            .delete();

        Map<String, String> rateLimitAnnotations = OperandUtils.buildRateLimitAnnotations(5, 5);
        Supplier<Map<String, String>> routeAnnotationFactory = () -> new HashMap<>(rateLimitAnnotations.size());

        informerManager.getRoutesInNamespace(namespace)
            .filter(r -> r.getMetadata().getName().startsWith(instanceName + "-kafka-"))
            .forEach(r -> {
                Map<String, String> routeAnnotations = Objects.requireNonNullElseGet(r.getMetadata().getAnnotations(), routeAnnotationFactory);
                routeAnnotations.putAll(rateLimitAnnotations);
                r.getMetadata().setAnnotations(routeAnnotations);
                OperandUtils.createOrUpdate(kubernetesClient.resources(Route.class), r);
                ensureBlueprintRouteForSuspendedRoute(r);
            });
    }

    protected void ensureBlueprintRouteForSuspendedRoute(Route r) {
    }

    protected List<GenericKafkaListener> buildListeners(ManagedKafka managedKafka, int replicas) {

        KafkaListenerAuthentication plainOverOauthAuthenticationListener = null;
        KafkaListenerAuthentication oauthAuthenticationListener = null;

        if (SecuritySecretManager.isKafkaAuthenticationEnabled(managedKafka)) {
            ManagedKafkaAuthenticationOAuth managedKafkaAuthenticationOAuth = managedKafka.getSpec().getOauth();

            CertSecretSource ssoTlsCertSecretSource = buildSsoTlsCertSecretSource(managedKafka);

            KafkaListenerAuthenticationOAuthBuilder plainOverOauthAuthenticationListenerBuilder = new KafkaListenerAuthenticationOAuthBuilder()
                    .withClientId(getOAuthClientId(managedKafka))
                    .withJwksEndpointUri(managedKafkaAuthenticationOAuth.getJwksEndpointURI())
                    .withUserNameClaim(managedKafkaAuthenticationOAuth.getUserNameClaim())
                    .withFallbackUserNameClaim(managedKafkaAuthenticationOAuth.getFallbackUserNameClaim())
                    .withCustomClaimCheck(managedKafkaAuthenticationOAuth.getCustomClaimCheck())
                    .withValidIssuerUri(managedKafkaAuthenticationOAuth.getValidIssuerEndpointURI())
                    .withClientSecret(buildSsoClientGenericSecretSource(managedKafka))
                    .withEnablePlain(true)
                    .withTokenEndpointUri(managedKafkaAuthenticationOAuth.getTokenEndpointURI());

            if (ssoTlsCertSecretSource != null) {
                plainOverOauthAuthenticationListenerBuilder.withTlsTrustedCertificates(ssoTlsCertSecretSource);
            }
            plainOverOauthAuthenticationListener = plainOverOauthAuthenticationListenerBuilder.build();

            KafkaListenerAuthenticationOAuthBuilder oauthAuthenticationListenerBuilder = new KafkaListenerAuthenticationOAuthBuilder()
                    .withClientId(getOAuthClientId(managedKafka))
                    .withJwksEndpointUri(managedKafkaAuthenticationOAuth.getJwksEndpointURI())
                    .withUserNameClaim(managedKafkaAuthenticationOAuth.getUserNameClaim())
                    .withFallbackUserNameClaim(managedKafkaAuthenticationOAuth.getFallbackUserNameClaim())
                    .withCustomClaimCheck(managedKafkaAuthenticationOAuth.getCustomClaimCheck())
                    .withValidIssuerUri(managedKafkaAuthenticationOAuth.getValidIssuerEndpointURI())
                    .withClientSecret(buildSsoClientGenericSecretSource(managedKafka));

            if (ssoTlsCertSecretSource != null) {
                oauthAuthenticationListenerBuilder.withTlsTrustedCertificates(ssoTlsCertSecretSource);
            }
            oauthAuthenticationListener = oauthAuthenticationListenerBuilder.build();
        }

        KafkaListenerType externalListenerType = kubernetesClient.isAdaptable(OpenShiftClient.class) ? KafkaListenerType.ROUTE : KafkaListenerType.INGRESS;

        // Limit client connections per listener
        KafkaInstanceConfiguration config = this.configs.getConfig(managedKafka);
        Integer totalMaxConnections = Objects.requireNonNullElse(managedKafka.getSpec().getCapacity().getTotalMaxConnections(), config.getKafka().getMaxConnections()) / replicas;
        // Limit connection attempts per listener
        Integer maxConnectionAttemptsPerSec = Objects.requireNonNullElse(managedKafka.getSpec().getCapacity().getMaxConnectionAttemptsPerSec(), config.getKafka().getConnectionAttemptsPerSec()) / replicas;

        GenericKafkaListenerConfigurationBuilder listenerConfigBuilder = new GenericKafkaListenerConfigurationBuilder()
                .withBootstrap(new GenericKafkaListenerConfigurationBootstrapBuilder()
                        .withHost(managedKafka.getSpec().getEndpoint().getBootstrapServerHost())
                        .withAnnotations(buildExternalListenerAnnotations(managedKafka))
                        .build()
                )
                .withBrokers(buildBrokerOverrides(managedKafka, replicas))
                .withBrokerCertChainAndKey(buildTlsCertAndKeySecretSource(managedKafka))
                .withMaxConnections(totalMaxConnections)
                .withMaxConnectionCreationRate(maxConnectionAttemptsPerSec);

        return Arrays.asList(
                        new GenericKafkaListenerBuilder()
                                .withName(EXTERNAL_LISTENER_NAME)
                                .withPort(9094)
                                .withType(externalListenerType)
                                .withTls(true)
                                .withAuth(plainOverOauthAuthenticationListener)
                                .withConfiguration(listenerConfigBuilder.build())
                                .build(),
                        new GenericKafkaListenerBuilder()
                                .withName("oauth")
                                .withPort(9095)
                                .withType(KafkaListenerType.INTERNAL)
                                .withTls(true)
                                .withAuth(oauthAuthenticationListener)
                                .withNetworkPolicyPeers(new NetworkPolicyPeerBuilder()
                                        .withNewPodSelector()
                                            .addToMatchLabels("app", AbstractAdminServer.adminServerName(managedKafka))
                                        .endPodSelector()
                                        .build())
                                .build(),
                        new GenericKafkaListenerBuilder()
                                .withName("sre")
                                .withPort(9096)
                                .withType(KafkaListenerType.INTERNAL)
                                .withNetworkPolicyPeers(new NetworkPolicyPeerBuilder()
                                        .withNewPodSelector()
                                            .addToMatchLabels("strimzi.io/name", managedKafka.getMetadata().getName() + "-kafka")
                                        .endPodSelector()
                                        .build())
                                .withTls(false)
                                .build()
                );
    }

    protected Map<String, String> buildExternalListenerAnnotations(ManagedKafka managedKafka) {
        return Map.of();
    }

    protected List<GenericKafkaListenerConfigurationBroker> buildBrokerOverrides(ManagedKafka managedKafka, int replicas) {
        List<GenericKafkaListenerConfigurationBroker> brokerOverrides = new ArrayList<>(replicas);
        for (int i = 0; i < replicas; i++) {
            brokerOverrides.add(
                    new GenericKafkaListenerConfigurationBrokerBuilder()
                            .withHost(String.format("broker-%d-%s", i, managedKafka.getSpec().getEndpoint().getBootstrapServerHost()))
                            .withBroker(i)
                            .build()
            );
        }
        return brokerOverrides;
    }

    protected CertAndKeySecretSource buildTlsCertAndKeySecretSource(ManagedKafka managedKafka) {
        if (!SecuritySecretManager.isKafkaExternalCertificateEnabled(managedKafka)) {
            return null;
        }
        return new CertAndKeySecretSourceBuilder()
                .withSecretName(SecuritySecretManager.kafkaTlsSecretName(managedKafka))
                .withCertificate("tls.crt")
                .withKey("tls.key")
                .build();
    }

    protected GenericSecretSource buildSsoClientGenericSecretSource(ManagedKafka managedKafka) {
        if (managedKafka.getSpec().getOauth().getClientSecret() == null &&
                managedKafka.getSpec().getOauth().getClientSecretRef() == null) {
            return null;
        }

        return new GenericSecretSourceBuilder()
                .withSecretName(SecuritySecretManager.ssoClientSecretName(managedKafka))
                .withKey("ssoClientSecret")
                .build();
    }

    protected CertSecretSource buildSsoTlsCertSecretSource(ManagedKafka managedKafka) {
        if (!SecuritySecretManager.isKafkaAuthenticationEnabled(managedKafka) || managedKafka.getSpec().getOauth().getTlsTrustedCertificate() == null) {
            return null;
        }
        return new CertSecretSourceBuilder()
                .withSecretName(SecuritySecretManager.ssoTlsSecretName(managedKafka))
                .withCertificate("keycloak.crt")
                .build();
    }

    /**
     * Extract the inter broker protocol (IBP) version corresponding to the provided Kafka version
     *
     * @param kafkaVersion Kafka version from which to extract the inter broker protocol version
     * @return inter broker protocol version
     */
    public static String getKafkaIbpVersion(String kafkaVersion) {
        return getMajorMinorKafkaVersion(kafkaVersion);
    }

    /**
     * Extract the log message format version corresponding to the provided Kafka version
     *
     * @param kafkaVersion Kafka version from which to extract the log message format version
     * @return log message format version
     */
    public static String getKafkaLogMessageFormatVersion(String kafkaVersion) {
        return getMajorMinorKafkaVersion(kafkaVersion);
    }

    /**
     * Extract <Major>.<Minor> version from Kafka version
     *
     * @param kafkaVersion Kafka version from which to extract the <Major>.<Minor> version
     * @return <Major>.<Minor> version
     */
    private static String getMajorMinorKafkaVersion(String kafkaVersion) {
        String[] digits = kafkaVersion.split("\\.");
        if (digits.length > 3) {
            throw new IllegalArgumentException(String.format("The Kafka version %s is not a valid one", kafkaVersion));
        }
        return String.format("%s.%s", digits[0], digits[1]);
    }

    public Quantity calculateRetentionSize(ManagedKafka managedKafka) {
        return managedKafka.getSpec().getCapacity().getMaxDataRetentionSize();
    }

    private String getOAuthClientId(ManagedKafka managedKafka) {
        return secretManager.getSecretValue(managedKafka,
                managedKafka.getSpec().getOauth(),
                ManagedKafkaAuthenticationOAuth::getClientIdRef,
                ManagedKafkaAuthenticationOAuth::getClientId);
    }

    public abstract int getReplicas(ManagedKafka managedKafka);

    public boolean updatesInProgress(ManagedKafka managedKafka) {
        boolean strimziUpdating = isStrimziUpdating(managedKafka);
        boolean kafkaUpdating = isKafkaUpdating(managedKafka);
        boolean kafkaIbpUpdating = isKafkaIbpUpdating(managedKafka);
        boolean kafkaStabilityChecking = isKafkaUpgradeStabilityChecking(managedKafka);

        return strimziUpdating || kafkaUpdating || kafkaIbpUpdating || kafkaStabilityChecking;
    }
}
