package org.bf2.operator.controllers;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.Operator;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.kubernetes.client.KubernetesServerTestResource;
import org.bf2.common.ManagedKafkaAgentResourceClient;
import org.bf2.common.OperandUtils;
import org.bf2.operator.MockResourceInformerFactory;
import org.bf2.operator.managers.CapacityManager;
import org.bf2.operator.resources.v1alpha1.ManagedKafkaAgent;
import org.bf2.operator.resources.v1alpha1.ProfileBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.inject.Inject;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTestResource(KubernetesServerTestResource.class)
@QuarkusTest
class ManagedKafkaAgentControllerTest {

    @Inject
    ManagedKafkaAgentController mkaController;

    @Inject
    ManagedKafkaAgentResourceClient agentClient;

    @Inject
    KubernetesClient client;

    @Inject
    Operator operator;

    @Inject
    MockResourceInformerFactory resourceInformerFactory;

    @BeforeEach
    void setup() {
        operator.start();
        resourceInformerFactory.setProxy(true);
    }

    @AfterEach
    void teardown() {
        resourceInformerFactory.setProxy(false);
    }

    @Test
    void shouldCreateStatus() {
        ManagedKafkaAgent dummyInstance = ManagedKafkaAgentResourceClient.getDummyInstance();
        dummyInstance.getMetadata().setNamespace(agentClient.getNamespace());
        assertNull(dummyInstance.getStatus());

        //should create the status
        agentClient.create(dummyInstance);

        await().ignoreException(NullPointerException.class).atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            ManagedKafkaAgent agent = agentClient.getByName(agentClient.getNamespace(), ManagedKafkaAgentResourceClient.RESOURCE_NAME);
            assertNotNull(agent.getStatus());
            assertTrue(agent.getStatus().getCapacity().isEmpty());
        });

        agentClient.delete(agentClient.getNamespace(), ManagedKafkaAgentResourceClient.RESOURCE_NAME);

        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            ManagedKafkaAgent agent = agentClient.getByName(agentClient.getNamespace(), ManagedKafkaAgentResourceClient.RESOURCE_NAME);
            assertNull(agent);
        });
    }

    @Test
    void testMaxCapacityCalculations() {
        ManagedKafkaAgent dummyInstance = ManagedKafkaAgentResourceClient.getDummyInstance();
        dummyInstance.getMetadata().setNamespace(agentClient.getNamespace());
        dummyInstance.getSpec().setCapacity(Map.of("standard", new ProfileBuilder().withMaxNodes(30).build(), "developer", new ProfileBuilder().withMaxNodes(30).build()));

        // should work without the resource map aleady existing
        agentClient.create(dummyInstance);

        await().ignoreException(NullPointerException.class).atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            ManagedKafkaAgent agent = agentClient.getByName(agentClient.getNamespace(), ManagedKafkaAgentResourceClient.RESOURCE_NAME);
            assertEquals(10, agent.getStatus().getCapacity().get("standard").getMaxUnits());
            assertEquals(300, agent.getStatus().getCapacity().get("developer").getMaxUnits());
            assertEquals(10, agent.getStatus().getCapacity().get("standard").getRemainingUnits());

            // add some dummy resources
            ConfigMap configMap = new ConfigMapBuilder().withNewMetadata()
                    .withLabels(OperandUtils.getDefaultLabels())
                    .withName(CapacityManager.FLEETSHARD_RESOURCES)
                    .endMetadata()
                    .withData(Map.of("standard", "5", "developer", "100"))
                    .build();
            OperandUtils.setAsOwner(agent, configMap);
            client.configMaps().replace(configMap);
        });

        await().ignoreException(NullPointerException.class).atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            ManagedKafkaAgent agent = agentClient.getByName(agentClient.getNamespace(), ManagedKafkaAgentResourceClient.RESOURCE_NAME);
            assertEquals(5, agent.getStatus().getCapacity().get("standard").getRemainingUnits());
            assertEquals(200, agent.getStatus().getCapacity().get("developer").getRemainingUnits());
        });

        agentClient.delete(agentClient.getNamespace(), ManagedKafkaAgentResourceClient.RESOURCE_NAME);

        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            ManagedKafkaAgent agent = agentClient.getByName(agentClient.getNamespace(), ManagedKafkaAgentResourceClient.RESOURCE_NAME);
            assertNull(agent);
        });

        client.configMaps().withName(CapacityManager.FLEETSHARD_RESOURCES).delete();
    }
}
