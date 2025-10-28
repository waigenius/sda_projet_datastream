package com.nifi.example;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;


public class ExempleDIATTARATest {

    private TestRunner testRunner;

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(ExempleDIATTARA.class);
    }

    @Test
    public void testProcessor() {
        // Ajouter des données à l'entrée
        testRunner.enqueue("diattara,25");

        // Exécuter le processeur
        testRunner.run();

        // Vérifier si le processeur a produit une sortie
        testRunner.assertTransferCount(ExempleDIATTARA.SUCCESS, 1);

        // Récupérer le contenu du FlowFile de sortie
        String outputContent = new String(testRunner.getFlowFilesForRelationship(ExempleDIATTARA.SUCCESS).get(0).toByteArray());

        // Vérifier le contenu de la sortie
        assertEquals("{\"nom\":\"diattara\", \"age\":25}", outputContent.trim());
    }
}

