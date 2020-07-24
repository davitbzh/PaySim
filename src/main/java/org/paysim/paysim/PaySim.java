package org.paysim.paysim;

import java.io.File;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

import org.paysim.paysim.flink.TransactionProducer;
import sim.engine.SimState;

import org.paysim.paysim.parameters.*;

import org.paysim.paysim.actors.Bank;
import org.paysim.paysim.actors.Client;
import org.paysim.paysim.actors.Fraudster;
import org.paysim.paysim.actors.Merchant;
import org.paysim.paysim.actors.networkdrugs.NetworkDrug;

import org.paysim.paysim.base.Transaction;
import org.paysim.paysim.base.ClientActionProfile;
import org.paysim.paysim.base.StepActionProfile;

import org.paysim.paysim.output.Output;
//https://towardsdatascience.com/the-art-of-engineering-features-for-a-strong-machine-learning-model-a47a876e654c
public class PaySim extends SimState {
    public static final double PAYSIM_VERSION = 2.0;
    private static final String[] DEFAULT_ARGS = new String[]{"", "-file", "PaySim.properties", "5"};

    public final String simulationName;
    private int totalTransactionsMade = 0;
    private int stepParticipated = 0;

    private ArrayList<Client> clients = new ArrayList<>();
    private ArrayList<Merchant> merchants = new ArrayList<>();
    private ArrayList<Fraudster> fraudsters = new ArrayList<>();
    private ArrayList<Bank> banks = new ArrayList<>();

    private ArrayList<Transaction> transactions = new ArrayList<>();
    private int currentStep;

    private Map<ClientActionProfile, Integer> countProfileAssignment = new HashMap<>();


    TransactionProducer transactionproducer = new TransactionProducer();

    public static void main(String[] args) {

        System.out.println("PAYSIM: Financial Simulator v" + PAYSIM_VERSION);
        if (args.length < 4) {
            args = DEFAULT_ARGS;
        }
        int nbTimesRepeat = Integer.parseInt(args[3]);

        String propertiesFile = "";
        for (int x = 0; x < args.length - 1; x++) {
            if (args[x].equals("-file")) {
                propertiesFile = args[x + 1];
            }
        }

        // https://stackoverflow.com/questions/57492083/running-java-jar-with-included-config-via-maven-on-flink-yarn-cluster
        // http://apache-flink-user-mailing-list-archive.2336050.n4.nabble.com/Linkage-Error-RocksDB-and-flink-1-6-4-td28385.html
        // add classloader.resolve-order: parent-first to /srv/hops/flink/conf/flink-conf.yaml
        ClassLoader classLoader = PaySim.class.getClassLoader();
        Parameters.initParameters(classLoader, classLoader.getResourceAsStream(propertiesFile));

//        Parameters.initParameters(propertiesFile);
        for (int i = 0; i < nbTimesRepeat; i++) {
            PaySim p = new PaySim();
            p.runSimulation(classLoader);
        }
    }

    public PaySim() {
        super(Parameters.getSeed());
        BalancesClients.setRandom(random);
        Parameters.clientsProfiles.setRandom(random);

        DateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
        Date currentTime = new Date();
        simulationName = "PS_" + dateFormat.format(currentTime) + "_" + seed();

//        File simulationFolder = new File(Parameters.outputPath + simulationName);
//        simulationFolder.mkdirs();
//
//        Output.initOutputFilenames(simulationName);
//        Output.writeParameters(seed());
    }

    private void runSimulation(ClassLoader classLoader) {
        System.out.println();
        System.out.println("Starting PaySim Running for " + Parameters.nbSteps + " steps.");
        long startTime = System.currentTimeMillis();
        super.start();

        initCounters();
        initActors(classLoader);

        //----------------------------------------------------------------------------
        while ((currentStep = (int) schedule.getSteps()) < Parameters.nbSteps) {
            if (!schedule.step(this))
                break;

//            writeOutputStep();
            if (currentStep % 100 == 100 - 1) {
                System.out.println("Step " + currentStep);
            } else {
                System.out.print("*");
            }
        }
        //----------------------------------------------------------------------------

        System.out.println();
        System.out.println("Finished running " + currentStep + " steps ");
//        finish();

        double total = System.currentTimeMillis() - startTime;
        total = total / 1000 / 60;
        System.out.println("It took: " + total + " minutes to execute the simulation");
        System.out.println("Simulation name: " + simulationName);
        System.out.println();
    }

    private void initCounters() {
        for (String action : ActionTypes.getActions()) {
            for (ClientActionProfile clientActionProfile : Parameters.clientsProfiles.getProfilesFromAction(action)) {
                countProfileAssignment.put(clientActionProfile, 0);
            }
        }
    }

    private void initActors(ClassLoader classLoader) {
        System.out.println("Init - Seed " + seed());

        //Add the merchants
        System.out.println("NbMerchants: " + (int) (Parameters.nbMerchants * Parameters.multiplier));
        for (int i = 0; i < Parameters.nbMerchants * Parameters.multiplier; i++) {
            Merchant m = new Merchant(generateId());
            merchants.add(m);
        }

        //Add the fraudsters
        System.out.println("NbFraudsters: " + (int) (Parameters.nbFraudsters * Parameters.multiplier));
        for (int i = 0; i < Parameters.nbFraudsters * Parameters.multiplier; i++) {
            Fraudster f = new Fraudster(generateId());
            fraudsters.add(f);
            schedule.scheduleRepeating(f);
        }

        //Add the banks
        System.out.println("NbBanks: " + Parameters.nbBanks);
        for (int i = 0; i < Parameters.nbBanks; i++) {
            Bank b = new Bank(generateId());
            banks.add(b);
        }

        //Add the clients
        System.out.println("NbClients: " + (int) (Parameters.nbClients * Parameters.multiplier));
        for (int i = 0; i < Parameters.nbClients * Parameters.multiplier; i++) {
            Client c = new Client(this);
            clients.add(c);
        }

        NetworkDrug.createNetwork(this, Parameters.typologiesFolder + TypologiesFiles.drugNetworkOne, classLoader);

        // Do not write code under this part otherwise clients will not be used in simulation
        // Schedule clients to act at each step of the simulation
        for (Client c : clients) {
            schedule.scheduleRepeating(c);
        }
    }

    public Map<String, ClientActionProfile> pickNextClientProfile() {
        Map<String, ClientActionProfile> profile = new HashMap<>();
        for (String action : ActionTypes.getActions()) {
            ClientActionProfile clientActionProfile = Parameters.clientsProfiles.pickNextActionProfile(action);

            profile.put(action, clientActionProfile);

            int count = countProfileAssignment.get(clientActionProfile);
            countProfileAssignment.put(clientActionProfile, count + 1);
        }
        return profile;
    }

//    public void finish() {
//        Output.writeFraudsters(fraudsters);
//        Output.writeClientsProfiles(countProfileAssignment, (int) (Parameters.nbClients * Parameters.multiplier));
//        Output.writeSummarySimulation(this);
//    }

//    private void resetVariables() {
//        if (transactions.size() > 0) {
//            stepParticipated++;
//        }
//        transactions = new ArrayList<>();
//    }

//    private void writeOutputStep() {
//        ArrayList<Transaction> transactions = getTransactions();
//
//        totalTransactionsMade += transactions.size();
//
//        Output.incrementalWriteRawLog(currentStep, transactions);
//        if (Parameters.saveToDB) {
//            Output.writeDatabaseLog(Parameters.dbUrl, Parameters.dbUser, Parameters.dbPassword, transactions, simulationName);
//        }
//
//        Output.incrementalWriteStepAggregate(currentStep, transactions);
//        resetVariables();
//    }

    public String generateId() {
        final String alphabet = "0123456789";
        final int sizeId = 10;
        StringBuilder idBuilder = new StringBuilder(sizeId);

        for (int i = 0; i < sizeId; i++)
            idBuilder.append(alphabet.charAt(random.nextInt(alphabet.length())));
        return idBuilder.toString();
    }

    public Merchant pickRandomMerchant() {
        return merchants.get(random.nextInt(merchants.size()));
    }

    public Bank pickRandomBank() {
        return banks.get(random.nextInt(banks.size()));
    }

    public Client pickRandomClient(String nameOrig) {
        Client clientDest = null;

        String nameDest = nameOrig;
        while (nameOrig.equals(nameDest)) {
            clientDest = clients.get(random.nextInt(clients.size()));
            nameDest = clientDest.getName();
        }
        return clientDest;
    }

    public int getTotalTransactions() {
        return totalTransactionsMade;
    }

    public int getStepParticipated() {
        return stepParticipated;
    }

//    public ArrayList<Transaction> getTransactions() {
//        return transactions;
//    }

    public void sendTransactiontoKafka(Transaction transaction) throws Exception {
        System.out.println("*** Sending transaction to kafka");
        transactionproducer.run(Parameters.kafkaBrockers, Parameters.kafkaTopic, transaction);
    }

    public ArrayList<Client> getClients() {
        return clients;
    }

    public void addClient(Client c) {
        clients.add(c);
    }

    public int getStepTargetCount() {
        return Parameters.stepsProfiles.getTargetCount(currentStep);
    }

    public Map<String, Double> getStepProbabilities() {
        return Parameters.stepsProfiles.getProbabilitiesPerStep(currentStep);
    }

    public StepActionProfile getStepAction(String action) {
        return Parameters.stepsProfiles.getActionForStep(currentStep, action);
    }
}