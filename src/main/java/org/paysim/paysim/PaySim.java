package org.paysim.paysim;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.spark.SparkConf;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SQLContext;
import org.xerial.snappy.OSInfo;
import sim.engine.SimState;

//import org.paysim.paysim.parameters.*;

import org.paysim.paysim.parameters.Parameters;
import org.paysim.paysim.parameters.BalancesClients;
import org.paysim.paysim.parameters.ActionTypes;
import org.paysim.paysim.parameters.TypologiesFiles;

import org.paysim.paysim.actors.Bank;
import org.paysim.paysim.actors.Client;
import org.paysim.paysim.actors.Fraudster;
import org.paysim.paysim.actors.Merchant;
import org.paysim.paysim.actors.networkdrugs.NetworkDrug;

import org.paysim.paysim.base.Transaction;
import org.paysim.paysim.base.ClientActionProfile;
import org.paysim.paysim.base.StepActionProfile;

//https://towardsdatascience.com/the-art-of-engineering-features-for-a-strong-machine-learning-model-a47a876e654c
public class PaySim extends SimState {
    public static final double PAYSIM_VERSION = 2.0;

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


    public static void main(String[] args)  throws Exception {

        // configure spark
        SparkConf sparkConf = new SparkConf().setAppName("Print Elements of RDD").setMaster("local[2]");
        // start a spark context
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(sc);

        // sample collection
        List<Integer> collection = Arrays.asList(1, 2); //, 3, 4, 5, 6, 7, 8, 9, 10

        // parallelize the collection to two partitions
        JavaRDD<Integer> rdd = sc.parallelize(collection);



        JavaRDD<Transaction> transactionRDD = rdd.map(new Function<Integer, ArrayList<Transaction>>() {
            @Override
            public ArrayList<Transaction> call(Integer integer) throws Exception {
                String propertiesFile = "PaySim.properties";

//        ClassLoader classLoader = PaySim.class.getClassLoader();
                ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

                Parameters.initParameters(classLoader, classLoader.getResourceAsStream(propertiesFile));

                PaySim p = new PaySim();
                ArrayList<Transaction> results = p.runSimulation(classLoader);
                return results;
            };
        }).flatMap(x -> {
            return x.iterator();
        });

        sqlContext.createDataFrame(transactionRDD, Transaction.class).show();

    }


    public PaySim() throws Exception {
//        super(Parameters.getSeed());
        super(System.currentTimeMillis() % Integer.MAX_VALUE);
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

    private ArrayList<Transaction> runSimulation(ClassLoader classLoader) {

        System.out.println();
        System.out.println("Starting PaySim Running for " + Parameters.nbSteps + " steps.");
        long startTime = System.currentTimeMillis();
        super.start();

        initCounters();
        initActors(classLoader);

        //----------------------------------------------------------------------------
        while ((currentStep = (int) schedule.getSteps()) < Parameters.nbSteps) {
            System.out.println(schedule.getSteps());
            if (!schedule.step(this))
                break;

            writeOutputStep();
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

        return transactions;
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

    private void resetVariables() {
        if (transactions.size() > 0) {
            stepParticipated++;
        }
        transactions = new ArrayList<>();
    }

    private void writeOutputStep() {
        ArrayList<Transaction> transactions = getTransactions();

        totalTransactionsMade += transactions.size();

//        resetVariables();
    }

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

    public ArrayList<Transaction> getTransactions() {
        return transactions;
    }

//    public void sendTransactiontoKafka(Transaction transaction) {
//        try {
//            transactionproducer.run(Parameters.kafkaBrockers, Parameters.kafkaTopic, transaction);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }

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