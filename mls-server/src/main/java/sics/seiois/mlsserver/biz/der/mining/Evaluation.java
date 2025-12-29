package sics.seiois.mlsserver.biz.der.mining;


import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sics.seiois.mlsserver.biz.der.metanome.REE;
import sics.seiois.mlsserver.biz.der.metanome.input.ParsedColumn;
import sics.seiois.mlsserver.biz.der.metanome.predicates.Predicate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import sics.seiois.mlsserver.biz.der.metanome.predicates.sets.PredicateSet;
import sics.seiois.mlsserver.biz.der.mining.utils.*;
import sics.seiois.mlsserver.service.impl.PredicateSetAssist;

import java.io.*;
import java.util.*;


// given a set of rules, evaluate it
public class Evaluation {
    private static Logger logger = LoggerFactory.getLogger(Evaluation.class);
    public static int MIN_NUM_WORK_UNITS = 200000;
    public static int MAX_CURRENT_PREDICTES = 5;

    // generate all predicate set including different tuple ID pair
    private static final PredicateProviderIndex predicateProviderIndex = PredicateProviderIndex.getInstance();
    private ArrayList<Predicate> allPredicates;
    private ArrayList<Predicate> allConstantPredicates;
    private ArrayList<Predicate> allNonConstantPredicates;
    private int maxTupleVariableNum; // the maximum number of tuples in a rule
    private String data_name;
    private double sampleRatioForTupleCov;
    private int maxOneRelationNum;
    private int allCount;
    private int K;

    private double lambda;

    private double w_supp;
    private double w_conf;
    private double w_rel_model;
    private double w_attr_non;
    private double w_pred_non;
    private double w_attr_dis;
    private double w_tuple_cov;

    private double w_fitness;
    private double w_unexpectedness;

    // input a set of REEs
    private String inputREEsPath;
    private String inputAllREEsPath;
    private ArrayList<REE> rees;
    private ArrayList<REE> all_rees;

    // evaluation results of the given rees
    private ArrayList<Long> supports;
    private double average_support = 0.0;
    private long maximum_support = 0L;
    private long minimum_support = 0L;

    private ArrayList<Double> confidences;
    private double average_confidence = 0.0;
    private double maximum_confidence = 0.0;
    private double minimum_confidence = 0.0;

    private ArrayList<Double> relevance_model_scores;
    private double average_relevance_model = 0.0;
    private double maximum_relevance_model = 0.0;
    private double minimum_relevance_model = 0.0;

    private double average_length;
    private int maximum_length;
    private int minimum_length;

    private int attribute_nonoverlap;
    private int predicate_nonoverlap;
    private double attribute_distance;

    private ArrayList<Long> tuple_coverages;
    private long tuple_coverage = 0L;

    private double relevance_score = 0.0;
    private double diversity_score = 0.0;
    private double score = 0.0;

    private Diversity diversity;

    private RelevanceModel relevance_model;

    private int index_null_string;
    private int index_null_double;
    private int index_null_long;

    // ----------------- for obtaining suboptimal results, using swapping algorithm -----------------
    private int topKNum;
    private HashMap<REE, Long> supports_map;
    private HashMap<REE, Double> confidences_map;
    private HashMap<REE, Double> relevance_model_scores_map;

//    private HashMap<REE, Double> fitness_map;
//    private HashMap<REE, Double> unexpectedness_map;
    private HashMap<REE, Double> relevance_scores_map;

    public Evaluation(String taskId) {
        this.data_name = taskId;
    }

    public Evaluation(ArrayList<Predicate> predicates, String inputREEsPath, String inputAllREEsPath,
                      int maxTupleVariableNum, int K, String taskId,
                      int maxOneRelationNum, int allCount, double lambda,
                      String predicateToIDFile, String relevanceModelFile, FileSystem hdfs,
                      int index_null_string, int index_null_double, int index_null_long,
                      double w_fitness, double w_unexpectedness) {
        this.allPredicates = new ArrayList<>();
        this.allConstantPredicates = new ArrayList<>();
        this.allNonConstantPredicates = new ArrayList<>();
        this.rees = new ArrayList<>();
        this.all_rees = new ArrayList<>();
        this.supports = new ArrayList<>();
        this.confidences = new ArrayList<>();
        this.relevance_model_scores = new ArrayList<>();
        this.tuple_coverages = new ArrayList<>();

        this.inputREEsPath = inputREEsPath;
        this.inputAllREEsPath = inputAllREEsPath;
        logger.info("#### inputREEsPath: {}", this.inputREEsPath);
        logger.info("#### inputAllREEsPath: {}", this.inputAllREEsPath);
        this.maxTupleVariableNum = maxTupleVariableNum;
        this.data_name = taskId;
        this.maxOneRelationNum = maxOneRelationNum;
        this.allCount = allCount;
        this.lambda = lambda;
        this.w_fitness = w_fitness;
        this.w_unexpectedness = w_unexpectedness;
        this.K = K;

        this.index_null_string = index_null_string;
        this.index_null_double = index_null_double;
        this.index_null_long = index_null_long;
        logger.info("index_null_string: {}", index_null_string);
        logger.info("index_null_double: {}", index_null_double);
        logger.info("index_null_long: {}", index_null_long);

        this.allPredicates = predicates;
        logger.info("allPredicates size: {}", this.allPredicates.size());
        for (Predicate p : this.allPredicates) {
            if (p.isConstant()) {
                this.allConstantPredicates.add(p);
            } else {
                this.allNonConstantPredicates.add(p);
            }
        }

        // set support for each predicate
        HashMap<String, HashMap<Integer, Long>> statistic = new HashMap<>();
        for (Predicate p : this.allPredicates) {
            // statistic
            // op1
            ParsedColumn<?> col1 = p.getOperand1().getColumn();
            if (!statistic.containsKey(col1.toStringData())) {
                HashMap<Integer, Long> temp = new HashMap<>();
                for (int i = 0; i < col1.getValueIntSize(); i++) {
                    if (!temp.containsKey(col1.getValueInt(i))) {
                        temp.put(col1.getValueInt(i), 1L);
                    } else {
                        temp.put(col1.getValueInt(i), temp.get(col1.getValueInt(i)) + 1);
                    }
                }
                statistic.put(col1.toStringData(), temp);
            }

            // op2
            ParsedColumn<?> col2 = p.getOperand2().getColumn();
            if (!statistic.containsKey(col2.toStringData())) {
                HashMap<Integer, Long> temp = new HashMap<>();
                for (int i = 0; i < col2.getValueIntSize(); i++) {
                    if (!temp.containsKey(col2.getValueInt(i))) {
                        temp.put(col2.getValueInt(i), 1L);
                    } else {
                        temp.put(col2.getValueInt(i), temp.get(col2.getValueInt(i)) + 1);
                    }
                }
                statistic.put(col2.toStringData(), temp);
            }
            // statistic support for predicate
            p.setSupportPredicate(statistic);
        }

        // add to Predicate Set
        for (Predicate p : this.allPredicates) {
            predicateProviderIndex.addPredicate(p);
        }

        this.prepareAllPredicatesMultiTuples();

        logger.info("after adding new predicates, allPredicates size: {}", this.allPredicates.size());
        logger.info("#### all non-constant predicates size: {}", this.allNonConstantPredicates.size());
        logger.info("#### all constant predicates size: {}", this.allConstantPredicates.size());

        this.relevance_model = new RelevanceModel(predicateToIDFile, relevanceModelFile, hdfs, 2);
    }

    public Evaluation(ArrayList<Predicate> predicates, String inputREEsPath, int maxTupleVariableNum, String taskId,
                      int maxOneRelationNum, int allCount, double sampleRatioForTupleCov, double lambda,
                      double w_supp, double w_conf, double w_rel_model, double w_attr_non, double w_pred_non, double w_attr_dis, double w_tuple_cov,
                      String predicateToIDFile, String relevanceModelFile, FileSystem hdfs,
                      int index_null_string, int index_null_double, int index_null_long,
                      boolean flag) {
        this.allPredicates = new ArrayList<>();
        this.allConstantPredicates = new ArrayList<>();
        this.allNonConstantPredicates = new ArrayList<>();
        this.rees = new ArrayList<>();
        this.supports = new ArrayList<>();
        this.confidences = new ArrayList<>();
        this.relevance_model_scores = new ArrayList<>();
        this.tuple_coverages = new ArrayList<>();

        this.inputREEsPath = inputREEsPath;
        this.maxTupleVariableNum = maxTupleVariableNum;
        this.data_name = taskId;
        this.maxOneRelationNum = maxOneRelationNum;
        this.allCount = allCount;
        this.sampleRatioForTupleCov = sampleRatioForTupleCov;
        this.lambda = lambda;
        this.w_supp = w_supp;
        this.w_conf = w_conf;
        this.w_rel_model = w_rel_model;
        this.w_attr_non = w_attr_non;
        this.w_pred_non = w_pred_non;
        this.w_attr_dis = w_attr_dis;
        this.w_tuple_cov = w_tuple_cov;

        this.index_null_string = index_null_string;
        this.index_null_double = index_null_double;
        this.index_null_long = index_null_long;
        logger.info("index_null_string: {}", index_null_string);
        logger.info("index_null_double: {}", index_null_double);
        logger.info("index_null_long: {}", index_null_long);

        this.allPredicates = predicates;
        logger.info("allPredicates size: {}", this.allPredicates.size());
        for (Predicate p : this.allPredicates) {
            if (p.isConstant()) {
                this.allConstantPredicates.add(p);
            } else {
                this.allNonConstantPredicates.add(p);
            }
        }

        // set support for each predicate
        HashMap<String, HashMap<Integer, Long>> statistic = new HashMap<>();
        for (Predicate p : this.allPredicates) {
            // statistic
            // op1
            ParsedColumn<?> col1 = p.getOperand1().getColumn();
            if (!statistic.containsKey(col1.toStringData())) {
                HashMap<Integer, Long> temp = new HashMap<>();
                for (int i = 0; i < col1.getValueIntSize(); i++) {
                    if (!temp.containsKey(col1.getValueInt(i))) {
                        temp.put(col1.getValueInt(i), 1L);
                    } else {
                        temp.put(col1.getValueInt(i), temp.get(col1.getValueInt(i)) + 1);
                    }
                }
                statistic.put(col1.toStringData(), temp);
            }

            // op2
            ParsedColumn<?> col2 = p.getOperand2().getColumn();
            if (!statistic.containsKey(col2.toStringData())) {
                HashMap<Integer, Long> temp = new HashMap<>();
                for (int i = 0; i < col2.getValueIntSize(); i++) {
                    if (!temp.containsKey(col2.getValueInt(i))) {
                        temp.put(col2.getValueInt(i), 1L);
                    } else {
                        temp.put(col2.getValueInt(i), temp.get(col2.getValueInt(i)) + 1);
                    }
                }
                statistic.put(col2.toStringData(), temp);
            }
            // statistic support for predicate
            p.setSupportPredicate(statistic);
        }

        // add to Predicate Set
        for (Predicate p : this.allPredicates) {
            predicateProviderIndex.addPredicate(p);
        }

        if (flag) {
            this.prepareAllPredicatesMultiTuples();
        }
        logger.info("after adding new predicates, allPredicates size: {}", this.allPredicates.size());
        logger.info("#### all non-constant predicates size: {}", this.allNonConstantPredicates.size());
        logger.info("#### all constant predicates size: {}", this.allConstantPredicates.size());

        this.relevance_model = new RelevanceModel(predicateToIDFile, relevanceModelFile, hdfs);
    }

    // Given a set of all rules, this is for obtaining suboptimal top-k diversified results, using swapping algorithm. version 1 [SIGMOD'25]
    public Evaluation(ArrayList<Predicate> predicates, String inputREEsPath, int maxTupleVariableNum, String taskId,
                      int maxOneRelationNum, int allCount, double sampleRatioForTupleCov, double lambda,
                      double w_supp, double w_conf, double w_rel_model, double w_attr_non, double w_pred_non, double w_attr_dis, double w_tuple_cov,
                      String predicateToIDFile, String relevanceModelFile, FileSystem hdfs,
                      int index_null_string, int index_null_double, int index_null_long,
                      int topKNum) throws Exception {

        this(predicates, inputREEsPath, maxTupleVariableNum, taskId,
                maxOneRelationNum, allCount, sampleRatioForTupleCov, lambda,
                w_supp, w_conf, w_rel_model, w_attr_non, w_pred_non, w_attr_dis, w_tuple_cov,
                predicateToIDFile, relevanceModelFile, hdfs,
                index_null_string, index_null_double, index_null_long,
                true);

        this.topKNum = topKNum;
        this.supports_map = new HashMap<>();
        this.confidences_map = new HashMap<>();
        this.relevance_model_scores_map = new HashMap<>();

        this.diversity = new Diversity("tuple_coverage");

        this.loadREEs();

        this.printREEs();

        // compute relevance score for each ree
        for (REE ree : this.rees) {
            double rel_score = this.relevance_model.predict_score(ree.getCurrentList(), ree.getRHS());
            this.relevance_model_scores_map.put(ree, rel_score);
        }
    }

    // Given a set of all rules, this is for obtaining suboptimal top-k diversified results, using swapping algorithm. version 2
    public Evaluation(ArrayList<Predicate> predicates, String inputREEsPath, int maxTupleVariableNum, String taskId,
                      int maxOneRelationNum, int allCount, double lambda,
                      double w_fitness, double w_unexpectedness,
                      String predicateToIDFile, String relevanceModelFile, String attributesUserInterested, String rulesUserAlreadyKnownFile, FileSystem hdfs,
                      int index_null_string, int index_null_double, int index_null_long,
                      int topKNum) throws Exception {

        this(predicates, inputREEsPath, null, maxTupleVariableNum, topKNum, taskId,
                maxOneRelationNum, allCount, lambda,
                predicateToIDFile, relevanceModelFile, hdfs,
                index_null_string, index_null_double, index_null_long,
                w_fitness, w_unexpectedness);

        this.topKNum = topKNum;
//        this.fitness_map = new HashMap<>();
//        this.unexpectedness_map = new HashMap<>();
        this.relevance_scores_map = new HashMap<>();

        this.loadREEs();

        this.printREEs();


        // load rules that rules already known
        ArrayList<REE> rulesUserAlreadyKnown = new ArrayList<>();
        this.loadRuleSet(rulesUserAlreadyKnownFile, hdfs, rulesUserAlreadyKnown);
//        logger.info("rulesUserAlreadyKnown: {}", rulesUserAlreadyKnown);

        // load attributes user are interested in
        ArrayList<Predicate> attributesUserInterestedRelatedPredicates = new ArrayList<>();
        for (String attr : attributesUserInterested.split("##")) {
            for (Predicate p : this.allPredicates) {
                if (p.getOperand1().getColumnLight().getName().equals(attr)) {
                    attributesUserInterestedRelatedPredicates.add(p);
                }
            }
        }
//        logger.info("attributesUserInterestedRelatedPredicates: {}", this.attributesUserInterestedRelatedPredicates);

        // compute relevance score for each ree
        for (REE ree : this.rees) {
            double fitness = this.relevance_model.computeFitnessScore(ree.getCurrentList(), ree.getRHS(), attributesUserInterestedRelatedPredicates);
            double unexpectedness = this.relevance_model.computeUnexpectednessScore(ree.getCurrentList(), ree.getRHS(), rulesUserAlreadyKnown);
            double rel_score = this.w_fitness * fitness + this.w_unexpectedness * unexpectedness;
//            this.fitness_map.put(ree, fitness);
//            this.unexpectedness_map.put(ree, unexpectedness);
            this.relevance_scores_map.put(ree, rel_score);
        }
    }

    public void loadREEs() throws IOException {
        FileSystem hdfs = FileSystem.get(new Configuration());
        FSDataInputStream inputTxt = hdfs.open(new Path(this.inputREEsPath));
        BufferedInputStream bis = new BufferedInputStream(inputTxt);
        InputStreamReader sReader = new InputStreamReader(bis, "UTF-8");
        BufferedReader bReader = new BufferedReader(sReader);
        String line;
        while ((line = bReader.readLine()) != null) {
            if (!line.contains("Rule")) {
                continue;
            }
            this.rees.add(parseLine(line, this.allNonConstantPredicates, this.allConstantPredicates));
        }
    }

    public void loadAllREEs() throws IOException {
        FileSystem hdfs = FileSystem.get(new Configuration());
        FSDataInputStream inputTxt = hdfs.open(new Path(this.inputAllREEsPath));
        BufferedInputStream bis = new BufferedInputStream(inputTxt);
        InputStreamReader sReader = new InputStreamReader(bis, "UTF-8");
        BufferedReader bReader = new BufferedReader(sReader);
        String line;
        while ((line = bReader.readLine()) != null) {
            if (!line.contains("Rule")) {
                continue;
            }
            this.all_rees.add(parseLine(line, this.allNonConstantPredicates, this.allConstantPredicates));
        }
    }

    public void loadREEs(ArrayList<REE> selected) {
        this.rees = new ArrayList<>();
        for (REE ree: selected) {
            this.rees.add(ree);
        }
    }

    public void printREEs() {
        logger.info("#### Loaded REEs:");
        for (REE ree : this.rees) {
            logger.info(ree.toString());
        }
    }


    public int computeAttributeNonoverlap(ArrayList<REE> rees) {
        int attr_overlap = 0;
        for (int i = 1; i < rees.size(); i++) {
            HashSet<String> attrs_ = new HashSet<>();
            for (int j = i - 1; j >= 0; j--) {
                attrs_.addAll(rees.get(j).getCovered_attrs());
            }
            attrs_.retainAll(rees.get(i).getCovered_attrs()); // intersection
            attr_overlap += attrs_.size();
        }
        return -attr_overlap;
    }

    public int computePredicateNonoverlap(ArrayList<REE> rees) {
        int pred_overlap = 0;
        for (int i = 1; i < rees.size(); i++) {
            HashSet<Predicate> predicates = new HashSet<>();
            for (int j = i - 1; j >= 0; j--) {
                for (Predicate p : rees.get(j).getCurrentList()) {
                    predicates.add(p);
                }
                predicates.add(rees.get(j).getRHS());
            }
            HashSet<Predicate> predicates_ = new HashSet<>();
            for (Predicate p : rees.get(i).getCurrentList()) {
                predicates_.add(p);
            }
            predicates_.add(rees.get(i).getRHS());

            predicates.retainAll(predicates_); // intersection
            pred_overlap += predicates.size();
        }
        return -pred_overlap;
    }

    public double computeAttributeDistance(ArrayList<REE> rees) {
        double min_dis = Double.POSITIVE_INFINITY;
        for (int i = 0; i < rees.size() - 1; i++) {
            for (int j = i + 1; j < rees.size(); j++) {
                double dis = rees.get(i).computeAttributeJaccardDistance(rees.get(j));
                if (dis < min_dis) {
                    min_dis = dis;
                }
            }
        }
        return min_dis;
    }

    /*
    * ------------------------------------------------- for evaluation - version 1 -------------------------------------------------
    * */
    public void evaluate(SparkSession spark, StringBuffer statisticsInfo) {
        int rule_num = this.rees.size();
        if (rule_num == 0 || rule_num == 1) {
            this.attribute_nonoverlap = 0;
            this.predicate_nonoverlap = 0;
            this.attribute_distance = 0.0;
        } else {
            // 1. attribute nonoverlap
            this.attribute_nonoverlap = computeAttributeNonoverlap(this.rees);

            // 2. predicate nonoverlap
            this.predicate_nonoverlap = computePredicateNonoverlap(this.rees);

            // 3. compute attribute distance
            this.attribute_distance = computeAttributeDistance(this.rees);
        }

        // 4. compute tuple coverage
        if (rule_num == 0) {
            this.tuple_coverage = 0L;
            return;
        }

        this.diversity = new Diversity("tuple_coverage");

        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        PredicateSetAssist psAssist = new PredicateSetAssist();
        psAssist.setIndexProvider(PredicateSet.indexProvider);//在这里设置，这个值肯定是确定了的
        psAssist.setBf(PredicateSet.bf);
        psAssist.setTaskId(this.data_name);
        Broadcast<PredicateSetAssist> bcpsAssist = sc.broadcast(psAssist);

        for (int i = 0; i < rule_num; i++) {
            // generate work unit of this.rees.get(i)
            WorkUnit workUnit = new WorkUnit();
            for (Predicate p : this.rees.get(i).getCurrentList()) {
                workUnit.addCurrent(p);
            }
            workUnit.addRHS(this.rees.get(i).getRHS());
            workUnit.setTransferData();

            ArrayList<WorkUnit> workUnits = new ArrayList<>();
            this.generateWorkunitsWithDesignatedPartition(workUnit, workUnits);

            // compute the sum of partialSolution's coverage and this.rees.get(i)'s marginal coverage w.r.t. partialSolution
            ArrayList<REE> partialSolution = new ArrayList<>();
//            for (int j = i - 1; j >= 0; j--) {
//                partialSolution.add(this.rees.get(j));
//            }
            for (int j = 0; j < i; j++) {
                partialSolution.add(this.rees.get(j));
            }

            List<Message> messages = this.run(workUnits, sc, bcpsAssist, partialSolution);
            Message message = this.integrateMessages(messages);  // only contain message for this.rees.get(i)

            logger.info("#### message.getAllCurrentRHSsSupport().size(): {}", message.getAllCurrentRHSsSupport().size()); // size = 1
            for (Map.Entry<Predicate, Long> entry : message.getAllCurrentRHSsSupport().entrySet()) {
                Predicate rhs = entry.getKey();
                long supportXRHS = entry.getValue();
//                double conf;
//                if (rhs.isConstant()) {
//                    if (rhs.getIndex1() == 1) {
//                        conf = supportXRHS * 1.0 / message.getCurrentSuppCP1();
//                    } else {
//                        conf = supportXRHS * 1.0 / message.getCurrentSuppCP0();
//                    }
//                } else {
//                    conf = supportXRHS * 1.0 / message.getCurrentSupp();
//                }
                double conf = supportXRHS * 1.0 / message.getCurrentSupp();
                this.supports.add(supportXRHS);
                this.confidences.add(conf);
                this.tuple_coverages.add(message.getMarginalTupleCoverage(PredicateSet.getIndex(rhs)));
            }

            this.diversity.updateCurrentPartialSolutionDiv(message.getCarriedInfoLight(this.rees.get(i).getRHS()));
        }

        // 5. relevance score
        for (REE ree : this.rees) {
            double rel_score = this.relevance_model.predict_score(ree.getCurrentList(), ree.getRHS());
            this.relevance_model_scores.add(rel_score);
        }

        // ----------------- collect statistics -----------------
        // support
        this.average_support = this.supports.get(0);
        this.maximum_support = this.supports.get(0);
        this.minimum_support = this.supports.get(0);
        for (int i = 1; i < rule_num; i++) {
            long supp = this.supports.get(i);
            this.average_support += supp;
            if (supp > this.maximum_support) {
                this.maximum_support = supp;
            }
            if (supp < this.minimum_support) {
                this.minimum_support = supp;
            }
        }
        this.average_support = this.average_support / rule_num;

        // confidence
        this.average_confidence = this.confidences.get(0);
        this.maximum_confidence = this.confidences.get(0);
        this.minimum_confidence = this.confidences.get(0);
        for (int i = 1; i < rule_num; i++) {
            double conf = this.confidences.get(i);
            this.average_confidence += conf;
            if (conf > this.maximum_confidence) {
                this.maximum_confidence = conf;
            }
            if (conf < this.minimum_confidence) {
                this.minimum_confidence = conf;
            }
        }
        this.average_confidence = this.average_confidence / rule_num;

        // relevance model score
        this.average_relevance_model = this.relevance_model_scores.get(0);
        this.maximum_relevance_model = this.relevance_model_scores.get(0);
        this.minimum_relevance_model = this.relevance_model_scores.get(0);
        for (int i = 1; i < rule_num; i++) {
            double rel_model_score = this.relevance_model_scores.get(i);
            this.average_relevance_model += rel_model_score;
            if (rel_model_score > this.maximum_relevance_model) {
                this.maximum_relevance_model = rel_model_score;
            }
            if (rel_model_score < this.minimum_relevance_model) {
                this.minimum_relevance_model = rel_model_score;
            }
        }
        this.average_relevance_model = this.average_relevance_model / rule_num;

        // rule length
        int len_rule0 = this.rees.get(0).getLength();
        this.average_length = len_rule0;
        this.maximum_length = len_rule0;
        this.minimum_length = len_rule0;
        for (int i = 1; i < rule_num; i++) {
            int len = this.rees.get(i).getLength();
            this.average_length += len;
            if (len > this.maximum_length) {
                this.maximum_length = len;
            }
            if (len < this.minimum_length) {
                this.minimum_length = len;
            }
        }
        this.average_length = this.average_length / rule_num;

        // tuple coverage
        for (int i = 0; i < rule_num; i++) {
            this.tuple_coverage += this.tuple_coverages.get(i);
        }

        // relevance score for each rule
        int K = 10; // default setting: Top-10
        ArrayList<Double> relevance_scores_all = new ArrayList<>();
        for (int i = 0; i < rule_num; i++) {
            double score_temp = 0.0;
            score_temp += this.w_supp * this.supports.get(i) * 1.0 / this.allCount / this.allCount;
            score_temp += this.w_conf * this.confidences.get(i);
            score_temp += this.w_rel_model * this.relevance_model_scores.get(i);
            relevance_scores_all.add(score_temp);
        }
        Collections.sort(relevance_scores_all, Collections.reverseOrder());
        int idx_temp = 0;
        double relevance_score_k_opt = 0.0;
        for (Double score_temp : relevance_scores_all) {
            if (idx_temp >= K) {
                break;
            }
            logger.info("In all rules, Top-{} relevance score: {}", idx_temp, score_temp);
            relevance_score_k_opt += score_temp;
            idx_temp++;
        }
        logger.info("relevance score for the optimal k set: {}", relevance_score_k_opt);

        // relevance score
        double sum_supp = 0;
        double sum_conf = 0;
        double sum_rel_model = 0;
        for (int i = 0; i < rule_num; i++) {
            this.relevance_score += this.w_supp * this.supports.get(i) * 1.0 / this.allCount / this.allCount;
            this.relevance_score += this.w_conf * this.confidences.get(i);
            this.relevance_score += this.w_rel_model * this.relevance_model_scores.get(i);
            if (i < K) {
                sum_supp += this.supports.get(i) * 1.0 / this.allCount / this.allCount;
                sum_conf += this.confidences.get(i);
                sum_rel_model += this.relevance_model_scores.get(i);
            }
        }
        this.relevance_score = this.relevance_score / rule_num;

        // diversity score
        this.diversity_score += this.w_attr_non * this.attribute_nonoverlap * 1.0 / MAX_CURRENT_PREDICTES;
        this.diversity_score += this.w_pred_non * this.predicate_nonoverlap * 1.0 / MAX_CURRENT_PREDICTES;
        this.diversity_score += this.w_attr_dis * this.attribute_distance;
        this.diversity_score += this.w_tuple_cov * this.tuple_coverage * 1.0 / this.allCount / this.allCount;

        // score
        this.score = this.relevance_score + this.lambda * this.diversity_score;


        // write statistical information into file
        statisticsInfo.append("rules: ").append("\n");
        for (REE ree : this.rees) {
            statisticsInfo.append(ree.toString()).append("\n");
        }

        statisticsInfo.append("\n");
        statisticsInfo.append("data size: ").append(this.allCount).append("\n");

        statisticsInfo.append("average support: ").append(this.average_support).append("\n");
        statisticsInfo.append("maximum support: ").append(this.maximum_support).append("\n");
        statisticsInfo.append("minimum support: ").append(this.minimum_support).append("\n\n");

        statisticsInfo.append("average confidence: ").append(this.average_confidence).append("\n");
        statisticsInfo.append("maximum confidence: ").append(this.maximum_confidence).append("\n");
        statisticsInfo.append("minimum confidence: ").append(this.minimum_confidence).append("\n\n");

        statisticsInfo.append("average relevance model score: ").append(this.average_relevance_model).append("\n");
        statisticsInfo.append("maximum relevance model score: ").append(this.maximum_relevance_model).append("\n");
        statisticsInfo.append("minimum relevance model score: ").append(this.minimum_relevance_model).append("\n\n");

        statisticsInfo.append("average length: ").append(this.average_length).append("\n");
        statisticsInfo.append("maximum length: ").append(this.maximum_length).append("\n");
        statisticsInfo.append("minimum length: ").append(this.minimum_length).append("\n\n");

        statisticsInfo.append("attribute nonoverlap: ").append(this.attribute_nonoverlap).append("\n");
        statisticsInfo.append("predicate nonoverlap: ").append(this.predicate_nonoverlap).append("\n");
        statisticsInfo.append("attribute distance: ").append(this.attribute_distance).append("\n");
        statisticsInfo.append("tuple coverage: ").append(this.tuple_coverage).append("\n\n");

        statisticsInfo.append("relevance score: ").append(this.relevance_score).append("\n");
        statisticsInfo.append("diversity score: ").append(this.diversity_score).append("\n");
        statisticsInfo.append("score: ").append(this.score).append("\n\n");

        statisticsInfo.append("sum of support score: ").append(sum_supp).append("\n\n");
        statisticsInfo.append("sum of confidence score: ").append(sum_conf).append("\n\n");
        statisticsInfo.append("sum of rel_model score: ").append(sum_rel_model).append("\n\n");
        statisticsInfo.append("relevance score for the optimal k set: ").append(relevance_score_k_opt).append("\n\n");

    }


    private void loadRuleSet(String rulesUserAlreadyKnownFile, FileSystem hdfs, ArrayList<REE> ruleSet) throws Exception {
        FSDataInputStream inputTxt = hdfs.open(new Path(rulesUserAlreadyKnownFile));
        BufferedInputStream bis = new BufferedInputStream(inputTxt);
        InputStreamReader sReader = new InputStreamReader(bis, "UTF-8");
        BufferedReader bReader = new BufferedReader(sReader);
        String line;
        while ((line = bReader.readLine()) != null) {
            if (!line.contains("Rule")) {
                continue;
            }
            ruleSet.add(this.parseLine(line, this.allPredicates, this.allPredicates));
        }
    }

    public double computeJaccardSimilarity(REE ree_1, REE ree_2) {
        // all predicates in ree_1
        HashSet<Predicate> pred_ree_1 = new HashSet<>();
        for (Predicate p : ree_1.getCurrentList()) {
            pred_ree_1.add(p);
        }
        pred_ree_1.add(ree_1.getRHS());

        // all predicates in ree_2
        HashSet<Predicate> pred_ree_2 = new HashSet<>();
        for (Predicate p : ree_2.getCurrentList()) {
            pred_ree_2.add(p);
        }
        pred_ree_2.add(ree_2.getRHS());

        // union
        HashSet<Predicate> union = new HashSet<>(pred_ree_1);
        union.addAll(pred_ree_2);

        // intersection
        HashSet<Predicate> intersection = new HashSet<>(pred_ree_1);
        intersection.retainAll(pred_ree_2);

        if (union.isEmpty()) {
            return 0.0;
        }
        return intersection.size() * 1.0 / union.size();
    }

    /*
     * ------------------------------------------------- for evaluation - version 2 -------------------------------------------------
     * */
    public void evaluate(StringBuffer statisticsInfo, FileSystem hdfs, String attributesUserInterested, String rulesUserAlreadyKnownFile) throws Exception {
        ArrayList<REE> rulesUserAlreadyKnown = new ArrayList<>();

        // load rules that rules already known
        this.loadRuleSet(rulesUserAlreadyKnownFile, hdfs, rulesUserAlreadyKnown);
//        logger.info("rulesUserAlreadyKnown: {}", rulesUserAlreadyKnown);

        // load attributes user are interested in
        ArrayList<Predicate> attributesUserInterestedRelatedPredicates = new ArrayList<>();
        for (String attr : attributesUserInterested.split("##")) {
            for (Predicate p : this.allPredicates) {
                if (p.getOperand1().getColumnLight().getName().equals(attr)) {
                    attributesUserInterestedRelatedPredicates.add(p);
                }
            }
        }
//        logger.info("attributesUserInterestedRelatedPredicates: {}", this.attributesUserInterestedRelatedPredicates);

        double total_fitness_score = 0.0;
        double total_unexpectedness_score = 0.0;
        double total_relevance_score = 0.0;
        double total_diversity_score = 0.0;
        double total_score = 0.0;
        // evaluate F() score of a given k-size rule set
        for (REE ree : this.rees) {
            double fitness = this.relevance_model.computeFitnessScore(ree.getCurrentList(), ree.getRHS(), attributesUserInterestedRelatedPredicates);
            double unexpectedness = this.relevance_model.computeUnexpectednessScore(ree.getCurrentList(), ree.getRHS(), rulesUserAlreadyKnown);
            double rel_score = this.w_fitness * fitness + this.w_unexpectedness * unexpectedness;
            logger.info("#### ree: {}, fitness: {}, unexpectedness: {}, rel_score: {}", ree, fitness, unexpectedness, rel_score);
            total_fitness_score += fitness;
            total_unexpectedness_score += unexpectedness;
            total_relevance_score += rel_score;
            ree.setFitness(fitness);
            ree.setUnexpectedness(fitness);
            ree.setRelevanceScore(rel_score);
        }
        total_diversity_score = this.relevance_model.computeDiversityScoreInRuleSet(this.rees);
        total_score = total_relevance_score + this.lambda * total_diversity_score;
        logger.info("total fitness score: {}", total_fitness_score);
        logger.info("total unexpectedness score: {}", total_unexpectedness_score);
        logger.info("total relevance score: {}", total_relevance_score);
        logger.info("total diversity score: {}", total_diversity_score);
        logger.info("total score: {}", total_score);

        // obtain the optimal relevance score
        ArrayList<Double> all_relevance_scores = new ArrayList<>();
        for (REE ree : this.all_rees) {
            double fitness = this.relevance_model.computeFitnessScore(ree.getCurrentList(), ree.getRHS(), attributesUserInterestedRelatedPredicates);
            double unexpectedness = this.relevance_model.computeUnexpectednessScore(ree.getCurrentList(), ree.getRHS(), rulesUserAlreadyKnown);
            double rel_score = this.w_fitness * fitness + this.w_unexpectedness * unexpectedness;
            all_relevance_scores.add(rel_score);
            logger.info("#### All ree: {}, fitness: {}, unexpectedness: {}, rel_score: {}", ree, fitness, unexpectedness, rel_score);
        }
        Collections.sort(all_relevance_scores, Collections.reverseOrder());
        int idx_temp = 0;
        double relevance_score_k_opt = 0.0;
        for (Double score_temp : all_relevance_scores) {
            if (idx_temp >= this.K) {
                break;
            }
            logger.info("In all rules, Top-{} relevance score: {}", idx_temp, score_temp);
            relevance_score_k_opt += score_temp;
            idx_temp++;
        }
        logger.info("relevance score for the optimal k set: {}", relevance_score_k_opt);

        // compute nRev score
        double nRev = total_relevance_score / relevance_score_k_opt;
        logger.info("nRev: {}", nRev);

        // write statistical information into file
        statisticsInfo.append("rules: ").append("\n");
        for (REE ree : this.rees) {
            statisticsInfo.append(ree.toString()).append("\n");
        }

        statisticsInfo.append("\n");
        statisticsInfo.append("data size: ").append(this.allCount).append("\n");

        statisticsInfo.append("total fitness score: ").append(total_fitness_score).append("\n");
        statisticsInfo.append("total unexpectedness score: ").append(total_unexpectedness_score).append("\n");
        statisticsInfo.append("total relevance score: ").append(total_relevance_score).append("\n");
        statisticsInfo.append("total diversity score: ").append(total_diversity_score).append("\n");
        statisticsInfo.append("total score: ").append(total_score).append("\n\n");

        statisticsInfo.append("relevance score for the optimal k set: ").append(relevance_score_k_opt).append("\n");
        statisticsInfo.append("nRev: ").append(nRev).append("\n\n");

        // compute rCov score
        ArrayList<Double> similarity_thresholds = new ArrayList<>();
        similarity_thresholds.add(0.1);
        similarity_thresholds.add(0.2);
        similarity_thresholds.add(0.3);
        similarity_thresholds.add(0.4);
        similarity_thresholds.add(0.5);
        similarity_thresholds.add(0.6);
        similarity_thresholds.add(0.7);
        similarity_thresholds.add(0.8);
        similarity_thresholds.add(0.9);
        for (double sim_thr : similarity_thresholds) {
            int count = 0;
            for (REE ree_i : this.all_rees) {
                for (REE ree_j : this.rees) {
                    double sim = this.computeJaccardSimilarity(ree_i, ree_j);
                    if (sim > sim_thr) {
                        count++;
                        break;
                    }
                }
            }
            double rCov = count * 1.0 / this.all_rees.size();

            statisticsInfo.append("----------------------").append("\n");
            statisticsInfo.append("Jaccard similarity threshold: ").append(sim_thr).append("\n");
            statisticsInfo.append("rCov: ").append(rCov).append("\n\n");
        }
    }

    public List<Message> run(ArrayList<WorkUnit> workUnits, JavaSparkContext sc,
                             Broadcast<PredicateSetAssist> bcpsAssist, ArrayList<REE> partialSolution) {
        BroadcastObj broadcastObj = new BroadcastObj(this.maxTupleVariableNum, this.maxOneRelationNum,
                this.sampleRatioForTupleCov, this.diversity, partialSolution,
                this.index_null_string, this.index_null_double, this.index_null_long);

        broadcastObj.setValidConstantRule(new HashMap<>());
        Broadcast<BroadcastObj> scInputLight = sc.broadcast(broadcastObj);

        //增加聚类方法聚合Unit
        ArrayList<WorkUnits> unitSets;
        unitSets = new ArrayList<>();
        for (WorkUnit workUnit : workUnits) {
            WorkUnits cluster = new WorkUnits();
            cluster.addUnit(workUnit);
            unitSets.add(cluster);
        }

        for (WorkUnit task : workUnits) {
            task.clearData();
        }

        List<Message> ruleMessages = new ArrayList<>();
        logger.info("running by MultiTuplesRuleMiningOpt!!");

        // set allCount of each work unit
        for(WorkUnits set : unitSets) {
            set.setAllCount(this.allCount);
        }

        List<Message> ruleMessagesSub = sc.parallelize(unitSets, unitSets.size()).map(unitSet -> {
            if (unitSet.getCurrrent().size() == 0) {
                return null;
            }

            PredicateSetAssist assist = bcpsAssist.getValue();
            PredicateSet.indexProvider = assist.getIndexProvider();
            PredicateSet.bf = assist.getBf();
            String taskid = assist.getTaskId();

            BroadcastObj bobj = scInputLight.getValue();
            Map<PredicateSet, List<Predicate>> validConsRuleMap = bobj.getValidConstantRule();
            Map<PredicateSet, Map<String, Predicate>> constantXMap = new HashMap<>();

            PredicateSet sameSet = unitSet.getSameSet();
            for (PredicateSet set : validConsRuleMap.keySet()) {
                PredicateSet tupleX = new PredicateSet();
                Map<String, Predicate> constantX = new HashMap<>();
                for (Predicate p : set) {
                    if (p.isConstant()) {
                        constantX.put(p.getOperand1().toString_(0), p);
                    } else {
                        tupleX.add(p);
                    }
                }
                if (tupleX.size() > 0) {
                    constantXMap.putIfAbsent(tupleX, constantX);
                }
            }

            List<WorkUnit> units = unitSet.getUnits();
            for (PredicateSet tuplePs : constantXMap.keySet()) {
                if (sameSet.size() > 0) {
                    if (!tuplePs.containsPS(sameSet)) {
                        continue;
                    }
                }
                for (WorkUnit unit : units) {
                    PredicateSet tupleX = new PredicateSet();
                    PredicateSet constantX = new PredicateSet();
                    for (Predicate p : unit.getCurrrent()) {
                        if (p.isConstant()) {
                            constantX.add(p);
                        } else {
                            tupleX.add(p);
                        }
                    }
                    if (tupleX.containsPS(tuplePs)) {
                        Map<String, Predicate> constantsMap = constantXMap.get(tuplePs);
                        boolean iscont = true;
                        for (Predicate p : constantX) {
                            if (!constantsMap.containsKey(p.getOperand1().toString_(0))) {
                                iscont = false;
                                break;
                            }
                        }
                        if (iscont) {
                            PredicateSet lhs = new PredicateSet();
                            lhs.addAll(tuplePs);
                            for (Predicate p : constantsMap.values()) {
                                lhs.add(p);
                            }
                            List<Predicate> rhs = validConsRuleMap.get(lhs);

                            for (Predicate p : rhs) {
                                if (unit.getRHSs().containsPredicate(p)) {
                                    unit.getRHSs().remove(p);
                                }
                            }
                        }
                    }
                }
            }

            Predicate pBegin = null;
            for (Predicate p : unitSet.getSameSet()) {
                if (!p.isML() && !p.isConstant()) {
                    pBegin = p;
                    break;
                }
            }

            MultiTuplesRuleMiningOpt multiTuplesRuleMining = new MultiTuplesRuleMiningOpt(bobj.getMax_num_tuples(),
                    bobj.getMaxOneRelationNum(), unitSet.getAllCount(), bobj.getSampleRatioForTupleCov(), bobj.getDiversity(),
                    bobj.getIndex_null_string(), bobj.getIndex_null_double(), bobj.getIndex_null_long());

            List<Message> messages = multiTuplesRuleMining.validationMap1(unitSet, pBegin, bobj.getPartial_solution());

            return messages;

        }).aggregate(null, new IMessageAggFunction(), new IMessageAggFunction());

        ruleMessages.addAll(ruleMessagesSub);

        return ruleMessages;
    }

    Message integrateMessages(List<Message> messages) {
        Message messages_new = messages.get(0);
        for (int i = 1; i < messages.size(); i++) {
            if (messages.get(i).isEmpty()) {
                continue;
            }
            messages_new.mergeMessage(messages.get(i), new ArrayList<>(), this.diversity.getCandidate_functions());
        }
        return messages_new;
    }

    public void generateWorkunitsWithDesignatedPartition(WorkUnit task, ArrayList<WorkUnit> tasks_new) {
        HashMap<String, ArrayList<int[]>> recordComs = new HashMap<>();
        int[] _pids = new int[this.maxTupleVariableNum + 1];
        String[] keys = new String[this.maxTupleVariableNum + 1];
        if (tasks_new.size() > MIN_NUM_WORK_UNITS) {
            return;
        }

        for (int i = 0; i < _pids.length; i++) _pids[i] = 0;

        // collect the number of pids for each tid
        int tupleNum = 0;
        // only consider current, because RHS is <t_0, t_1>
        for (Predicate p : task.getCurrrent()) {
            // index1 : pid
            tupleNum = Math.max(tupleNum, p.getIndex1());
            int numOfPid = p.getOperand1().getColumn().getPliSections().size();
            _pids[p.getIndex1()] = numOfPid;
            keys[p.getIndex1()] = p.getOperand1().getColumn().getTableName();
            tupleNum = Math.max(tupleNum, p.getIndex2());
            _pids[p.getIndex2()] = p.getOperand2().getColumn().getPliSections().size();
            keys[p.getIndex2()] = p.getOperand2().getColumn().getTableName();
        }
        String comKey = "";
        for (int i = 0; i <= tupleNum; i++) comKey += keys[i] + "-";
        if (!recordComs.containsKey(comKey)) {
            // enumerate all pid combinations
            ArrayList<int[]> combinations = new ArrayList<>();
            int[] oneCom = new int[this.maxTupleVariableNum + 1];
            enumCombinations(oneCom, combinations, _pids, 0, tupleNum + 1);
            recordComs.put(comKey, combinations);
        }
        // split work units
        ArrayList<int[]> allCombinations = recordComs.get(comKey);
        for (int[] combination : allCombinations) {
            WorkUnit task_new = new WorkUnit(task, combination);
            task_new.setTransferData();
            tasks_new.add(task_new);
        }
    }

    private void enumCombinations(int[] currentPIDs, ArrayList<int[]> results, int[] _pids, int script, int tupleNum) {
        if (script >= tupleNum) {
            int[] com = currentPIDs.clone();
            results.add(com);
            return;
        }
        for (int i = 0; i < _pids[script]; i++) {
            currentPIDs[script] = i;
            enumCombinations(currentPIDs, results, _pids, script + 1, tupleNum);
        }
    }

    public ArrayList<WorkUnit> generateWorkUnits() {
        ArrayList<WorkUnit> workUnits = new ArrayList<>();

        for (REE ree : this.rees) {
            WorkUnit workUnit = new WorkUnit();
            for (Predicate p : ree.getCurrentList()) {
                workUnit.addCurrent(p);
            }
            workUnit.addRHS(ree.getRHS());
            workUnits.add(workUnit);
        }

        return workUnits;
    }

    // bi-variable REEs, only contains "=" as operator
    public REE parseLine(String line, ArrayList<Predicate> allNonConstantPredicates, ArrayList<Predicate> allConstantPredicates) {
//        logger.info("line: {}", line);
        PredicateSet Xset = new PredicateSet();
        Predicate rhs;

        String[] rule = line.split(":", 2)[1].trim().split("->");
        String precondition = rule[0].trim();
        String consequence = rule[1].trim();
//        logger.info("precondition: {}", precondition);
//        logger.info("consequence: {}", consequence);

        // precondition
        String[] Xset_str = null;
        if (precondition.contains("⋀")) {
            Xset_str = precondition.split("⋀");
        } else if (precondition.contains("^")) {
            Xset_str = precondition.split("\\^");
        } else {
            Xset_str = new String[1];
            Xset_str[0] = precondition;
        }
        for (String pred : Xset_str) {
            Predicate p = this.transformPredicate(pred, allNonConstantPredicates, allConstantPredicates);
            if (p == null) {
                continue;
            }
            Xset.add(p);
//            logger.info("Xset predicate: {}", p);
        }

        // consequence
        String[] info = consequence.split(",");
        rhs = this.transformPredicate(info[0].trim(), allNonConstantPredicates, allConstantPredicates);
//        logger.info("rhs predicate: {}", rhs);

        // other info: supp, conf, rel_score, div_score, score
        HashMap<String, Double> info_map = new HashMap<>();
        for (int i = 1; i < info.length; i++) {
            String[] k_v = info[i].trim().split(":");
            String key = k_v[0].trim();
            Double value = Double.valueOf(k_v[1].trim());
            info_map.put(key, value);
        }
//        logger.info("#### statistics: {}", info_map);

        REE ree = new REE(Xset, rhs);

        return ree;
    }

    public Predicate transformPredicate(String pred, ArrayList<Predicate> allNonConstantPredicates, ArrayList<Predicate> allConstantPredicates) {
//        logger.info("-------------- transformPredicate --------------");
//        logger.info("str_pred: {}", pred);

        String operator = this.obtainOperator(pred);
        if (operator == null) {
            return null;
        }

        String[] infos = pred.trim().split(operator);
        String info1 = infos[0].trim();
        String info2 = infos[1].trim();
//        logger.info("info1: {}", info1);
//        logger.info("info2: {}", info2);

        String[] index_attr1 = info1.split("\\.");

        int index1 = 0;
        String attr1 = "";
        if (index_attr1.length == 2) {
            // ree.toREEString()
            index1 = Integer.parseInt(index_attr1[0].trim().split("t")[1].trim());
            attr1 = index_attr1[1].trim();
        } else if (index_attr1.length == 3) {
            // ree.toString()
            index1 = Integer.parseInt(index_attr1[1].trim().split("t")[1].trim());
            attr1 = index_attr1[2].trim();
        }
//        logger.info("index1: {}, attr1: {}", index1, attr1);

        // toREEString()
        // if (info2.startsWith("t1.")) {
        if (info2.contains("t1.")) {
            String[] index_attr2 = info2.split("\\.");
            int index2 = 1;
            String attr2 = "";
            if (index_attr2.length == 2) {
                // ree.toREEString()
                index2 = Integer.parseInt(index_attr2[0].trim().split("t")[1].trim());
                attr2 = index_attr2[1].trim();
            } else if (index_attr2.length == 3) {
                // ree.toString()
                index2 = Integer.parseInt(index_attr2[1].trim().split("t")[1].trim());
                attr2 = index_attr2[2].trim();
            }
//            logger.info("index2: {}, attr2: {}", index2, attr2);
            for (Predicate p : allNonConstantPredicates) {
                if (p.isConstant()) {
                    continue;
                }
                if (!p.getTableName().toLowerCase().contains(this.data_name.toLowerCase()) && !this.data_name.toLowerCase().contains(p.getTableName().toLowerCase())) { // toLowerCase() is for aminer - AMiner_Author, AMiner_Paper, AMiner_Author2Paper
                    continue;
                }
                if (p.getOperand1().getColumnLight().getName().equals(attr1) &&
                        p.getOperand2().getColumnLight().getName().equals(attr2) &&
                        p.getIndex1() == index1 && p.getIndex2() == index2) {
                    return p;
                }
            }
        } else {
            String constant = info2.trim().replace("'", "");
//            logger.info("constant: {}", constant);
            for (Predicate p : allConstantPredicates) {
                if (!p.isConstant()) {
                    continue;
                }
                if (!p.getTableName().toLowerCase().contains(this.data_name.toLowerCase()) && !this.data_name.toLowerCase().contains(p.getTableName().toLowerCase())) { // toLowerCase is for aminer - AMiner_Author, AMiner_Paper, AMiner_Author2Paper
                    continue;
                }
                if (p.getOperand1().getColumnLight().getName().equals(attr1)  &&
                        p.getConstant().equals(constant) && p.getIndex1() == index1) {
                    return p;
                }
            }
        }
        return null;
    }

    public String obtainOperator(String str) {
        if (str.contains("==")) {
            return "==";
        } else if (str.contains("=")) {
            return "=";
        } else if (str.contains("<>")) {
            return "<>";
        } else if (str.contains(">=")) {
            return ">=";
        } else if (str.contains("<=")) {
            return "<=";
        } else if (str.contains(">")) {
            return ">";
        } else if (str.contains("<")) {
            return "<";
        }
        return null;
    }

    private void prepareAllPredicatesMultiTuples() {
        HashMap<String, ParsedColumnLight<?>> colsMap = new HashMap<>();
        for (Predicate p : this.allPredicates) {
            String k = p.getOperand1().getColumn().toStringData();
            if (!colsMap.containsKey(k)) {
                ParsedColumnLight<?> col = new ParsedColumnLight<>(p.getOperand1().getColumn(), p.getOperand1().getColumn().getType());
                colsMap.put(k, col);
            }
            k = p.getOperand2().getColumn().toStringData();
            if (!colsMap.containsKey(k)) {
                ParsedColumnLight<?> col = new ParsedColumnLight<>(p.getOperand2().getColumn(), p.getOperand2().getColumn().getType());
                colsMap.put(k, col);
            }
        }
        // delete value int data of ParsedColumn
        for (Predicate p : this.allPredicates) {
            p.getOperand1().getColumn().cleanValueIntBeforeBroadCast();
            p.getOperand2().getColumn().cleanValueIntBeforeBroadCast();
        }
        ArrayList<Predicate> newPredicates = new ArrayList<>();
        // insert parsedColumnLight for each predicate
        PredicateSet ps = new PredicateSet();
        for (Predicate p : this.allPredicates) {
            // set columnLight
            String k = p.getOperand1().getColumn().toStringData();
            p.getOperand1().setColumnLight(colsMap.get(k));
            k = p.getOperand2().getColumn().toStringData();
            p.getOperand2().setColumnLight(colsMap.get(k));
            ps.add(p);
            for (int t1 = 0; t1 < maxTupleVariableNum; t1++) {
                if (p.isConstant()) {
                    Predicate p_new = predicateProviderIndex.getPredicate(p, t1, t1);
                    k = p_new.getOperand1().getColumn().toStringData();
                    p_new.getOperand1().setColumnLight(colsMap.get(k));
                    k = p_new.getOperand2().getColumn().toStringData();
                    p_new.getOperand2().setColumnLight(colsMap.get(k));
                    ps.add(p_new);
                    newPredicates.add(p_new);
                    if (!this.allConstantPredicates.contains(p_new)) {
                        this.allConstantPredicates.add(p_new);
                    }
                    continue;
                }
                for (int t2 = t1 + 1; t2 < maxTupleVariableNum; t2++) {
                    Predicate p_new = predicateProviderIndex.getPredicate(p, t1, t2);
                    k = p_new.getOperand1().getColumn().toStringData();
                    p_new.getOperand1().setColumnLight(colsMap.get(k));
                    k = p_new.getOperand2().getColumn().toStringData();
                    p_new.getOperand2().setColumnLight(colsMap.get(k));
                    ps.add(p_new);
                    newPredicates.add(p_new);
                    if (!this.allNonConstantPredicates.contains(p_new)) {
                        this.allNonConstantPredicates.add(p_new);
                    }
                }
            }
        }

        for (Predicate p : newPredicates) {
            if (this.allPredicates.contains(p)) {
                continue;
            }
            this.allPredicates.add(p);
        }
    }

    /*
     * ------------------- for obtaining suboptimal top-k diversified results from the whole rule set -------------------
     * */
    // given a set of rees, compute the tuple coverage
    public Long computeTupleCoverage(ArrayList<REE> rees, SparkSession spark) {
        Diversity diversity_temp = new Diversity("tuple_coverage");

        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        PredicateSetAssist psAssist = new PredicateSetAssist();
        psAssist.setIndexProvider(PredicateSet.indexProvider);//在这里设置，这个值肯定是确定了的
        psAssist.setBf(PredicateSet.bf);
        psAssist.setTaskId(this.data_name);
        Broadcast<PredicateSetAssist> bcpsAssist = sc.broadcast(psAssist);

        long tuple_coverage = 0L;
        for (int i = 0; i < rees.size(); i++) {
            // generate work unit of rees.get(i)
            WorkUnit workUnit = new WorkUnit();
            for (Predicate p : rees.get(i).getCurrentList()) {
                workUnit.addCurrent(p);
            }
            workUnit.addRHS(rees.get(i).getRHS());
            workUnit.setTransferData();

            ArrayList<WorkUnit> workUnits = new ArrayList<>();
            this.generateWorkunitsWithDesignatedPartition(workUnit, workUnits);

            // compute the sum of partialSolution's coverage and rees.get(i)'s marginal coverage w.r.t. partialSolution
            ArrayList<REE> partialSolution = new ArrayList<>();
            for (int j = 0; j < i; j++) {
                partialSolution.add(rees.get(j));
            }

            List<Message> messages = this.run(workUnits, sc, bcpsAssist, partialSolution);
            Message message = this.integrateMessages(messages);  // only contain message for rees.get(i)

            logger.info("#### message.getAllCurrentRHSsSupport().size(): {}", message.getAllCurrentRHSsSupport().size()); // size = 1
            for (Map.Entry<Predicate, Long> entry : message.getAllCurrentRHSsSupport().entrySet()) {
                Predicate rhs = entry.getKey();
                long supportXRHS = entry.getValue();
                double conf = supportXRHS * 1.0 / message.getCurrentSupp();
                this.supports_map.putIfAbsent(rees.get(i), supportXRHS);
                this.confidences_map.putIfAbsent(rees.get(i), conf);
                tuple_coverage += message.getMarginalTupleCoverage(PredicateSet.getIndex(rhs));
            }

            diversity_temp.updateCurrentPartialSolutionDiv(message.getCarriedInfoLight(rees.get(i).getRHS()));
        }

        return tuple_coverage;
    }

    // given a set of partialSolution and a new ree, compute the marginal tuple coverage of ree_new w.r.t. partialSolution
    public Long computeMarginalTupleCoverage(ArrayList<REE> partialSolution, REE ree_new, SparkSession spark) {
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        PredicateSetAssist psAssist = new PredicateSetAssist();
        psAssist.setIndexProvider(PredicateSet.indexProvider);//在这里设置，这个值肯定是确定了的
        psAssist.setBf(PredicateSet.bf);
        psAssist.setTaskId(this.data_name);
        Broadcast<PredicateSetAssist> bcpsAssist = sc.broadcast(psAssist);

        long tuple_coverage = 0L;
        // generate work unit of ree_new
        WorkUnit workUnit = new WorkUnit();
        for (Predicate p : ree_new.getCurrentList()) {
            workUnit.addCurrent(p);
        }
        workUnit.addRHS(ree_new.getRHS());
        workUnit.setTransferData();

        ArrayList<WorkUnit> workUnits = new ArrayList<>();
        this.generateWorkunitsWithDesignatedPartition(workUnit, workUnits);

        // compute the sum of partialSolution's coverage and ree_new's marginal coverage w.r.t. partialSolution
        List<Message> messages = this.run(workUnits, sc, bcpsAssist, partialSolution);
        Message message = this.integrateMessages(messages);  // only contain message for rees.get(i)

//        logger.info("#### message.getAllCurrentRHSsSupport().size(): {}", message.getAllCurrentRHSsSupport().size()); // size = 1
        for (Map.Entry<Predicate, Long> entry : message.getAllCurrentRHSsSupport().entrySet()) {
            Predicate rhs = entry.getKey();
            long supportXRHS = entry.getValue();
            double conf = supportXRHS * 1.0 / message.getCurrentSupp();
            this.supports_map.putIfAbsent(ree_new, supportXRHS);
            this.confidences_map.putIfAbsent(ree_new, conf);
            tuple_coverage = message.getMarginalTupleCoverage(PredicateSet.getIndex(rhs));
        }

        return tuple_coverage;
    }

    public double computeRelevanceScores(ArrayList<REE> rees, int version) {
        double relevance_score = 0.0;
        if (version == 1) {
            for (REE ree : rees) {
                relevance_score += this.w_supp * this.supports_map.get(ree) * 1.0 / this.allCount / this.allCount;
                relevance_score += this.w_conf * this.confidences_map.get(ree);
                relevance_score += this.w_rel_model * this.relevance_model_scores_map.get(ree);
            }
        } else if (version == 2) {
            for (REE ree : rees) {
                relevance_score += this.relevance_scores_map.get(ree);
            }
        }
        return relevance_score;
    }

    public double computeDiversityScores(ArrayList<REE> rees, SparkSession spark, int version) {
        double diversity_score = 0.0;
        if (version == 1) {
            diversity_score += this.w_attr_non * computeAttributeNonoverlap(rees) * 1.0 / MAX_CURRENT_PREDICTES;
            diversity_score += this.w_pred_non * computePredicateNonoverlap(rees) * 1.0 / MAX_CURRENT_PREDICTES;
            diversity_score += this.w_attr_dis * computeAttributeDistance(rees);
            diversity_score += this.w_tuple_cov * computeTupleCoverage(rees, spark) * 1.0 / this.allCount / this.allCount;
        } else if (version == 2) {
            diversity_score = this.relevance_model.computeDiversityScoreInRuleSet(rees);
        }
        return diversity_score;
    }

    public double computeScores(ArrayList<REE> rees, SparkSession spark, int version) {
//        return computeRelevanceScores(rees) + this.lambda * computeDiversityScores(rees, spark);
        return this.lambda * computeDiversityScores(rees, spark, version) + computeRelevanceScores(rees, version); // after computing tuple coverage, we get the results of support and confidence
    }

    public ArrayList<REE> swapping(SparkSession spark, int version) {
        // initially selected k elements randomly
        Random random = new Random();
        HashSet<Integer> initialIndex = new HashSet<>();
        while (initialIndex.size() < this.topKNum) {
            int randomIndex = random.nextInt(this.rees.size());
            initialIndex.add(randomIndex);
        }
        ArrayList<REE> selected = new ArrayList<>();
        for (int idx : initialIndex) {
            selected.add(this.rees.get(idx));
        }
        // begin swapping
        boolean active = true;
        while (active) {
            active = false;
            double score_before_replace = computeScores(selected, spark, version);
            for (REE ree_new : this.rees) {
                if (selected.contains(ree_new)) {
                    continue;
                }
                // check whether replace ree_check with ree_new
                REE ree_to_be_replace = null;
                double max_score_new = Double.NEGATIVE_INFINITY;
                for (REE ree_check : selected) {
                    ArrayList<REE> selected_after_replace = new ArrayList<>();
                    for (REE r_tmp : selected) {
                        if (r_tmp.equals(ree_check)) {
                            continue;
                        }
                        selected_after_replace.add(r_tmp);
                    }
                    selected_after_replace.add(ree_new);
                    double score_after_replace = computeScores(selected_after_replace, spark, version);
                    if (score_after_replace > score_before_replace && score_after_replace > max_score_new) {
                        ree_to_be_replace = ree_check;
                        max_score_new = score_after_replace;
                    }
                }
                // swap
                if (ree_to_be_replace != null) {
                    selected.remove(ree_to_be_replace);
                    selected.add(ree_new);
                    active = true;
                    break;
                }
            }
        }

        return selected;
    }

}
