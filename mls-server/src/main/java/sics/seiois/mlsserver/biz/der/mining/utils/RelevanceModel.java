package sics.seiois.mlsserver.biz.der.mining.utils;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.ejml.simple.SimpleMatrix;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sics.seiois.mlsserver.biz.der.metanome.REE;
import sics.seiois.mlsserver.biz.der.metanome.predicates.Predicate;
import sics.seiois.mlsserver.biz.der.metanome.predicates.sets.PredicateSet;
import spire.algebra.Bool;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;


public class RelevanceModel implements Serializable {
    private static Logger logger = LoggerFactory.getLogger(RelevanceModel.class);

    private SimpleMatrix predicateEmbedMatrix;           // shape (predicate_size, predicate_embedding_size)
    private SimpleMatrix weightPredicates;               // shape (1, predicate_embedding_size)
    private SimpleMatrix weightREEsEmbed;                // shape (predicate_embedding_size * 2, rees_embedding_size)
    private SimpleMatrix weightRelevance;                // shape (ree_embedding_size, 1)
    private SimpleMatrix weightUBSub;                    // shape (1, 1)

    static public int MAX_LHS_PREDICATES = 5; // 10
    static public int MAX_RHS_PREDICATES = 1;
    static public String PADDING_VALUE = "PAD";

    HashMap<String, Integer> predicateToIDs;

    private HashMap<REE, Double> rule_fitness;
    private HashMap<REE, Double> rule_unexpectedness;
    private HashMap<PredicateSet, Double> predicate_pair_distance;
    private HashMap<HashSet<REE>, Double> rule_pair_distance;
    private HashMap<REE, SimpleMatrix> rule_embeddings;

    public RelevanceModel(String predicateToIDFile, String relevanceModelFile, FileSystem hdfs) {
        try {
            this.loadPredicateIDs(predicateToIDFile, hdfs);
            this.loadRelevanceModel(relevanceModelFile, hdfs);
        } catch (Exception e) {

        }
    }

    public RelevanceModel(String predicateToIDFile, String relevanceModelFile, FileSystem hdfs, int version) {
        this.rule_fitness = new HashMap<>();
        this.rule_unexpectedness = new HashMap<>();
        this.predicate_pair_distance = new HashMap<>();
        this.rule_pair_distance = new HashMap<>();
        this.rule_embeddings = new HashMap<>();
        try {
            this.loadPredicateIDs(predicateToIDFile, hdfs);
            this.loadPredicateEmbeddingsAndWeights(relevanceModelFile, hdfs);
        } catch (Exception e) {

        }
    }

    public void loadRelevanceModel(String relevanceModelFile, FileSystem hdfs) throws Exception {
        FSDataInputStream inputTxt = hdfs.open(new Path(relevanceModelFile));
        BufferedInputStream bis = new BufferedInputStream(inputTxt);
        InputStreamReader sReader = new InputStreamReader(bis, "UTF-8");
        BufferedReader bReader = new BufferedReader(sReader);
        ArrayList<SimpleMatrix> matrices = new ArrayList<>();
        String line = null;
        boolean beginMatrix = true;
        while ( (line = bReader.readLine()) != null) {
            if (line.trim().equals("")) {
                continue;
            }
            int row = 0, col = 0;
            if (beginMatrix) {
                String[] info = line.split(" ");
                row = Integer.parseInt(info[0].trim());
                col = Integer.parseInt(info[1].trim());
            }
            double[][] mat = new double[row][col];
            for (int r = 0; r < row; r++) {
                line = bReader.readLine();
                String[] info = line.split(" ");
                for (int c = 0; c < info.length; c++) {
                    mat[r][c] = Double.parseDouble(info[c].trim());
                }
            }
            SimpleMatrix matrix = new SimpleMatrix(mat);
            matrices.add(matrix);
        }

        // assign to matrices
        this.predicateEmbedMatrix = matrices.get(0);
        this.weightPredicates = matrices.get(1);
        this.weightREEsEmbed = matrices.get(2);
        this.weightRelevance = matrices.get(3);
        this.weightUBSub = matrices.get(4);

//        logger.info("predicateEmbedMatrix size: {}*{}", predicateEmbedMatrix.numRows(), predicateEmbedMatrix.numCols());
//        logger.info("weightPredicates size: {}*{}", weightPredicates.numRows(), weightPredicates.numCols());
//        logger.info("weightREEsEmbed size: {}*{}", weightREEsEmbed.numRows(), weightREEsEmbed.numCols());
//        logger.info("weightRelevance size: {}*{}", weightRelevance.numRows(), weightRelevance.numCols());
//        logger.info("weightUBSub size: {}*{}", weightUBSub.numRows(), weightUBSub.numCols());
//
//        logger.info("predicateEmbedMatrix:\n{}", predicateEmbedMatrix);
//        logger.info("weightPredicates:\n{}", weightPredicates);
//        logger.info("weightREEsEmbed:\n{}", weightREEsEmbed);
//        logger.info("weightRelevance:\n{}", weightRelevance);
//        logger.info("weightUBSub:\n{}", weightUBSub);

//        // weights of features are non-negative, old version
//        this.weightUBSubj = this.weightUBSubj.elementMult(this.weightUBSubj);
    }

    // for the diversified-version2
    public void loadPredicateEmbeddingsAndWeights(String predicateEmbeddingsFile, FileSystem hdfs) throws Exception {
        FSDataInputStream inputTxt = hdfs.open(new Path(predicateEmbeddingsFile));
        BufferedInputStream bis = new BufferedInputStream(inputTxt);
        InputStreamReader sReader = new InputStreamReader(bis, "UTF-8");
        BufferedReader bReader = new BufferedReader(sReader);
        ArrayList<SimpleMatrix> matrices = new ArrayList<>();
        String line = null;
        boolean beginMatrix = true;
        while ( (line = bReader.readLine()) != null) {
            if (line.trim().equals("")) {
                continue;
            }
            int row = 0, col = 0;
            if (beginMatrix) {
                String[] info = line.split(" ");
                row = Integer.parseInt(info[0].trim());
                col = Integer.parseInt(info[1].trim());
            }
            double[][] mat = new double[row][col];
            for (int r = 0; r < row; r++) {
                line = bReader.readLine();
                String[] info = line.split(" ");
                for (int c = 0; c < info.length; c++) {
                    mat[r][c] = Double.parseDouble(info[c].trim());
                }
            }
            SimpleMatrix matrix = new SimpleMatrix(mat);
            matrices.add(matrix);
        }

        // assign to matrices
        this.predicateEmbedMatrix = matrices.get(0);
        this.weightPredicates = matrices.get(1);
        this.weightREEsEmbed = matrices.get(2);
        this.weightRelevance = matrices.get(3);

//        logger.info("predicateEmbedMatrix size: {}*{}", predicateEmbedMatrix.numRows(), predicateEmbedMatrix.numCols());
//        logger.info("weightPredicates size: {}*{}", weightPredicates.numRows(), weightPredicates.numCols());
//        logger.info("weightREEsEmbed size: {}*{}", weightREEsEmbed.numRows(), weightREEsEmbed.numCols());
//        logger.info("weightRelevance size: {}*{}", weightRelevance.numRows(), weightRelevance.numCols());
//        logger.info("weightUBSub size: {}*{}", weightUBSub.numRows(), weightUBSub.numCols());
//
//        logger.info("predicateEmbedMatrix:\n{}", predicateEmbedMatrix);
//        logger.info("weightPredicates:\n{}", weightPredicates);
//        logger.info("weightREEsEmbed:\n{}", weightREEsEmbed);
//        logger.info("weightRelevance:\n{}", weightRelevance);
//        logger.info("weightUBSub:\n{}", weightUBSub);

//        // weights of features are non-negative, old version
//        this.weightUBSubj = this.weightUBSubj.elementMult(this.weightUBSubj);
    }

    private void loadPredicateIDs(String predicateToIDFile, FileSystem hdfs) throws Exception {
        FSDataInputStream inputTxt = hdfs.open(new Path(predicateToIDFile));
        BufferedInputStream bis = new BufferedInputStream(inputTxt);
        InputStreamReader sReader = new InputStreamReader(bis, "UTF-8");
        BufferedReader bReader = new BufferedReader(sReader);

        this.predicateToIDs = new HashMap<>();
        String line = null;
        int ID = 0;
//        logger.info("#### predicateToIDs:");
        while ((line = bReader.readLine()) != null) {
            if (line.trim().equals("")) {
                continue;
            }
            String predicate_ = line.trim();
            this.predicateToIDs.put(predicate_, ID);
//            logger.info("#### predicate: {}, ID: {}", predicate_, ID);
            ID++;
        }
    }

    public HashMap<String, Integer> getPredicateToIDs() {
        return predicateToIDs;
    }

    public double predict_score(PredicateSet Xset, Predicate rhs) {
        ArrayList<Predicate> reeLHS = new ArrayList<>();
        for (Predicate p : Xset) {
            reeLHS.add(p);
        }
        ArrayList<Predicate> reeRHS = new ArrayList<>();
        reeRHS.add(rhs);
        int[] ree_lhs = this.transformFeature(reeLHS, MAX_LHS_PREDICATES);
        int[] ree_rhs = this.transformFeature(reeRHS, MAX_RHS_PREDICATES);
        // 1. get embeddings of all predicate embeddings
        ArrayList<SimpleMatrix> predicateEmbedsLHS = this.extractEmbeddings(ree_lhs);
        ArrayList<SimpleMatrix> predicateEmbedsRHS = this.extractEmbeddings(ree_rhs);
        // 2. get embeddings of LHS and RHS
        SimpleMatrix lhsEmbed = this.clauseEmbed(predicateEmbedsLHS);
        SimpleMatrix rhsEmbed = this.clauseEmbed(predicateEmbedsRHS);
        // 3. get embeddings of REE
        SimpleMatrix reeEmbed = this.concatTwoByCol(lhsEmbed, rhsEmbed);
        reeEmbed = this.ReLU(reeEmbed.mult(this.weightREEsEmbed));
        // 4. compute the rule relevance, i.e., the subjective score
        SimpleMatrix subjectiveScore = reeEmbed.mult(this.weightRelevance);
        // 5. compute the rule relevance subjective score with UB
        subjectiveScore = this.weightUBSub.minus(this.ReLU(subjectiveScore));
        return subjectiveScore.get(0, 0);
    }

    private int[] transformFeature(ArrayList<Predicate> selectedPredicates, int max_num_predicates) {
        int[] features = new int[max_num_predicates];
        int count = 0;
        for (Predicate p : selectedPredicates) {
//            logger.info("#### predicate: {}", p.toString().trim());
            features[count++] = this.predicateToIDs.get(p.toString().trim());
        }
        for (int i = count; i < features.length; i++) {
            features[i] = this.predicateToIDs.get(PADDING_VALUE);
        }
        return features;
    }

    private ArrayList<SimpleMatrix> extractEmbeddings(int[] predicateIDs) {
        ArrayList<SimpleMatrix> res = new ArrayList<>();
        for (int i = 0; i < predicateIDs.length; i++) {
            int tid = predicateIDs[i];
            res.add(this.predicateEmbedMatrix.extractVector(true, tid)); // <predicate_num, 1, predicate_embedding_dim>
        }
        return res;
    }

    private SimpleMatrix ReLU(SimpleMatrix input) {
        SimpleMatrix output = new SimpleMatrix(input);
        for (int i = 0; i < output.numRows(); ++i) {
            for (int j = 0; j < output.numCols(); ++j) {
                output.set(i, j, Math.max(0, output.get(i, j)));
            }
        }
        return output;
    }

    /*
    private SimpleMatrix clauseEmbed(ArrayList<SimpleMatrix> embeddingsC) {
        SimpleMatrix res = embeddingsC.get(0).elementMult(this.weightPredicates.extractVector(true, 0));
        for (int i = 1; i < embeddingsC.size(); i++) {
            res = res.plus(embeddingsC.get(i).elementMult(this.weightPredicates.extractVector(true, 0)));
        }
        res = res.divide(embeddingsC.size() * 1.0); // plus & divide -> reduce_mean in Python
        res = this.ReLU(res);
        return res;
    }
    */

    private SimpleMatrix clauseEmbed(ArrayList<SimpleMatrix> embeddingsC) {
        ArrayList<SimpleMatrix> results = new ArrayList<>();
        SimpleMatrix weight_predicate = this.weightPredicates.extractVector(true, 0);
        for (SimpleMatrix pred_embed_mat : embeddingsC) {
            SimpleMatrix res = this.ReLU(pred_embed_mat.elementMult(weight_predicate));
            results.add(res);
        }
        // plus & divide -> reduce_mean in Python
        SimpleMatrix res = results.get(0);
        for (int i = 1; i < results.size(); i++) {
            res = res.plus(results.get(i));
        }
        res = res.divide(results.size() * 1.0);
        return res;

//        SimpleMatrix res = this.ReLU(embeddingsC.get(0).elementMult(this.weightPredicates.extractVector(true, 0)));
//        for (int i = 1; i < embeddingsC.size(); i++) {
//            res = res.plus(this.ReLU(embeddingsC.get(i).elementMult(this.weightPredicates.extractVector(true, 0))));
//        }
//        res = res.divide(embeddingsC.size() * 1.0); // plus & divide -> reduce_mean in Python
//        return res;
    }

    private SimpleMatrix concatTwoByCol(SimpleMatrix one, SimpleMatrix two) {
        int row = one.numRows();
        int col = one.numCols() + two.numCols();
        SimpleMatrix res = new SimpleMatrix(row, col);
        for (int r = 0; r < row; r++) {
            for (int c = 0; c < one.numCols(); c++) {
                res.set(r, c, one.get(r, c));
            }
            for (int c = 0; c < two.numCols(); c++) {
                res.set(r, c + one.numCols(), two.get(r, c));
            }
        }
        return res;
    }

    public double getWeightUBSubj() {
        return this.weightUBSub.get(0, 0);
    }

    public double euclideanDistance(SimpleMatrix vectorA, SimpleMatrix vectorB) {
        // Check if both are vectors and have the same size
//        if (vectorA.numRows() != vectorB.numRows() || vectorA.numCols() != 1 || vectorB.numCols() != 1) {
//            throw new IllegalArgumentException("Both parameters must be vectors of the same size.");
//        }

        // Compute the difference vector
        SimpleMatrix diff = vectorA.minus(vectorB);

        // Calculate the squared sum of differences
        double sumSquaredDiffs = diff.dot(diff);

        // Return the square root of the sum of squared differences
        return Math.sqrt(sumSquaredDiffs);
    }

    public double cosineDistance(SimpleMatrix vectorA, SimpleMatrix vectorB) {
        // Check if both are vectors and have the same size
//        if (vectorA.numRows() != vectorB.numRows() || vectorA.numCols() != 1 || vectorB.numCols() != 1) {
//            throw new IllegalArgumentException("Both parameters must be vectors of the same size.");
//        }

        // Calculate the dot product of the two vectors
        double dotProduct = vectorA.dot(vectorB);

        // Calculate the magnitudes of each vector
        double magnitudeA = vectorA.normF();
        double magnitudeB = vectorB.normF();

        // Compute the cosine similarity
        double cosineSimilarity = dotProduct / (magnitudeA * magnitudeB);

        // Calculate cosine distance
        return 1.0 - cosineSimilarity;
    }

    public SimpleMatrix obtainRuleEmbedding(REE ree) {
        if (this.rule_embeddings.containsKey(ree)) {
            return this.rule_embeddings.get(ree);
        }
        ArrayList<Predicate> reeLHS = new ArrayList<>();
        for (Predicate p : ree.getCurrentList()) {
            reeLHS.add(p);
        }
        ArrayList<Predicate> reeRHS = new ArrayList<>();
        reeRHS.add(ree.getRHS());
        int[] ree_lhs = this.transformFeature(reeLHS, MAX_LHS_PREDICATES);
        int[] ree_rhs = this.transformFeature(reeRHS, MAX_RHS_PREDICATES);
        // 1. get embeddings of all predicate embeddings
        ArrayList<SimpleMatrix> predicateEmbedsLHS = this.extractEmbeddings(ree_lhs);
        ArrayList<SimpleMatrix> predicateEmbedsRHS = this.extractEmbeddings(ree_rhs);
        // 2. get embeddings of LHS and RHS
        SimpleMatrix lhsEmbed = this.clauseEmbed(predicateEmbedsLHS);
        SimpleMatrix rhsEmbed = this.clauseEmbed(predicateEmbedsRHS);
        // 3. get embeddings of REE
        SimpleMatrix reeEmbed = this.concatTwoByCol(lhsEmbed, rhsEmbed);
        // update rule_embeddings map
        this.rule_embeddings.put(ree, reeEmbed);
        return reeEmbed;
    }

    public double computeFitnessScore(PredicateSet Xset, Predicate rhs, ArrayList<Predicate> attributesUserInterested) {
        REE ree = new REE(Xset, rhs);
        if (this.rule_fitness.containsKey(ree)) {
            return this.rule_fitness.get(ree);
        }

        double min_max_corr = Double.POSITIVE_INFINITY;
        PredicateSet X_Y = new PredicateSet(Xset);
        X_Y.add(rhs);
        for (Predicate p_i : X_Y) {
            double max_corr = Double.NEGATIVE_INFINITY;
            for (Predicate p_j : attributesUserInterested) {
                PredicateSet ps = new PredicateSet(p_i);
                ps.add(p_j);
                double corr;
                if (this.predicate_pair_distance.containsKey(ps)) {
                    double dis = this.predicate_pair_distance.get(ps);
//                    corr = 1 - dis;
                    corr = 1.0 / (1 + dis);
                } else {
                    SimpleMatrix embedding_p_i = this.predicateEmbedMatrix.extractVector(true, this.predicateToIDs.get(p_i.toString().trim()));
                    SimpleMatrix embedding_p_j = this.predicateEmbedMatrix.extractVector(true, this.predicateToIDs.get(p_j.toString().trim()));
                    double dis = this.euclideanDistance(embedding_p_i, embedding_p_j);
                    this.predicate_pair_distance.put(ps, dis);
//                    corr = 1 - dis;
                    corr = 1.0 / (1 + dis);
                }
                if (corr > max_corr) {
                    max_corr = corr;
                }
            }
            if (max_corr < min_max_corr) {
                min_max_corr = max_corr;
            }
        }
        this.rule_fitness.put(ree, min_max_corr);
        return min_max_corr;
    }

    public double computeUnexpectednessScore(PredicateSet Xset, Predicate rhs, ArrayList<REE> rulesUserAlreadyKnown) {
        REE ree_new = new REE(Xset, rhs);
        if (this.rule_unexpectedness.containsKey(ree_new)) {
            return this.rule_unexpectedness.get(ree_new);
        }

        double total_corr = 0.0;
        SimpleMatrix ree_new_embed = this.obtainRuleEmbedding(ree_new);
        for (REE ree : rulesUserAlreadyKnown) {
            SimpleMatrix ree_embed = this.obtainRuleEmbedding(ree);
            HashSet<REE> temp = new HashSet<>();
            temp.add(ree_new);
            temp.add(ree);
            double dis;
            if (this.rule_pair_distance.containsKey(temp)) {
                dis = this.rule_pair_distance.get(temp);
            } else {
                dis = this.euclideanDistance(ree_new_embed, ree_embed);
                this.rule_pair_distance.put(temp, dis);
            }
//            total_corr += 1 - dis;
            total_corr += 1.0 / (1 + dis);
        }
        total_corr = total_corr / rulesUserAlreadyKnown.size();
        this.rule_unexpectedness.put(ree_new, total_corr);
        return total_corr;
    }

    public double computeRulePairDistance(REE ree_1, REE ree_2) {
        SimpleMatrix ree_embed_1 = this.obtainRuleEmbedding(ree_1);
        SimpleMatrix ree_embed_2 = this.obtainRuleEmbedding(ree_2);
        HashSet<REE> temp = new HashSet<>();
        temp.add(ree_1);
        temp.add(ree_2);
        if (this.rule_pair_distance.containsKey(temp)) {
            return this.rule_pair_distance.get(temp);
        } else {
            double dis = this.euclideanDistance(ree_embed_1, ree_embed_2);
            this.rule_pair_distance.put(temp, dis);
            return dis;
        }
    }

    public double computeDiversityScoreInRuleSet(ArrayList<REE> ruleSet) {
        double score = 0.0;
        for (int i = 0; i < ruleSet.size()-1; i++) {
            for (int j = i+1; j < ruleSet.size(); j++) {
                score += this.computeRulePairDistance(ruleSet.get(i), ruleSet.get(j));
            }
        }
        int total_size = ruleSet.size() * (ruleSet.size() - 1) / 2;
        score /= total_size;
        return score;
    }

    // compute the marginal distance when remove ree_min from ruleSet and add ree_new into ruleSet
    public double computeMarginalRuleDistances(ArrayList<REE> ruleSet, REE ree_min, REE ree_new) {
        double total_dis_new = 0.0;
        double total_dis_min = 0.0;
        SimpleMatrix ree_new_embed = this.obtainRuleEmbedding(ree_new);
        SimpleMatrix ree_min_embed = this.obtainRuleEmbedding(ree_min);
        for (REE ree : ruleSet) {
            SimpleMatrix ree_embed = this.obtainRuleEmbedding(ree);
            // ree_new
            HashSet<REE> temp_new = new HashSet<>();
            temp_new.add(ree_new);
            temp_new.add(ree);
            double dis_new;
            if (this.rule_pair_distance.containsKey(temp_new)) {
                dis_new = this.rule_pair_distance.get(temp_new);
            } else {
                dis_new = this.euclideanDistance(ree_new_embed, ree_embed);
                this.rule_pair_distance.put(temp_new, dis_new);
            }
            total_dis_new += dis_new;
            // ree_min
            HashSet<REE> temp_min = new HashSet<>();
            temp_min.add(ree_min);
            temp_min.add(ree);
            double dis_min;
            if (this.rule_pair_distance.containsKey(temp_min)) {
                dis_min = this.rule_pair_distance.get(temp_min);
            } else {
                dis_min = this.euclideanDistance(ree_min_embed, ree_embed);
                this.rule_pair_distance.put(temp_min, dis_min);
            }
            total_dis_min += dis_min;
        }
        total_dis_new = total_dis_new / ruleSet.size();
        total_dis_min = total_dis_min / ruleSet.size();
        double marginal_dis = total_dis_new - total_dis_min;

        return marginal_dis;
    }

    public REE obtainMinREE(ArrayList<REE> ruleSet, double lambda, boolean ifMin) {
        REE ree_min = null;
        REE ree_max = null;
        double score_min = Double.POSITIVE_INFINITY;
        double score_max = Double.NEGATIVE_INFINITY;
        for (REE ree_i : ruleSet) {
            SimpleMatrix ree_embed_i = this.obtainRuleEmbedding(ree_i);
            double score = 0.0;
            for (REE ree_j : ruleSet) {
                SimpleMatrix ree_embed_j = this.obtainRuleEmbedding(ree_j);
                score += ree_i.getRelevance_score();
                score += ree_j.getRelevance_score();
                HashSet<REE> temp = new HashSet<>();
                temp.add(ree_i);
                temp.add(ree_j);
                double dis;
                if (this.rule_pair_distance.containsKey(temp)) {
                    dis = this.rule_pair_distance.get(temp);
                } else {
                    dis = this.euclideanDistance(ree_embed_i, ree_embed_j);
                    this.rule_pair_distance.put(temp, dis);
                }
                score += lambda * dis;
            }
            if (score < score_min) {
                score_min = score;
                ree_min = ree_i;
            }
            if (score > score_max) {
                score_max = score;
                ree_max = ree_i;
            }
        }
        if (ifMin) {
            return ree_min;
        } else {
            return ree_max;
        }
    }

    public ArrayList<Double> computeRationalityScore(PredicateSet Xset, Predicate rhs) {
        double max_corr = Double.NEGATIVE_INFINITY;
        double min_corr = Double.POSITIVE_INFINITY;
        SimpleMatrix embedding_rhs = this.predicateEmbedMatrix.extractVector(true, this.predicateToIDs.get(rhs.toString().trim()));
        for (Predicate p : Xset) {
            PredicateSet ps = new PredicateSet(p);
            ps.add(rhs);
            double corr;
            if (this.predicate_pair_distance.containsKey(ps)) {
                corr = 1 - this.predicate_pair_distance.get(ps);
            } else {
                SimpleMatrix embedding_p = this.predicateEmbedMatrix.extractVector(true, this.predicateToIDs.get(p.toString().trim()));
                double dis = this.euclideanDistance(embedding_p, embedding_rhs);
                this.predicate_pair_distance.put(ps, dis);
//                corr = 1 - dis;
                corr = 1.0 / (1 + dis);
            }
            if (corr > max_corr) {
                max_corr = corr;
            }
            if (corr < min_corr) {
                min_corr = corr;
            }
        }
        ArrayList<Double> rationality = new ArrayList<>();
        rationality.add(min_corr);
        rationality.add(max_corr);
        return rationality;
    }

}
