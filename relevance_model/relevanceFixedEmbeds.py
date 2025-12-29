import numpy as np
import time
from parameters import *
from parameters import __eval__, MAX_LHS_PREDICATES, MAX_RHS_PREDICATES
from REEs_repr import REEsRepr
from tensorflow.keras.models import Model
from tensorflow.keras.layers import Lambda, Dense, Input, Flatten, Dropout, Conv2D, MaxPooling2D, Conv1D, BatchNormalization, ReLU, Reshape
from tensorflow.keras.optimizers import RMSprop
import tensorflow.compat.v1 as tf
tf.disable_v2_behavior()


class RelevanceEmbeds(object):
    def __init__(self,
                 all_predicate_size,
                 predicate_embedding_size,
                 hidden_size,
                 rees_embedding_size,
                 max_predicates_lhs,
                 max_predicates_rhs,
                 lr,
                 epochs,
                 batch_size,
                 pretrained_matrix,
                 margin,
                 epochs_pre_train,
                 lr_pre_train):
        # setup rule representation
        self.reesRepr = REEsRepr(all_predicate_size,
                                 predicate_embedding_size,
                                 hidden_size,
                                 rees_embedding_size,
                                 max_predicates_lhs,
                                 max_predicates_rhs,
                                 pretrained_matrix)

        # relevance weights
        self.weight_relevance = tf.Variable(tf.random_normal([rees_embedding_size, 1]), trainable=True)
        self.learning_rate = lr
        self.epochs = epochs
        self.batch_size = batch_size
        self.all_predicate_size = all_predicate_size
        self.predicate_embedding_size = predicate_embedding_size
        self.rees_embedding_size = rees_embedding_size
        self.margin = margin
        self.epochs_pre_train = epochs_pre_train
        self.lr_pre_train = lr_pre_train

        # construct the model
        self.pre_training_construct()
        self.construct()
        # initialize all variables
        init_op = tf.global_variables_initializer()
        self.saver = tf.train.Saver()
        # open a session and run the training graph
        session_config = tf.ConfigProto(log_device_placement=True)
        session_config.gpu_options.allow_growth = True

        self.sess = tf.Session(config=session_config)

        self.saver = tf.train.Saver()
        self.sess.run(init_op)

    def getAllPredicateEmbeddings(self):
        predicate_ph = tf.placeholder(dtype=tf.int32, shape=[1, self.all_predicate_size], name='all_predicates_input')
        pEmbeddings = self.reesRepr.getPredicatesEmbeddings(predicate_ph)[0]
        dummy_input = np.array([[e for e in range(self.all_predicate_size)]])
        w1 = self.sess.run([pEmbeddings], feed_dict={predicate_ph: dummy_input})
        w1 = np.array(w1)
        return w1

    def saveOneMatrix(self, fout, matrix):
        if len(matrix.shape) <= 1:
            fout.write(str(matrix.shape[0]))
        else:
            fout.write(str(matrix.shape[0]) + " " + str(matrix.shape[1]))
        fout.write("\n")
        for i in range(matrix.shape[0]):
            fout.write(' '.join([str(e) for e in matrix[i]]))
            fout.write("\n")
        fout.write("\n")

    def saveModelToText(self, model_txt_path):
        predicate_ph = tf.placeholder(dtype=tf.int32, shape=[1, self.all_predicate_size], name='all_predicates_input')
        pEmbeddings = self.reesRepr.getPredicatesEmbeddings(predicate_ph)[0]
        w2, w3 = self.reesRepr.extractParameters(self.sess)
        dummy_input = np.array([[e for e in range(self.all_predicate_size)]])
        # w1, w4, w6 = self.sess.run([pEmbeddings, self.weight_relevance, self.weight_ub_sub], feed_dict={predicate_ph: dummy_input})
        w1, w4 = self.sess.run([pEmbeddings, self.weight_relevance], feed_dict={predicate_ph: dummy_input})
        w1 = np.array(w1)
        w2 = np.array(w2)
        w3 = np.array(w3)
        w4 = np.array(w4)
        # w6 = np.array(w6)
        # print(w1.shape, w2.shape, w3.shape, w4.shape, w6.shape)
        print(w1.shape, w2.shape, w3.shape, w4.shape)
        f = open(model_txt_path, 'w')
        self.saveOneMatrix(f, np.array(w1))        
        self.saveOneMatrix(f, np.array(w2))
        self.saveOneMatrix(f, np.array(w3))
        self.saveOneMatrix(f, np.array(w4))
        # self.saveOneMatrix(f, np.array(w6))
        f.close()

    def saveModel(self, model_path):
        self.saver.save(self.sess, model_path)

    def loadModel(self, model_path):
        self.saver.restore(self.sess, model_path)

    def predicate_relevance_scores(self, rees_lhs, rees_rhs):
        relevance_scores = []
        for idx in range(len(rees_lhs)):
            feed_dict_predict = {
                self.lhs_vec_ph_left: np.array([rees_lhs[idx]], 'int'),
                self.rhs_vec_ph_left: np.array([rees_rhs[idx]], 'int'),
            }
            # print("feed_dict_predict:", feed_dict_predict)
            rel_score = self.sess.run(self.subjective_left, feed_dict=feed_dict_predict)
            relevance_scores.append(rel_score)
        return relevance_scores

    def inference_classification(self, relevance_1, relevance_2):
        relevance_logits = tf.concat([relevance_1, relevance_2], 1)
        predictions = tf.nn.softmax(relevance_logits)
        return relevance_logits, predictions

    def loss_compute(self, predictions, GT):
        # loss = tf.reduce_sum(tf.losses.mean_squared_error(labels=GT,predictions=prediction))
        # loss
        # cross_entropy = tf.reduce_mean(-tf.reduce_sum(GT * tf.log(predictions), reduction_indices=[1]))
        cross_entropy = tf.reduce_mean(tf.nn.softmax_cross_entropy_with_logits(logits=predictions, labels=GT))
        return cross_entropy

    def contrastive_loss(self, label, distance):
        square_pred = tf.square(distance)
        margin_square = tf.square(tf.maximum(self.margin - distance, 0))
        return tf.reduce_mean(label * square_pred + (1 - label) * margin_square)

    # def compute_loss(self, label, distance):
    #     contrastive_loss = self.contrastive_loss(label, distance)
    #     print("contrastive_loss: ", contrastive_loss)
    #     self.predictions = tf.cast(distance < self.threshold, dtype=tf.float32)
    #     classification_loss = tf.reduce_mean(tf.keras.losses.binary_crossentropy(label, self.predictions))
    #
    #     print("classification_loss: ", classification_loss)
    #     total_loss = contrastive_loss + classification_loss
    #     print("total_loss: ", total_loss)
    #     return total_loss

    def create_embedding_network(self):
        # Define input tensor
        input_tensor = Input(shape=(self.rees_embedding_size,))
        x = Reshape((self.rees_embedding_size, 1))(input_tensor)

        x = Conv1D(filters=64, kernel_size=3, padding='same')(x)
        x = BatchNormalization()(x)
        x = ReLU()(x)
        x = Conv1D(filters=64, kernel_size=3, padding='same')(x)
        x = BatchNormalization()(x)
        x = ReLU()(x)
        x = Conv1D(filters=1, kernel_size=3, padding='same')(x)
        x = BatchNormalization()(x)
        x = ReLU()(x)
        x = Lambda(lambda z: z[..., 0])(x)  # Reduce the last dimension
        return Model(inputs=input_tensor, outputs=x)

    def build_model(self):
        # Input layers
        ree_embed_left = Input(shape=(self.rees_embedding_size,), name="left_input")
        ree_embed_right = Input(shape=(self.rees_embedding_size,), name="right_input")

        # Instantiate embedding model
        embedding_network = self.create_embedding_network()

        # Process both inputs using the same model
        process_embedding_left = embedding_network(ree_embed_left)
        process_embedding_right = embedding_network(ree_embed_right)

        # Calculating the Euclidean distance
        distance = Lambda(self.euclidean_distance)([process_embedding_left, process_embedding_right])

        # Create and return the model
        return Model(inputs=[ree_embed_left, ree_embed_right], outputs=distance)

    def euclidean_distance(self, vectors):
        x, y = vectors
        sum_square = tf.reduce_sum(tf.square(x - y), axis=1, keepdims=True)
        return tf.sqrt(tf.maximum(sum_square, tf.keras.backend.epsilon()))

    def pre_training_construct(self): #, rees_lhs, rees_rhs,
                               # train_pair_ids, train_labels_vec,
                               # valid_pair_ids, valid_labels_vec,
                               # test_pair_ids, test_labels_vec):
        self.lhs_vec_ph_left_pre_train = tf.placeholder(dtype=tf.int32, shape=[None, MAX_LHS_PREDICATES], name='LHS_vec_ph_left_train')
        self.rhs_vec_ph_left_pre_train = tf.placeholder(dtype=tf.int32, shape=[None, MAX_RHS_PREDICATES], name='RHS_vec_ph_left_train')
        self.lhs_vec_ph_right_pre_train = tf.placeholder(dtype=tf.int32, shape=[None, MAX_LHS_PREDICATES], name='LHS_vec_ph_rightv')
        self.rhs_vec_ph_right_pre_train = tf.placeholder(dtype=tf.int32, shape=[None, MAX_RHS_PREDICATES], name='RHS_vec_ph_right_train')
        self.label_ph_pre_train = tf.placeholder(dtype=tf.float32, shape=[None, 1], name='label')

        ree_embed_left = self.reesRepr.encode(self.lhs_vec_ph_left_pre_train, self.rhs_vec_ph_left_pre_train)
        ree_embed_right = self.reesRepr.encode(self.lhs_vec_ph_right_pre_train, self.rhs_vec_ph_right_pre_train)

        embedding_network = self.create_embedding_network()
        process_embedding_left = embedding_network(ree_embed_left)
        process_embedding_right = embedding_network(ree_embed_right)

        self.pre_train_distance = Lambda(self.euclidean_distance)([process_embedding_left, process_embedding_right])
        self.pre_train_loss = self.contrastive_loss(self.label_ph_pre_train, self.pre_train_distance)

        # self.pre_train_model = Model(inputs=[ree_embed_left, ree_embed_right], outputs=distance)

        # self.pre_train_model.compile(loss=self.contrastive_loss, optimizer=RMSprop(), metrics=['accuracy'])

        self.pre_train_optimizer = tf.train.AdamOptimizer(learning_rate=self.lr_pre_train)
        # self.pre_train_optimizer = tf.keras.optimizers.Adam(learning_rate=self.learning_rate)
        self.pre_train_train_op = self.pre_train_optimizer.minimize(self.pre_train_loss)

        '''
        model = self.build_model()
        model.compile(loss=self.contrastive_loss, optimizer=RMSprop(), metrics=['accuracy'])
        # generate validation data
        valid_batch_lhs_left = np.array([rees_lhs[e[0]] for e in valid_pair_ids], 'int')
        valid_batch_rhs_left = np.array([rees_rhs[e[0]] for e in valid_pair_ids], 'int')
        valid_batch_lhs_right = np.array([rees_lhs[e[1]] for e in valid_pair_ids], 'int')
        valid_batch_rhs_right = np.array([rees_rhs[e[1]] for e in valid_pair_ids], 'int')
        ree_embed_left_valid = self.reesRepr.encode(valid_batch_lhs_left, valid_batch_rhs_left)
        ree_embed_right_valid = self.reesRepr.encode(valid_batch_lhs_right, valid_batch_rhs_right)

        # generate testing data
        test_batch_lhs_left = np.array([rees_lhs[e[0]] for e in test_pair_ids], 'int')
        test_batch_rhs_left = np.array([rees_rhs[e[0]] for e in test_pair_ids], 'int')
        test_batch_lhs_right = np.array([rees_lhs[e[1]] for e in test_pair_ids], 'int')
        test_batch_rhs_right = np.array([rees_rhs[e[1]] for e in test_pair_ids], 'int')
        ree_embed_left_test = self.reesRepr.encode(test_batch_lhs_left, test_batch_rhs_left)
        ree_embed_right_test = self.reesRepr.encode(test_batch_lhs_right, test_batch_rhs_right)

        print('start pre-training...')
        for epoch in range(self.epochs):
            num_batch = len(train_pair_ids) // self.batch_size + 1
            for batch_id in range(num_batch):
                # training data
                batch_lhs_left, batch_rhs_left, batch_lhs_right, batch_rhs_right, train_batch_labels_vec = self.generate_batch(
                    batch_id,
                    self.batch_size, rees_lhs, rees_rhs, train_pair_ids, train_labels_vec
                )
                ree_embed_left_train = self.reesRepr.encode(batch_lhs_left, batch_rhs_left)
                ree_embed_right_train = self.reesRepr.encode(batch_lhs_right, batch_rhs_right)

                model.fit([ree_embed_left_train, ree_embed_right_train], train_batch_labels_vec,
                          epochs=10,
                          batch_size=128,
                          steps_per_epoch=100,
                          validation_data=([ree_embed_left_valid, ree_embed_right_valid], valid_labels_vec))

        results = model.evaluate([ree_embed_left_test, ree_embed_right_test], test_labels_vec)
        print(f"Test Loss: {results[0]}")
        print(f"Test Accuracy: {results[1]}")
        '''

    def construct(self):
        # token ids of tokeVob
        self.lhs_vec_ph_left = tf.placeholder(dtype=tf.int32, shape=[None, MAX_LHS_PREDICATES], name='LHS_vec_ph_left')
        self.rhs_vec_ph_left = tf.placeholder(dtype=tf.int32, shape=[None, MAX_RHS_PREDICATES], name='RHS_vec_ph_left')
        self.lhs_vec_ph_right = tf.placeholder(dtype=tf.int32, shape=[None, MAX_LHS_PREDICATES], name='LHS_vec_ph_right')
        self.rhs_vec_ph_right = tf.placeholder(dtype=tf.int32, shape=[None, MAX_RHS_PREDICATES], name='RHS_vec_ph_right')
        self.label_ph = tf.placeholder(dtype=tf.float32, shape=[None, 2], name='label')

        # construct the rule relevance model
        ree_embed_left = self.reesRepr.encode(self.lhs_vec_ph_left, self.rhs_vec_ph_left)
        ree_embed_right = self.reesRepr.encode(self.lhs_vec_ph_right, self.rhs_vec_ph_right)

        self.subjective_left = tf.nn.relu(tf.matmul(ree_embed_left, self.weight_relevance))
        self.subjective_right = tf.nn.relu(tf.matmul(ree_embed_right, self.weight_relevance))

        # predictions
        self.logits, self.predictions = self.inference_classification(self.subjective_left, self.subjective_right)
        self.loss = self.loss_compute(self.predictions, self.label_ph)
        self.optimizer = tf.train.AdamOptimizer(learning_rate=self.learning_rate)
        #self.optimizer = tf.keras.optimizers.Adam(learning_rate=self.learning_rate)
        self.train_op = self.optimizer.minimize(self.loss)
        # accuracy
        correct_predictions = tf.equal(tf.argmax(self.predictions, 1), tf.argmax(self.label_ph, 1))
        self.accuracy_tf = tf.reduce_mean(tf.cast(correct_predictions, 'float'))

    def generate_batch(self, batch_id, batch_size, rees_lhs, rees_rhs, train_pair_ids, train_labels):
        train_num = len(train_pair_ids)
        start_id = batch_size * batch_id
        end_id = batch_size * (batch_id + 1)
        batch_train_pair_ids = train_pair_ids[start_id: end_id]
        batch_train_labels = train_labels[start_id: end_id]
        # generate real training data
        batch_lhs_left = [rees_lhs[e[0]] for e in batch_train_pair_ids]
        batch_rhs_left = [rees_rhs[e[0]] for e in batch_train_pair_ids]
        batch_lhs_right = [rees_lhs[e[1]] for e in batch_train_pair_ids]
        batch_rhs_right = [rees_rhs[e[1]] for e in batch_train_pair_ids]
        return np.array(batch_lhs_left, 'int'), np.array(batch_rhs_left, 'int'), \
                np.array(batch_lhs_right, 'int'), np.array(batch_rhs_right, 'int'), batch_train_labels

    def compute_relevance(self, rees_lhs, rees_rhs):
        num_batch = len(rees_lhs) // self.batch_size + 1
        relevance_values = []
        for batch_id in range(num_batch):
            # fetch data
            start_id, end_id = self.batch_size * batch_id, self.batch_size * (batch_id + 1)
            batch_rees_lhs, batch_rees_rhs = rees_lhs[start_id: end_id], rees_rhs[start_id: end_id]
            feed_dict_interest = {self.lhs_vec_ph_left: batch_rees_lhs,
                                  self.rhs_vec_ph_left: batch_rees_rhs}
            batch_relevance = self.sess.run(self.relevance_left, feed_dict_interest)
            if len(batch_relevance) > 0:
                relevance_values += list(np.hstack(batch_relevance))
        return relevance_values[:len(rees_lhs)]

    def evaluate(self, rees_lhs, rees_rhs, test_pair_ids, test_labels):
        start_time = time.time()
        test_lhs_left, test_rhs_left, test_lhs_right, test_rhs_right, test_labels_ = self.generate_batch(
            0, len(rees_lhs), rees_lhs, rees_rhs, test_pair_ids, test_labels
        )
        feed_dict_test = {self.lhs_vec_ph_left: test_lhs_left,
                          self.rhs_vec_ph_left: test_rhs_left,
                          self.lhs_vec_ph_right: test_lhs_right,
                          self.rhs_vec_ph_right: test_rhs_right
        }
        test_predictions = self.sess.run(self.predictions, feed_dict=feed_dict_test)
        predict_time = time.time() - start_time
        test_measurements = __eval__(np.argmax(test_predictions, 1), np.argmax(test_labels_, 1))
        test_log = f'test_acc: {test_measurements[0]}, test_recall: {test_measurements[1]}, ' \
                    f'test_precision: {test_measurements[2]}, test_f1: {test_measurements[3]}, test_time: {predict_time} '
        print(test_log)
        return test_log

    def train(self, rees_lhs, rees_rhs, train_pair_ids, train_labels, valid_pair_ids, valid_labels):
        print('start training...')

        start_total = 0  # time.time()
        for epoch in range(self.epochs):
            start_train = time.time()
            # Generate Training Batch
            num_batch = len(train_pair_ids) // self.batch_size + 1
            for batch_id in range(num_batch):
                batch_lhs_left, batch_rhs_left, batch_lhs_right, batch_rhs_right, train_batch_labels = self.generate_batch(
                    batch_id,
                    self.batch_size, rees_lhs, rees_rhs, train_pair_ids, train_labels)
                feed_dict_train = {self.lhs_vec_ph_left: batch_lhs_left,
                                   self.rhs_vec_ph_left: batch_rhs_left,
                                   self.lhs_vec_ph_right: batch_lhs_right,
                                   self.rhs_vec_ph_right: batch_rhs_right,
                                   self.label_ph: train_batch_labels}

                _, batch_train_predictions, loss_epoch = self.sess.run([self.train_op, self.predictions, self.loss], feed_dict=feed_dict_train)
                end_train = time.time()
                start_total += end_train - start_train
                # print(f'epoch {epoch}, mean batch loss: {loss_epoch}, acc: {measurements[0]}, recall: {measurements[1]}, precision: {measurements[2]}, f1: {measurements[3]}, time cost: {end_train - start_train}')
                train_measurements = __eval__(np.argmax(batch_train_predictions, 1), np.argmax(train_batch_labels, 1))
                log = f'epoch {epoch}, train_loss: {loss_epoch}, train_acc: {train_measurements[0]}, train_recall: {train_measurements[1]}, ' \
                      f'train_precision: {train_measurements[2]}, train_f1: {train_measurements[3]}, time: {end_train - start_train} '
                print(log)

            valid_batch_lhs_left, valid_batch_rhs_left, valid_batch_lhs_right, valid_batch_rhs_right, valid_batch_labels = self.generate_batch(
                    0, len(valid_pair_ids), rees_lhs, rees_rhs, valid_pair_ids, valid_labels)
            feed_dict_valid = {self.lhs_vec_ph_left: valid_batch_lhs_left,
                                   self.rhs_vec_ph_left: valid_batch_rhs_left,
                                   self.lhs_vec_ph_right: valid_batch_lhs_right,
                                   self.rhs_vec_ph_right: valid_batch_rhs_right,
                                   self.label_ph: valid_batch_labels}
            valid_predictions = self.sess.run(self.predictions, feed_dict=feed_dict_valid)
            valid_measurements = __eval__(np.argmax(valid_predictions, 1), np.argmax(valid_batch_labels, 1))
            valid_log = f'epoch {epoch}, valid_acc: {valid_measurements[0]}, valid_recall: {valid_measurements[1]}, ' \
                    f'valid_precision: {valid_measurements[2]}, valid_f1: {valid_measurements[3]} '
            print(valid_log)

    def pre_training_train(self, rees_lhs, rees_rhs, train_pair_ids, train_labels_pre_train, valid_pair_ids, valid_labels_pre_train):
        print('start pre-training...')

        start_total = 0
        for epoch in range(self.epochs_pre_train):
            start_train = time.time()
            # Generate Training Batch
            num_batch = len(train_pair_ids) // self.batch_size + 1
            for batch_id in range(num_batch):
                batch_lhs_left, batch_rhs_left, batch_lhs_right, batch_rhs_right, train_batch_labels = self.generate_batch(
                    batch_id,
                    self.batch_size, rees_lhs, rees_rhs, train_pair_ids, train_labels_pre_train
                )
                feed_dict_train = {self.lhs_vec_ph_left_pre_train: batch_lhs_left,
                                   self.rhs_vec_ph_left_pre_train: batch_rhs_left,
                                   self.lhs_vec_ph_right_pre_train: batch_lhs_right,
                                   self.rhs_vec_ph_right_pre_train: batch_rhs_right,
                                   self.label_ph_pre_train: train_batch_labels}

                _, batch_pre_train_distance, pre_train_loss_epoch = self.sess.run([self.pre_train_train_op, self.pre_train_distance, self.pre_train_loss],
                                                                                  feed_dict=feed_dict_train)
                end_train = time.time()
                start_total += end_train - start_train
                log = f'epoch {epoch}, train_loss: {pre_train_loss_epoch}, distance: {batch_pre_train_distance} time: {end_train - start_train} '
                print(log)

            valid_batch_lhs_left, valid_batch_rhs_left, valid_batch_lhs_right, valid_batch_rhs_right, valid_batch_labels = self.generate_batch(
                    0, len(valid_pair_ids), rees_lhs, rees_rhs, valid_pair_ids, valid_labels_pre_train
            )
            feed_dict_valid = {self.lhs_vec_ph_left_pre_train: valid_batch_lhs_left,
                               self.rhs_vec_ph_left_pre_train: valid_batch_rhs_left,
                               self.lhs_vec_ph_right_pre_train: valid_batch_lhs_right,
                               self.rhs_vec_ph_right_pre_train: valid_batch_rhs_right,
                               self.label_ph_pre_train: valid_batch_labels}
            valid_loss = self.sess.run(self.pre_train_loss, feed_dict=feed_dict_valid)
            valid_log = f'epoch {epoch}, valid_loss: {valid_loss} '
            print(valid_log)

    def pre_training_evaluate(self, rees_lhs, rees_rhs, test_pair_ids, test_labels_pre_train):
        test_lhs_left, test_rhs_left, test_lhs_right, test_rhs_right, test_labels_ = self.generate_batch(
            0, len(rees_lhs), rees_lhs, rees_rhs, test_pair_ids, test_labels_pre_train
        )
        feed_dict_test = {self.lhs_vec_ph_left_pre_train: test_lhs_left,
                          self.rhs_vec_ph_left_pre_train: test_rhs_left,
                          self.lhs_vec_ph_right_pre_train: test_lhs_right,
                          self.rhs_vec_ph_right_pre_train: test_rhs_right,
                          self.label_ph_pre_train: test_labels_}
        test_loss = self.sess.run(self.pre_train_loss, feed_dict=feed_dict_test)
        test_log = f'test_loss: {test_loss} '
        print(test_log)
