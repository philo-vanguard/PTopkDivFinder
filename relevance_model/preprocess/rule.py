import operator
import pandas as pd
import numpy as np

operators = {'==': operator.eq, '=': operator.eq, '<>': operator.ne, '>': operator.gt, '<': operator.lt, '>=': operator.ge, '<=': operator.le}


class Predicate:
    def __init__(self, tableName=None):
        self.index1 = None
        self.index2 = None
        self.attr1 = ""
        self.attr2 = ""
        self.constant = ""
        self.operator = ""
        self.tableName = tableName
        self.confidence = 0.0  # the confidence threshold for Mc rules
        self.type = ""  # an Enum {constant, non-constant, ML}

    def transform(self, str_predicate, op):
        info = str_predicate.strip().split(op)
        self.index1 = int(info[0].strip().split(".")[1][1])
        self.attr1 = info[0].strip().split(".")[2]
        self.operator = op
        if "t1." in info[1]:
            self.index2 = int(info[1].strip().split(".")[1][1])
            self.attr2 = info[1].strip().split(".")[2]
            self.type = "non-constant"
        else:
            self.constant = eval(info[1].strip()) if "\'" in info[1] else info[1].strip()
            self.type = "constant"

    def get_index1(self):
        return self.index1

    def get_index2(self):
        return self.index2

    def get_attr1(self):
        return self.attr1

    def get_attr2(self):
        return self.attr2

    def get_constant(self):
        return self.constant

    def get_operator(self):
        return self.operator

    def get_type(self):
        return self.type

    def is_constant(self):
        if self.type == "non-constant":
            return False
        else:
            return True

    def get_confidence(self):
        return self.confidence

    def assign_info(self, index1, index2, attr1, attr2, constant, operator, p_type):
        self.index1 = index1
        self.index2 = index2
        self.attr1 = attr1
        self.attr2 = attr2
        self.constant = constant
        self.operator = operator
        self.type = p_type  # an Enum {constant, non-constant, ML}

    def print_predicate(self):
        output = ""
        if self.type == "non-constant":
            output = "t" + str(self.index1) + "." + self.attr1 + " = t" + str(self.index2) + "." + self.attr2
        elif self.type == "constant":
            output = "t" + str(self.index1) + "." + self.attr1 + " = " + str(self.constant)
        elif self.type == "Mc":
            output = "Mc(t" + str(self.index1) + ".[" + ", ".join(self.attr1) + "], t" + str(self.index1) + "." + self.attr2 + ") > " + str(self.confidence)
        return output

    def print_predicate_new(self):
        output = ""
        if self.type == "non-constant":
            output = self.tableName + ".t" + str(self.index1) + "." + self.attr1 + " == " + self.tableName + ".t" + str(self.index2) + "." + self.attr2
        elif self.type == "constant":
            output = self.tableName + ".t" + str(self.index1) + "." + self.attr1 + " == " + str(self.constant)
        return output


class REELogic:
    def __init__(self):
        self.type = "logic"  # an Enum {logic, KG, M_d, M_c}
        self.currents = []  # the predicates in X
        self.RHS = None  # the predicate of Y
        self.support = None
        self.confidence = None  # the confidence of this REE
        self.tuple_variable_cnt = 0
        self.has_computed_satisfied_tuplesIDs = False
        self.satisfied_tupleIDs = None

    # identify precondition X and consequence e based on textual rule
    def load_X_and_e(self, textual_rule, data_name=None):
        precondition, rhs_info = textual_rule.split("->")[0].strip().split(":")[1].strip(), textual_rule.split("->")[1].strip().split(",")
        consequence = rhs_info[0].strip()

        if "t0." in consequence and "t1." in consequence:
            self.tuple_variable_cnt = 2
        elif "t0." in consequence:
            self.tuple_variable_cnt = 1
        elif "t1." in consequence:
            self.tuple_variable_cnt = 1

        if '⋀' in precondition:
            precondition = precondition.split('⋀')
        elif '^' in precondition:
            precondition = precondition.split('^')
        else:
            precondition = [precondition]
        for idx in range(len(precondition)):
            predicate = precondition[idx].strip()
            operator = self.obtain_operator(predicate)
            p = []
            if operator != '':  # if operator is one of <>, >=, <=, ==, =, > and <
                pre = Predicate(data_name)
                pre.transform(predicate, operator)
                self.currents.append(pre)

        # obtain consequence e
        operator = self.obtain_operator(consequence)
        self.RHS = Predicate(data_name)
        self.RHS.transform(consequence, operator)
        self.support = float(rhs_info[1].split(':')[1].strip())  # the suport of this REE
        self.confidence = float(rhs_info[2].split(':')[1].strip())  # the confidence of this REE


    # identify the operator from <>, >=, <=, =, > and <
    def obtain_operator(self, predicate):
        operator = ''
        if (predicate.find('<>') != -1):
            operator = '<>'
        elif (predicate.find('>=') != -1):
            operator = '>='
        elif (predicate.find('<=') != -1):
            operator = '<='
        elif (predicate.find('==') != -1):
            operator = '=='
        elif (predicate.find('=') != -1):
            operator = '='
        elif (predicate.find('>') != -1):
            operator = '>'
        elif (predicate.find('<') != -1):
            operator = '<'
        return operator

    def get_support(self):
        return self.support

    def get_confidence(self):
        return self.confidence

    def get_type(self):
        return self.type

    def get_tuple_variable_cnt(self):
        return self.tuple_variable_cnt

    def is_constant(self):
        if self.tuple_variable_cnt == 0:
            self.tuple_variable_cnt = 1
            for pred in self.currents:
                if not pred.is_constant():
                    self.tuple_variable_cnt = 2
            if not self.RHS.is_constant():
                self.tuple_variable_cnt = 2

        return self.tuple_variable_cnt

    def get_currents(self):
        return self.currents

    def get_RHS(self):
        return self.RHS

    # given a dataset, calculate the tuples that satisfy the rule, and return the tuple ids
    def get_satisfied_tuple_ids(self, data_df):
        if self.has_computed_satisfied_tuplesIDs is True:
            return self.satisfied_tupleIDs

        if self.tuple_variable_cnt == 1:  # single-variable
            reserved_indices = data_df.index
            for predicate in self.currents:
                attr = predicate.get_attr1()
                operator = predicate.get_operator()
                constant = predicate.get_constant()
                reserved_tuples_check = data_df.loc[reserved_indices]
                reserved_indices = reserved_tuples_check.loc[operators[operator](reserved_tuples_check[attr].astype(str), constant)].index.values
                if reserved_indices.shape[0] == 0:
                    break

            if reserved_indices.shape[0] == 0:  # there exist no tuples that satisfy the X of rule
                self.has_computed_satisfied_tuplesIDs = True
                self.satisfied_tupleIDs = None
                return None

            attr = self.RHS.get_attr1()
            operator = self.RHS.get_operator()
            constant = self.RHS.get_constant()
            reserved_tuples_check = data_df.loc[reserved_indices]
            satisfy_rhs_indices = reserved_tuples_check.loc[operators[operator](reserved_tuples_check[attr].astype(str), constant)].index.values  # the indices of tuples that both satisfy X and Y
            if satisfy_rhs_indices.shape[0] == 0:
                self.has_computed_satisfied_tuplesIDs = True
                self.satisfied_tupleIDs = None
                return None
            else:
                self.has_computed_satisfied_tuplesIDs = True
                self.satisfied_tupleIDs = satisfy_rhs_indices
                return satisfy_rhs_indices  # tuples ids, array

        elif self.tuple_variable_cnt == 2:  #bi-variable
            rhs = self.RHS
            attr_RHS = rhs.get_attr1()
            value2tid_tuples_t0, value2tid_tuples_t1 = {}, {}
            constants_in_X = [[], []]
            reserved_indices = data_df.index
            # 1. get key
            key_attributes_non_constant = []
            for predicate in self.currents:
                if predicate.get_type() == "non-constant":
                    key_attributes_non_constant.append(predicate.get_attr1())
                else:
                    tid = predicate.get_index1()
                    constants_in_X[tid].append((predicate.get_attr1(), predicate.get_constant()))  # (A, a)
            # 2. get value of tuple t0
            # (1) first filter tuples that not satisfy the constant predicates
            for attr, v in constants_in_X[0]:  # constant predicates
                tuples_check_constants = data_df.loc[reserved_indices]
                reserved_indices = tuples_check_constants.loc[tuples_check_constants[attr].astype(str) == v].index.values
                if reserved_indices.shape[0] == 0:
                    break
            if reserved_indices.shape[0] == 0:  # there's no tuples satisfy the constant predicates in X of current ree; we should go for the next ree
                self.has_computed_satisfied_tuplesIDs = True
                self.satisfied_tupleIDs = None
                return None
            # (2) then construct dict by non-constant predicates
            for value, df in data_df.loc[reserved_indices].groupby(key_attributes_non_constant):
                if df.shape[0] == 1:
                    continue
                value2tid_tuples_t0[value] = df
            if len(value2tid_tuples_t0) == 0:  # there's no tuples satisfy the non-constant predicates in X of current ree
                self.has_computed_satisfied_tuplesIDs = True
                self.satisfied_tupleIDs = None
                return None
            # 3. get value of tuple t1
            # (1) first filter tuples that not satisfy the constant predicates
            reserved_indices = data_df.index
            for attr, v in constants_in_X[1]:  # constant predicates
                tuples_check_constants = data_df.loc[reserved_indices]
                reserved_indices = tuples_check_constants.loc[tuples_check_constants[attr].astype(str) == v].index.values
                if reserved_indices.shape[0] == 0:
                    break
            if reserved_indices.shape[0] == 0:  # there's no tuples satisfy the constant predicates in X of current ree; we should go for the next ree
                self.has_computed_satisfied_tuplesIDs = True
                self.satisfied_tupleIDs = None
                return None
            # (2) then construct dict by non-constant predicates
            for value, df in data_df.loc[reserved_indices].groupby(key_attributes_non_constant):
                if df.shape[0] == 1:
                    continue
                value2tid_tuples_t1[value] = df  # df.index.values
            if len(value2tid_tuples_t1) == 0:  # there's no tuples satisfy the non-constant predicates in X of current ree
                self.has_computed_satisfied_tuplesIDs = True
                self.satisfied_tupleIDs = None
                return None
            # 4. check Y
            satisfied_indices = set()
            # (1) check constant Y
            if rhs.get_type() == "constant":
                for key in value2tid_tuples_t0.keys():
                    if key not in value2tid_tuples_t1.keys():
                        continue
                    if rhs.get_index1() == 0:
                        check_df = value2tid_tuples_t0[key]
                        satisfy_index = check_df.loc[check_df[attr_RHS].astype(str) == rhs.get_constant()].index.values
                        if satisfy_index.shape[0] == 0:
                            continue
                        satisfied_indices.update(satisfy_index)  # t0
                        satisfied_indices.update(value2tid_tuples_t1[key].index)  # t1
                    else:
                        check_df = value2tid_tuples_t1[key]
                        satisfy_index = check_df.loc[check_df[attr_RHS].astype(str) == rhs.get_constant()].index.values
                        if satisfy_index.shape[0] == 0:
                            continue
                        satisfied_indices.update(satisfy_index)  # t1
                        satisfied_indices.update(value2tid_tuples_t0[key].index)  # t0
            # (2) check non-constant Y
            else:
                for key in value2tid_tuples_t0.keys():
                    if key not in value2tid_tuples_t1.keys():
                        continue
                    check_series = value2tid_tuples_t0[key][attr_RHS]
                    for index_t_ in check_series.index:
                        check_value = check_series[index_t_]  # t0
                        if pd.isnull(check_value):
                            continue
                        check_df = value2tid_tuples_t1[key]  # t1
                        satisfy_index = check_df.loc[check_df[attr_RHS].astype(str) == str(check_value)].index.values
                        if satisfy_index.shape[0] == 0:
                            continue
                        satisfied_indices.add(index_t_)  # t0
                        if satisfy_index.shape[0] == 1:
                            satisfied_indices.add(satisfy_index[0])
                        satisfied_indices.update(satisfy_index)  # t1

            if len(satisfied_indices) == 0:
                self.has_computed_satisfied_tuplesIDs = True
                self.satisfied_tupleIDs = None
                return None
            else:
                self.has_computed_satisfied_tuplesIDs = True
                self.satisfied_tupleIDs = np.array(list(satisfied_indices))
                return np.array(list(satisfied_indices))  # tuples ids, array

    # given another rule, compute their predicate overlap
    def cal_predicates_overlap(self, rule):
        predicates_current = []
        for pre in self.currents:
            predicates_current.append(pre.print_predicate())
        predicates_current.append(self.RHS.print_predicate())  ##

        predicates_another = []
        for pre in rule.get_currents():
            predicates_another.append(pre.print_predicate())
        predicates_another.append(rule.get_RHS().print_predicate())  ##

        unionset = list(set(predicates_current).union(set(predicates_another)))
        intersection = list(set(predicates_current) & set(predicates_another))

        # same_rhs = 0.2 if self.RHS.print_predicate() == rule.get_RHS().print_predicate() else 0
        # overlap_ratio = len(intersection) * 1.0 / len(unionset) + same_rhs

        overlap_ratio = len(intersection) * 1.0 / len(unionset)

        return overlap_ratio

    def print_rule(self):
        output = ""

        output += self.currents[0].print_predicate()
        for idx in range(1, len(self.currents)):
            output += " ^ "
            output += self.currents[idx].print_predicate()

        output += " -> "
        output += self.RHS.print_predicate()

        return output

    def print_rule_new(self):
        output = "Rule : "

        output += self.currents[0].print_predicate_new()
        for idx in range(1, len(self.currents)):
            output += " ^ "
            output += self.currents[idx].print_predicate_new()

        output += " -> "
        output += self.RHS.print_predicate_new()

        return output