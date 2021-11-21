package models

import (
	"encoding/json"
	"testing"

	"../labrpc"
)

const studentTableName_ = "student"
const courseRegistrationTableName_ = "courseRegistration"

var c_ *Cluster
var network_ *labrpc.Network
var cli_ *labrpc.ClientEnd

var studentTableSchema_ *TableSchema
var courseRegistrationTableSchema_ *TableSchema

var studentRows_ []Row
var courseRegistrationRows_ []Row

var joinedTableSchema_ TableSchema
var joinedTableContent_ []Row

var studentTablePartitionRules_ []byte
var courseRegistrationTablePartitionRules_ []byte

func defineTables_() {
	studentTableSchema_ = &TableSchema{TableName: studentTableName_, ColumnSchemas: []ColumnSchema{
		{Name: "sid", DataType: TypeInt32},
		{Name: "name", DataType: TypeString},
		{Name: "age", DataType: TypeInt32},
		{Name: "grade", DataType: TypeFloat},
	}}

	courseRegistrationTableSchema_ = &TableSchema{TableName: courseRegistrationTableName_, ColumnSchemas: []ColumnSchema{
		{Name: "sid", DataType: TypeInt32},
		{Name: "courseId", DataType: TypeInt32},
	}}

	studentRows_ = []Row{
		{0, "John", 22, 4.0},
		{1, "Smith", 23, 3.6},
		{2, "Hana", 21, 4.0},
	}

	courseRegistrationRows_ = []Row{
		{0, 0},
		{0, 1},
		{1, 0},
		{2, 2},
	}

	joinedTableSchema_ = TableSchema{
		"",
		[]ColumnSchema{
			{"sid", TypeInt32},
			{"name", TypeString},
			{"age", TypeInt32},
			{"grade", TypeFloat},
			{"courseId", TypeInt32},
		},
	}

	joinedTableContent_ = []Row{
		{0, "John", 22, 4.0, 0},
		{0, "John", 22, 4.0, 1},
		{1, "Smith", 23, 3.6, 0},
		{2, "Hana", 21, 4.0, 2},
	}
}

func setup_() {
	// set up a network and a cluster
	clusterName := "MyCluster"
	network_ = labrpc.MakeNetwork()
	c_ = NewCluster(3, network_, clusterName)

	// create a client and connect to the cluster
	clientName := "ClientA"
	cli_ = network_.MakeEnd(clientName)
	network_.Connect(clientName, c_.Name)
	network_.Enable(clientName, true)

	defineTables_()
}

// vertically and horizontally divide student table
func TestBlendDivision(t *testing.T) {
	setup_()

	// use the client to create table and insert
	// divide student table into three partitions and assign them to node0(sid, name, <= 3.6), node3(age, grade, <=3.6) and node2 ("sid", "name", "age", "grade", > 3.6)
	m := map[string]interface{}{
		"0": map[string]interface{}{
			"predicate": map[string]interface{}{
				"grade": [...]map[string]interface{}{{
					"op":  "<=",
					"val": 3.6,
				},
				},
			},
			"column": [...]string{
				"sid", "name",
			},
		},
		"1": map[string]interface{}{
			"predicate": map[string]interface{}{
				"grade": [...]map[string]interface{}{{
					"op":  ">",
					"val": 3.6,
				},
				},
			},
			"column": [...]string{
				"sid", "name", "age", "grade",
			},
		},
		"2": map[string]interface{}{
			"predicate": map[string]interface{}{
				"grade": [...]map[string]interface{}{{
					"op":  "<=",
					"val": 3.6,
				},
				},
			},
			"column": [...]string{
				"age", "grade",
			},
		},
	}
	studentTablePartitionRules_, _ = json.Marshal(m)

	// assign course registration to node2
	m = map[string]interface{}{
		"2": map[string]interface{}{
			"predicate": map[string]interface{}{
				"courseId": [...]map[string]interface{}{{
					"op":  ">=",
					"val": 0,
				},
				},
			},
			"column": [...]string{
				"sid", "courseId",
			},
		},
	}
	courseRegistrationTablePartitionRules_, _ = json.Marshal(m)

	buildTables_(cli_)
	insertData_(cli_)

	// perform a join and check the result
	results := Dataset{}
	cli_.Call("Cluster.Join", []string{studentTableName_, courseRegistrationTableName_}, &results)
	expectedDataset := Dataset{
		Schema: joinedTableSchema_,
		Rows:   joinedTableContent_,
	}
	if !compareDataset(expectedDataset, results) {
		t.Errorf("Incorrect join results, expected %v, actual %v", expectedDataset, results)
	}
}

// both student and register are empty
func TestTwoEmptyTable(t *testing.T) {
	setup_()

	studentRows_ = []Row{}
	courseRegistrationRows_ = []Row{}
	joinedTableContent_ = []Row{}

	// use the client to create table and insert
	// divide student table into two partitions and assign them to node0 and node1
	m := map[string]interface{}{
		"0": map[string]interface{}{
			"predicate": map[string]interface{}{
				"grade": [...]map[string]interface{}{{
					"op":  "<=",
					"val": 3.6,
				},
				},
			},
			"column": [...]string{
				"sid", "name", "age", "grade",
			},
		},
		"1": map[string]interface{}{
			"predicate": map[string]interface{}{
				"grade": [...]map[string]interface{}{{
					"op":  ">",
					"val": 3.6,
				},
				},
			},
			"column": [...]string{
				"sid", "name", "age", "grade",
			},
		},
	}
	studentTablePartitionRules_, _ = json.Marshal(m)

	// assign course registration to node1 and node2
	m = map[string]interface{}{
		"1": map[string]interface{}{
			"predicate": map[string]interface{}{
				"courseId": [...]map[string]interface{}{{
					"op":  "<=",
					"val": 1,
				},
				},
			},
			"column": [...]string{
				"sid", "courseId",
			},
		},
		"2": map[string]interface{}{
			"predicate": map[string]interface{}{
				"courseId": [...]map[string]interface{}{{
					"op":  ">",
					"val": 1,
				},
				},
			},
			"column": [...]string{
				"sid", "courseId",
			},
		},
	}
	courseRegistrationTablePartitionRules_, _ = json.Marshal(m)

	buildTables_(cli_)
	insertData_(cli_)

	// perform a join and check the result
	results := Dataset{}
	cli_.Call("Cluster.Join", []string{studentTableName_, courseRegistrationTableName_}, &results)
	expectedDataset := Dataset{
		Schema: joinedTableSchema_,
		Rows:   joinedTableContent_,
	}
	if !compareDataset(expectedDataset, results) {
		t.Errorf("Incorrect join results, expected %v, actual %v", expectedDataset, results)
	}
}

func buildTables_(cli *labrpc.ClientEnd) {
	replyMsg := ""
	cli.Call("Cluster.BuildTable",
		[]interface{}{courseRegistrationTableSchema_, courseRegistrationTablePartitionRules_}, &replyMsg)
	replyMsg = ""
	cli.Call("Cluster.BuildTable", []interface{}{studentTableSchema_, studentTablePartitionRules_}, &replyMsg)
}

func insertData_(cli *labrpc.ClientEnd) {
	replyMsg := ""
	for _, row := range studentRows_ {
		cli.Call("Cluster.FragmentWrite", []interface{}{studentTableName_, row}, &replyMsg)
	}

	replyMsg = ""
	for _, row := range courseRegistrationRows_ {
		cli.Call("Cluster.FragmentWrite", []interface{}{courseRegistrationTableName_, row}, &replyMsg)
	}
}

// vertically divide student table, horizontally divide course table
func TestVstudentHcourseDivision(t *testing.T) {
	setup_()

	// use the client to create table and insert
	// divide student table into two partitions and assign them to node0(sid, grade), node1(age, name)
	m := map[string]interface{}{
		"0": map[string]interface{}{
			"predicate": map[string]interface{}{
				"grade": [...]map[string]interface{}{{
					"op":  "<=",
					"val": 4.0,
				},
				},
			},
			"column": [...]string{
				"sid", "grade",
			},
		},
		"1": map[string]interface{}{
			"predicate": map[string]interface{}{
				"grade": [...]map[string]interface{}{{
					"op":  "<=",
					"val": 4.0,
				},
				},
			},
			"column": [...]string{
				"age", "name",
			},
		},
	}
	studentTablePartitionRules_, _ = json.Marshal(m)

	// assign course registration to node2(<1) node1(>=1)
	m = map[string]interface{}{
		"1": map[string]interface{}{
			"predicate": map[string]interface{}{
				"courseId": [...]map[string]interface{}{{
					"op":  ">=",
					"val": 1,
				},
				},
			},
			"column": [...]string{
				"sid", "courseId",
			},
		},
		"2": map[string]interface{}{
			"predicate": map[string]interface{}{
				"courseId": [...]map[string]interface{}{{
					"op":  "<",
					"val": 1,
				},
				},
			},
			"column": [...]string{
				"sid", "courseId",
			},
		},
	}
	courseRegistrationTablePartitionRules_, _ = json.Marshal(m)

	buildTables_(cli_)
	insertData_(cli_)

	// perform a join and check the result
	results := Dataset{}
	cli_.Call("Cluster.Join", []string{studentTableName_, courseRegistrationTableName_}, &results)
	expectedDataset := Dataset{
		Schema: joinedTableSchema_,
		Rows:   joinedTableContent_,
	}
	if !compareDataset(expectedDataset, results) {
		t.Errorf("Incorrect join results, expected %v, actual %v", expectedDataset, results)
	}
}

func TestVstudentVcourseDivision(t *testing.T) {
	setup_()

	// use the client to create table and insert
	// divide student table into two partitions and assign them to node0(sid, grade), node2(age, name)
	m := map[string]interface{}{
		"0": map[string]interface{}{
			"predicate": map[string]interface{}{
				"grade": [...]map[string]interface{}{{
					"op":  "<=",
					"val": 4.0,
				},
				},
			},
			"column": [...]string{
				"sid", "grade",
			},
		},
		"2": map[string]interface{}{
			"predicate": map[string]interface{}{
				"grade": [...]map[string]interface{}{{
					"op":  "<=",
					"val": 4.0,
				},
				},
			},
			"column": [...]string{
				"age", "name",
			},
		},
	}
	studentTablePartitionRules_, _ = json.Marshal(m)

	// assign course registration to  node1(sid) node2(courseid)
	m = map[string]interface{}{
		"1": map[string]interface{}{
			"predicate": map[string]interface{}{
				"courseId": [...]map[string]interface{}{{
					"op":  "<=",
					"val": 2,
				},
				},
			},
			"column": [...]string{
				"sid",
			},
		},
		"2": map[string]interface{}{
			"predicate": map[string]interface{}{
				"courseId": [...]map[string]interface{}{{
					"op":  "<=",
					"val": 2,
				},
				},
			},
			"column": [...]string{
				"courseId",
			},
		},
	}
	courseRegistrationTablePartitionRules_, _ = json.Marshal(m)

	buildTables_(cli_)
	insertData_(cli_)

	// perform a join and check the result
	results := Dataset{}
	cli_.Call("Cluster.Join", []string{studentTableName_, courseRegistrationTableName_}, &results)
	expectedDataset := Dataset{
		Schema: joinedTableSchema_,
		Rows:   joinedTableContent_,
	}
	if !compareDataset(expectedDataset, results) {
		t.Errorf("Incorrect join results, expected %v, actual %v", expectedDataset, results)
	}
}

// vertically divide student table and repeate some columns
func TestVerticallyOverlapDivision(t *testing.T) {
	setup_()

	// use the client to create table and insert
	// divide student table into two partitions and assign them to node0(sid, grade), node2(age, name)
	m := map[string]interface{}{
		"0": map[string]interface{}{
			"predicate": map[string]interface{}{
				"grade": [...]map[string]interface{}{{
					"op":  "<=",
					"val": 4.0,
				},
				},
			},
			"column": [...]string{
				"sid", "grade", "age",
			},
		},
		"2": map[string]interface{}{
			"predicate": map[string]interface{}{
				"grade": [...]map[string]interface{}{{
					"op":  "<=",
					"val": 4.0,
				},
				},
			},
			"column": [...]string{
				"age", "name", "grade",
			},
		},
	}
	studentTablePartitionRules_, _ = json.Marshal(m)

	// assign course registration to  node1(sid) node2(courseid)
	m = map[string]interface{}{
		"1": map[string]interface{}{
			"predicate": map[string]interface{}{
				"courseId": [...]map[string]interface{}{{
					"op":  "<=",
					"val": 2,
				},
				},
			},
			"column": [...]string{
				"sid",
			},
		},
		"2": map[string]interface{}{
			"predicate": map[string]interface{}{
				"courseId": [...]map[string]interface{}{{
					"op":  "<=",
					"val": 2,
				},
				},
			},
			"column": [...]string{
				"courseId",
			},
		},
	}
	courseRegistrationTablePartitionRules_, _ = json.Marshal(m)

	buildTables_(cli_)
	insertData_(cli_)

	// perform a join and check the result
	results := Dataset{}
	cli_.Call("Cluster.Join", []string{studentTableName_, courseRegistrationTableName_}, &results)
	expectedDataset := Dataset{
		Schema: joinedTableSchema_,
		Rows:   joinedTableContent_,
	}
	if !compareDataset(expectedDataset, results) {
		t.Errorf("Incorrect join results, expected %v, actual %v", expectedDataset, results)
	}
}
