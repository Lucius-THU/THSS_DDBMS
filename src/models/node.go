package models

import (
	"encoding/json"
	"errors"
	"fmt"
)

// Node manages some tables defined in models/table.go
type Node struct {
	// the name of the Node, and it should be unique across the cluster
	Identifier string
	// tableName -> table
	TableMap map[string]*Table
}

// NewNode creates a new node with the given name and an empty set of tables
func NewNode(id string) *Node {
	return &Node{TableMap: make(map[string]*Table), Identifier: id}
}

// SayHello is an example about how to create a method that can be accessed by RPC (remote procedure call, methods that
// can be called through network from another node). RPC methods should have exactly two arguments, the first one is the
// actual argument (or an argument list), while the second one is a reference to the result.
func (n *Node) SayHello(args interface{}, reply *string) {
	// NOTICE: use reply (the second parameter) to pass the return value instead of "return" statements.
	*reply = fmt.Sprintf("Hello %s, I am Node %s", args, n.Identifier)
}

// CreateTable creates a Table on this node with the provided schema. It returns nil if the table is created
// successfully, or an error if another table with the same name already exists.
func (n *Node) CreateTable(schema *TableSchema) error {
	// check if the table already exists
	if _, ok := n.TableMap[schema.TableName]; ok {
		return errors.New("table already exists")
	}
	// create a table and store it in the map
	t := NewTable(
		schema,
		NewMemoryListRowStore(),
	)
	n.TableMap[schema.TableName] = t
	return nil
}

// Insert inserts a row into the specified table, and returns nil if succeeds or an error if the table does not exist.
func (n *Node) Insert(tableName string, row *Row) error {
	if t, ok := n.TableMap[tableName]; ok {
		t.Insert(row)
		return nil
	} else {
		return errors.New("no such table")
	}
}

// Remove removes a row from the specified table, and returns nil if succeeds or an error if the table does not exist.
// It does not concern whether the provided row exists in the table.
func (n *Node) Remove(tableName string, row *Row) error {
	if t, ok := n.TableMap[tableName]; ok {
		t.Remove(row)
		return nil
	} else {
		return errors.New("no such table")
	}
}

// IterateTable returns an iterator of the table through which the caller can retrieve all rows in the table in the
// order they are inserted. It returns (iterator, nil) if the Table can be found, or (nil, err) if the Table does not
// exist.
func (n *Node) IterateTable(tableName string) (RowIterator, error) {
	if t, ok := n.TableMap[tableName]; ok {
		return t.RowIterator(), nil
	} else {
		return nil, errors.New("no such table")
	}
}

// IterateTable returns the count of rows in a table. It returns (cnt, nil) if the Table can be found, or (-1, err)
// if the Table does not exist.
func (n *Node) count(tableName string) (int, error) {
	if t, ok := n.TableMap[tableName]; ok {
		return t.Count(), nil
	} else {
		return -1, errors.New("no such table")
	}
}

// ScanTable returns all rows in a table by the specified name or nothing if it does not exist.
// This method is recommended only to be used for TEST PURPOSE, and try not to use this method in your implementation,
// but you can use it in your own test cases.
// The reason why we deprecate this method is that in practice, every table is so large that you cannot transfer a whole
// table through network all at once, so sending a whole table in one RPC is very impractical. One recommended way is to
// fetch a batch of Rows a time.
func (n *Node) ScanTable(tableName string, dataset *Dataset) {
	if t, ok := n.TableMap[tableName]; ok {
		resultSet := Dataset{}

		tableRows := make([]Row, t.Count())
		i := 0
		iterator := t.RowIterator()
		for iterator.HasNext() {
			tableRows[i] = *iterator.Next()
			i = i + 1
		}

		resultSet.Rows = tableRows
		resultSet.Schema = *t.schema
		*dataset = resultSet
	}
}

func (n *Node) RPCCreateTable(args []interface{}, reply *string) {
	schema := args[0].(TableSchema)
	predicate := args[1].(Predicate)
	fullSchema := args[2].(TableSchema)
	for k, v := range predicate {
		for _, cs := range fullSchema.ColumnSchemas {
			if cs.Name == k {
				for i, value := range v {
					if value.Val == nil {
						if OpIsEqualOrNotEqual(value.Op) {
							predicate[k][i].RealType = cs.DataType
							continue
						} else {
							*reply = "1 Operator Not Suitable For null"
							return
						}
					}
					var ok bool
					switch cs.DataType {
					case TypeInt32, TypeInt64, TypeFloat, TypeDouble:
						predicate[k][i].NumberValue, ok = value.Val.(json.Number)
						if ok {
							if _, err1 := predicate[k][i].NumberValue.Float64(); err1 != nil {
								if _, err2 := predicate[k][i].NumberValue.Int64(); err2 != nil {
									ok = false
								}
							}
						}
					case TypeBoolean:
						predicate[k][i].BoolValue, ok = value.Val.(bool)
					case TypeString:
						predicate[k][i].StringValue, ok = value.Val.(string)
					}
					if !ok {
						*reply = "1 TypeError"
						return
					}
					predicate[k][i].RealType = cs.DataType
				}
				break
			}
		}
	}
	if err := n.CreateTable(&schema); err != nil {
		*reply = fmt.Sprintf("1 %v", err)
	} else {
		if t, ok := n.TableMap[schema.TableName]; ok {
			t.predicate = &predicate
			t.fullSchema = &fullSchema
			*reply = "0 OK"
		} else {
			*reply = "1 Create Table Fail"
		}
	}
}

func (n *Node) RPCInsert(args []interface{}, reply *string) {
	tableName := args[0].(string)
	if t, ok := n.TableMap[tableName]; ok {
		row := args[1].(Row)
		var subRow Row
		for i, v := range row {
			if atoms, exist := (*t.predicate)[t.fullSchema.ColumnSchemas[i].Name]; exist {
				for _, atom := range atoms {
					if !atom.Check(v) {
						*reply = "1 Predicate Check Fail"
						return
					}
				}
			}
		}
		for _, v := range t.schema.ColumnSchemas {
			for i, cs := range t.fullSchema.ColumnSchemas {
				if cs.Name == v.Name {
					subRow = append(subRow, row[i])
					break
				}
			}
		}
		if err := n.Insert(tableName, &subRow); err != nil {
			*reply = fmt.Sprintf("1 %v", err)
			return
		}
	}
	*reply = "0 OK"
}

func OpIsEqualOrNotEqual(op string) bool {
	return op == "==" || op == "=" || op == "!=" || op == "<>" || op == ">=" || op == "<="
}
