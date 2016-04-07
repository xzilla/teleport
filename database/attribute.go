package database

// Define a class attribute
type Attribute struct {
	Name     string `json:"attr_name"`
	Num      int    `json:"attr_num"`
	TypeName string `json:"type_name"`
	TypeOid  string `json:"type_oid"`
}
