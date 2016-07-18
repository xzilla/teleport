package database

// Implements Streamable

type SelectQuery struct {
	db              *database.Database
	Columns         string
	Where           string
	OrderBy         string
	PaginationSize  int
	rowsCache       []interface{}
	currentRow      int
	totalRows       int
}

func NewSelectQuery(db *database.Database, columns, where, orderBy string, paginationSize int) {
	query := &SelectQuery{
		db:              db,
		columns:         columns,
		where:           where,
		orderBy:         orderBy,
		paginationSize:  paginationSize,
		rowsCache:       make([]interface{}),
	}

	err := query.updateTotals()

	if err != nil {
		panic(err)
	}

	return query
}

func (q *SelectQuery) updateTotals() error {
}
