package database

type Streamable interface {
	// How many objects are there in the stream?
	Length() int
	CurrentIndex() int
	// Get next object of streaming
	Next() (interface{}, error)
}
