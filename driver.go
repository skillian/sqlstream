package sqlstream

type Driver interface {
	ArgWriterTo

	// ArgLimit returns the SQL statement parameter limit.
	ArgLimit() int
}
