package sqlstream

import (
	"database/sql"
	"errors"
	"fmt"
)

var (
	errNoSQLDB     = errors.New("*sql.DB not specified")
	errNoDBDialect = errors.New("DB Dialect not specified")
	errNoDBDriver  = errors.New("DB Driver not specified")
)

// DB wraps a *sql.DB to generate RDBMS-specific queries from expr.Expr
// expressions.
type DB struct {
	db      *sql.DB
	dialect Dialect
	driver  Driver
}

// NewDB creates a new DB with the given options.
func NewDB(options ...DBOption) (*DB, error) {
	db := &DB{}
	for _, opt := range options {
		if err := opt.apply(db); err != nil {
			return nil, fmt.Errorf(
				"failed to apply %v to %v: %w",
				opt, db, err,
			)
		}
	}
	if db.db == nil {
		return nil, errNoSQLDB
	}
	if db.dialect == nil {
		return nil, errNoDBDialect
	}
	if db.driver == nil {
		return nil, errNoDBDriver
	}
	return db, nil
}
