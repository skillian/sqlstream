package sqlstream

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
)

// DBOption applies an option to the DB.
type DBOption interface {
	apply(db *DB) error
}

var (
	errRedef = errors.New("redefinition")
)

// WithDialect specifies the Dialect to use with the database
func WithDialect(d Dialect) DBOption {
	return dbOptionFunc(func(db *DB) error {
		if db.dialect != nil {
			return fmt.Errorf(
				"%w of Dialect: %v to %v",
				errRedef, db.dialect, d,
			)
		}
		db.dialect = d
		return nil
	})
}

// WithDriver specifies the Driver to use with the database
func WithDriver(d Driver) DBOption {
	return dbOptionFunc(func(db *DB) error {
		if db.driver != nil {
			return fmt.Errorf(
				"%w of Driver: %v to %v",
				errRedef, db.driver, d,
			)
		}
		db.driver = d
		return nil
	})
}

// WithSQLDB specifies the *sql.DB for the sqlstream.DB to wrap
func WithSQLDB(sqlDB *sql.DB) DBOption {
	return dbOptionFunc(func(db *DB) error {
		if db.db != nil {
			return fmt.Errorf(
				"%w of *sql.DB: %v to %v",
				errRedef, db.db, sqlDB,
			)
		}
		db.db = sqlDB
		return nil
	})
}

// WithSQLOpen calls sql.Open with the given driverName and
// dataSourceName.
func WithSQLOpen(driverName, dataSourceName string) DBOption {
	sqlDB, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		return errDBOption{fmt.Errorf(
			PkgName+".WithSQLOpen(%q, %q) error: %w",
			driverName, dataSourceName, err,
		)}
	}
	return WithSQLDB(sqlDB)
}

// WithSQLOpenDB calls sql.OpenDB with the given connector.
func WithSQLOpenDB(c driver.Connector) DBOption {
	sqlDB := sql.OpenDB(c)
	if err := sqlDB.Ping(); err != nil {
		return errDBOption{fmt.Errorf(
			PkgName+".WithSQLOpenDB(%#v) error: %w",
			c, err,
		)}
	}
	return WithSQLDB(sqlDB)
}

type dbOptionFunc func(*DB) error

func (f dbOptionFunc) apply(db *DB) error { return f(db) }

type errDBOption struct{ err error }

func (e errDBOption) apply(db *DB) error { return e.err }
