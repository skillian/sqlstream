package sqlstream

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
)

// DBOption applies an option to the DB.
type DBOption interface {
	applyToDB(db *DB) error
}

// DBInfoOption applies an option to the DB.
type DBInfoOption interface {
	applyToDBInfo(dbi *DBInfo) error
}

var (
	errRedef = errors.New("redefinition")
)

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

func (f dbOptionFunc) applyToDB(db *DB) error { return f(db) }

type errDBOption struct{ err error }

func (e errDBOption) applyToDB(db *DB) error { return e.err }
