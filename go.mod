module github.com/skillian/sqlstream

go 1.17

require github.com/skillian/expr v0.0.0

require github.com/skillian/logging v0.0.0

require (
	github.com/skillian/errors v0.0.0-20220412220440-9e3e39f14923 // indirect
	github.com/skillian/errutil v0.0.0
)

require (
	github.com/mattn/go-sqlite3 v1.14.24
	github.com/skillian/ctxutil v0.0.0
	github.com/skillian/unsafereflect v0.0.0
)

replace github.com/skillian/ctxutil => ../ctxutil

replace github.com/skillian/errors => ../errors

replace github.com/skillian/errutil => ../errutil

replace github.com/skillian/expr => ../expr

replace github.com/skillian/logging => ../logging

replace github.com/skillian/unsafereflect => ../unsafereflect
