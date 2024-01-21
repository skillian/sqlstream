module github.com/skillian/sqlstream

go 1.20

require (
	github.com/skillian/errors v0.0.0-20220412220440-9e3e39f14923
	github.com/skillian/expr v0.0.0-20230305130052-6dd916612e69
	github.com/skillian/logging v0.0.0-20220617155357-42fdd303775d
	github.com/skillian/syng v0.0.0-00000000000000-000000000000
)

require github.com/skillian/ctxutil v0.0.0-00010101000000-000000000000 // indirect

replace github.com/skillian/ctxutil => ../ctxutil

replace github.com/skillian/errors => ../errors

replace github.com/skillian/errutil => ../errutil

replace github.com/skillian/expr => ../expr

replace github.com/skillian/logging => ../logging

replace github.com/skillian/syng => ../syng
