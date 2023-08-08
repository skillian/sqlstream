module github.com/skillian/sqlstream

go 1.20

require github.com/skillian/expr v0.0.0

require (
	github.com/skillian/ctxutil v0.0.0 // indirect
	github.com/skillian/errors v0.0.0-20220412220440-9e3e39f14923 // indirect
	github.com/skillian/logging v0.0.0-20220617155357-42fdd303775d // indirect
)

replace github.com/skillian/expr => ../expr

replace github.com/skillian/ctxutil => ../ctxutil
