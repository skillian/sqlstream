module github.com/skillian/sqlstream

go 1.23.0

toolchain go1.24.2

require github.com/skillian/expr v0.0.0

require github.com/skillian/logging v0.0.0

require (
	github.com/skillian/errors v0.0.0-20220412220440-9e3e39f14923 // indirect
	github.com/skillian/errutil v0.0.0
)

require (
	github.com/davecgh/go-spew v1.1.1
	github.com/skillian/ctxutil v0.0.0
	github.com/skillian/unsafereflect v0.0.0
	modernc.org/sqlite v1.37.1
)

require (
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/ncruces/go-strftime v0.1.9 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20230129092748-24d4a6f8daec // indirect
	golang.org/x/exp v0.0.0-20250408133849-7e4ce0ab07d0 // indirect
	golang.org/x/sys v0.33.0 // indirect
	modernc.org/libc v1.65.7 // indirect
	modernc.org/mathutil v1.7.1 // indirect
	modernc.org/memory v1.11.0 // indirect
)

replace github.com/skillian/ctxutil => ../ctxutil

replace github.com/skillian/errors => ../errors

replace github.com/skillian/errutil => ../errutil

replace github.com/skillian/expr => ../expr

replace github.com/skillian/logging => ../logging

replace github.com/skillian/unsafereflect => ../unsafereflect

replace github.com/skillian/unsafereflecttest => ../unsafereflecttest
