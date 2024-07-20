package sqlstream

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/skillian/sqlstream/sqllang"
)

var (
	nvarchar = []byte("nvarchar()" + "binary")
	//                 01234567890    1234567

	errTypeNameSyntax = errors.New("type name syntax error")
)

func ParseTypeName(typeName string) (t sqllang.Type, err error) {
	typeName = strings.TrimSpace(typeName)
	typeNameBytes := make([]byte, 0, len(typeName))
	for _, r := range typeName {
		typeNameBytes = utf8.AppendRune(typeNameBytes, unicode.ToLower(r))
	}
	typeNameBytes, isN := bytes.CutPrefix(typeNameBytes, nvarchar[0:1])
	typeNameBytes, isVar := bytes.CutPrefix(typeNameBytes, nvarchar[1:4])
	if isVar && !isN {
		typeNameBytes, isN = bytes.CutPrefix(typeNameBytes, nvarchar[0:1])
	}
	var ok bool
	var deref func(interface{}) sqllang.Type
	params := make([]interface{}, 1, 3)
	if typeNameBytes, ok = bytes.CutPrefix(typeNameBytes, nvarchar[4:8]); ok {
		charType := sqllang.String{
			Flags: func() (f sqllang.StringFlags) {
				if isVar {
					f |= sqllang.Fixed
				}
				return
			}(),
		}
		params[0] = &charType
		params = append(params, &charType.Length)
	} else if typeNameBytes, ok = bytes.CutPrefix(typeNameBytes, nvarchar[11:17]); ok {
		binType := sqllang.Binary{
			Fixed: !isVar,
		}
		params[0] = &binType
		params = append(params, &binType.Length)
	}
	// TODO: handle other types
	if typeNameBytes, ok = bytes.CutPrefix(typeNameBytes, nvarchar[8:9]); ok {
		for i := 0; len(typeNameBytes) > 0; i++ {
			end := bytes.IndexFunc(typeNameBytes, func(r rune) bool {
				switch r {
				case ',', ')':
					return true
				}
				return unicode.IsSpace(r)
			})
			if end == -1 {
				return nil, fmt.Errorf(
					"%w: %q", errTypeNameSyntax, typeName,
				)
			}
			paramString := bytes.TrimSpace(typeNameBytes[:end])

		}
	}
}
