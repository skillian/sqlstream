package sqlstream

const PkgName = "sqlstream"

var (
	commonBytes = []byte{'"', '?'}
	dquoteBytes = commonBytes[0:1]
	qmarkBytes  = commonBytes[1:2]
)

// type exprWriter struct {
// 	aliases map[expr.Var]string
// 	params  Parameters
// 	vs      expr.Values
// }

// func (ew *exprWriter) init(ps Parameters) {
// 	if ew.aliases == nil {
// 		ew.aliases = make(map[query]string)
// 	}
// 	ew.params = ps
// }

// func (ew *exprWriter) reset() {
// 	for k := range ew.aliases {
// 		delete(ew.aliases, k)
// 	}
// 	ew.params = nil
// 	ew.vs = nil
// }

// func (ew *exprWriter) writeExpr(ctx context.Context, w io.Writer, e expr.Expr) (n int, err error) {
// 	bw, ok := w.(*bufio.Writer)
// 	if !ok {
// 		bw = bufio.NewWriter(w)
// 	}
// 	defer skerrors.WrapDeferred(&err, bw.Flush)
// 	type frame struct {
// 		e     expr.Expr
// 		infix string
// 	}
// 	stack := make([]frame, 1, arbitraryCapacity)
// 	write := func(bw *bufio.Writer, bs []byte, s string, n int) (nn int, err error) {
// 		if bs != nil {
// 			nn, err = bw.Write(bs)
// 		} else {
// 			nn, err = bw.WriteString(s)
// 		}
// 		nn += n
// 		return
// 	}
// 	_ = expr.Walk(e, func(e expr.Expr) bool {
// 		if err = ctx.Err(); err != nil {
// 			return false
// 		}
// 		top := &stack[len(stack)-1]
// 		if e != nil {
// 			stack = append(stack, frame{e: e})
// 			switch e.(type) {
// 			case expr.Mem:
// 			case expr.Binary:
// 				if err = bw.WriteByte('('); err != nil {
// 					return false
// 				}
// 				n++
// 			}
// 			top := &stack[len(stack)-1]
// 			switch e.(type) {
// 			case expr.Eq:
// 				top.infix = " = "
// 			case expr.Ne:
// 				top.infix = " <> "
// 			case expr.Gt:
// 				top.infix = " > "
// 			case expr.Ge:
// 				top.infix = " >= "
// 			case expr.Le:
// 				top.infix = " <= "
// 			case expr.Lt:
// 				top.infix = " < "
// 			case expr.Add:
// 				top.infix = " + "
// 			case expr.Sub:
// 				top.infix = " - "
// 			case expr.Mul:
// 				top.infix = " * "
// 			case expr.Div:
// 				top.infix = " / "
// 			case expr.And:
// 				top.infix = " AND "
// 			case expr.Or:
// 				top.infix = " OR "
// 			case expr.Mem:
// 				top.infix = "."
// 			}
// 			return true
// 		}
// 		sec := &stack[len(stack)-2]
// 		stack = stack[:len(stack)-1]
// 		var nn int
// 		switch e := top.e.(type) {
// 		case query:
// 			al, ok := ew.aliases[e]
// 			if !ok {
// 				// TODO: better alias convention eventually?
// 				al = fmt.Sprintf("T%d", len(ew.aliases))
// 				ew.aliases[e] = al
// 			}
// 			if n, err = write(bw, nil, al, n); err != nil {
// 				return false
// 			}
// 		case expr.Var:
// 			var v interface{}
// 			v, err = ew.vs.Get(ctx, e)
// 			if err != nil {
// 				return false
// 			}
// 			nn, err = ew.params.WriteParameter(bw, v)
// 			n += nn
// 			if err != nil {
// 				return false
// 			}
// 		case expr.Mem:
// 		case expr.Binary:
// 			if err = bw.WriteByte(')'); err != nil {
// 				return false
// 			}
// 			n++
// 		default:
// 			nn, err = ew.params.WriteParameter(bw, e)
// 			n += nn
// 			if err != nil {
// 				return false
// 			}
// 		}
// 		if sec.infix != "" {
// 			bw.WriteString(sec.infix)
// 			sec.infix = ""
// 		}
// 		return true
// 	})

// }
