package sqlstream

func Save(ctx context.Context, dbi *DBInfo, v interface{}, options ...SaveOption) (saveErr error) {
	vs := emptyInterfacesOf(v)
	for _, v := range vs {
		if saveErr = saveOne(ctx, dbi, v, options...); err != nil {
			return
		}
	}
	return nil
}

func saveOne(ctx context.Context, dbi *DBInfo, v interface{}, options ...SaveOption) (saveErr error) {
	ctx, endTx, err := WithTransaction(ctx)
	defer func() { endTx(saveErr) }()
	mt := modelTypeOf(v)
	st := dbi.sqlTableOf(mt)
	if (st.flags&sqlTableExists) != sqlTableExists {
		// TODO: Check if table exists?
		// TODO: Create table depending on option(s)?
	}
}

type SaveOption interface {
	applySaveOption(*saveConfig) error
}

type saveOptionFunc func(*saveConfig) error

func (f saveOptionFunc) applySaveOption(c *saveConfig) error {
	return f(c)
}

type saveConfig struct {
	flags saveFlags
}

type saveFlags

const (
	saveFlagCreate = 1 << iota
)

// WithCreateIfNotExist configures a save operation to create something
// if it does not already exist.
func WithCreateIfNotExist() SaveOption {
	return saveOptionFunc(func(c *saveConfig) error {
		c.flags |= saveFlagCreate
		return nil
	})
}

func emptyInterfacesOf(v interface{}) []interface{} {
	if vs, ok := v.([]interface{}); ok {
		return vs
	}
	return emptyInterfacesOfReflectValue(reflect.ValueOf(v))
}

func emptyInterfacesOfReflectValue(rv reflect.Value) []interface{} {
	if !rv.IsValid() {
		return nil
	}
	switch rv.Kind() {
	case reflect.Struct:
		return []interface{}{rv.Addr().Interface()}
	case reflect.Array, reflect.Slice:
		vs := make([]interface{}, 0, rv.Len())
		for i, limit := 0, len(vs); i < limit; i++ {
			vs2 := emptyInterfacesOfReflectValue(rv.Index(i))
			switch len(vs2) {
			case 0:
				// pass
			case 1:
				vs = append(vs, vs2[0])
			default:
				vs = append(vs, vs2)
			}
		}
		return vs
	default:
		return []interface{}{rv.Interface()}
	}
}

