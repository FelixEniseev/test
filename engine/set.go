package engine

// exclude duplicating
var isKeyExists = struct{}{}

type (
	Set struct {
		records map[string]set
	}

	set map[interface{}]struct{}
)

// non-thread safe thing
func newSet() set {
	s := make(map[interface{}]struct{})

	return s
}

//
func (s set) add(items ...interface{}) {
	if len(items) == 0 {
		return
	}

	for _, item := range items {
		s[item] = isKeyExists
	}
}

//
func (s set) remove(items ...interface{}) {
	if len(items) == 0 {
		return
	}

	for _, item := range items {
		delete(s, item)
	}
}

// Checks for the record existence. For multiple items it returns true only if all items exist.
func (s set) has(items ...interface{}) bool {
	//
	if len(items) == 0 {
		return false
	}

	exist := true
	for _, item := range items {
		if _, exist = s[item]; !exist {
			break
		}
	}
	return exist
}

// bypass elements
func (s set) each(f func(item interface{}) bool) {
	for item := range s {
		if !f(item) {
			break
		}
	}
}

// cloning
func (s set) copy() set {
	u := newSet()
	for item := range s {
		u.add(item)
	}
	return u
}

// retrieve all elements
func (s set) list() []interface{} {
	list := make([]interface{}, 0, len(s))

	for item := range s {
		list = append(list, item)
	}

	return list
}

func (s set) size() int {
	return len(s)
}

func (s *Set) isFieldExists(key string, field interface{}) bool {
	fieldSet, exist := s.records[key]
	if !exist {
		return false
	}
	ok := fieldSet.has(field)
	return ok
}

func (s *Set) Keys() []string {
	keys := make([]string, len(s.records))

	i := 0
	for k := range s.records {
		keys[i] = k
		i++
	}

	return keys
}
