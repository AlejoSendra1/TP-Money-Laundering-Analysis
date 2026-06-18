package batch_utils

type Set[E comparable] map[E]struct{}

func NewSet[E comparable]() Set[E] {
	return make(Set[E])
}

func (s Set[E]) Add(val E) {
	s[val] = struct{}{}
}

func (s Set[E]) Contains(val E) bool {
	_, ok := s[val]
	return ok
}

func (s Set[E]) Size() int {
	return len(s)
}

func (s Set[E]) Remove(val E) {
	delete(s, val)
}
