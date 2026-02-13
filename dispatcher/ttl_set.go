package dispatcher

import "time"

type ttlSet struct {
	ttl   time.Duration
	items map[string]time.Time
}

func newTTLSet(ttl time.Duration) *ttlSet {
	return &ttlSet{
		ttl:   ttl,
		items: make(map[string]time.Time),
	}
}

func (s *ttlSet) add(key string, now time.Time) {
	if s == nil || s.ttl <= 0 {
		return
	}

	s.items[key] = now.Add(s.ttl)
}

func (s *ttlSet) contains(key string, now time.Time) bool {
	if s == nil {
		return false
	}

	expiresAt, ok := s.items[key]
	if !ok {
		return false
	}

	if !expiresAt.After(now) {
		delete(s.items, key)
		return false
	}

	return true
}

func (s *ttlSet) cleanupExpired(now time.Time) {
	if s == nil {
		return
	}

	for key, expiresAt := range s.items {
		if !expiresAt.After(now) {
			delete(s.items, key)
		}
	}
}
