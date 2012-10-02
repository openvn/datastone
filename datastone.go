// Copyright 2012 The open-vn.org Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package datastone

import (
	"appengine"
	"appengine/datastore"

	"github.com/openvn/nstuff/model"
)

// Conn implement nstuff/model.Connecter
type Conn struct {
	context appengine.Context
}

func NewConn(ct appengine.Context) *Conn {
	conn := &Conn{}
	conn.context = ct
	return conn
}

func (c *Conn) SetContext(ct appengine.Context) {
	c.context = ct
}

func (c *Conn) Context() appengine.Context {
	return c.context
}

func (c *Conn) Storage(name string) model.Storer {
	s := &Storage{}
	s.name = name
	s.conn = c
	return s
}

// Storage implement nstuff/model.Storer
type Storage struct {
	name string
	conn *Conn
}

func (s *Storage) Conn() model.Connecter {
	return s.conn
}

func (s *Storage) NewKey() (model.Identifier, error) {
	return datastore.NewIncompleteKey(s.conn.context, s.name, nil), nil
}

func (s *Storage) DecodeKey(key string) (model.Identifier, error) {
	return datastore.DecodeKey(key)
}

func (s *Storage) NewQuery() model.Querier {
	q := &Query{}
	q.query = datastore.NewQuery(s.name)
	q.storage = s
	return q
}

func (s *Storage) Put(src interface{}) (model.Identifier, error) {
	return datastore.Put(s.conn.context, datastore.NewIncompleteKey(s.conn.context, s.name, nil), src)
}

func (s *Storage) Get(key model.Identifier, dst interface{}) error {
	err := datastore.Get(s.conn.Context(), key.(*datastore.Key), dst)
	if err == datastore.Done {
		return model.ErrNotFound
	}
	return err
}

func (s *Storage) Update(key model.Identifier, change interface{}) error {
	_, err := datastore.Put(s.conn.Context(), key.(*datastore.Key), change)
	if err == datastore.Done {
		return model.ErrNotFound
	}
	return err
}

func (s *Storage) Delete(key model.Identifier) error {
	return datastore.Delete(s.conn.Context(), key.(*datastore.Key))
}

// Query implement nstuff/model.Querier
type Query struct {
	query   *datastore.Query
	storage *Storage
}

func (q *Query) Storage() model.Storer {
	return q.storage
}

func (q *Query) Filter(field string, operator model.Operator, value interface{}) model.Querier {
	switch operator {
	case model.EQ:
		q.query = q.query.Filter(field+" =", value)
	case model.GE:
		q.query = q.query.Filter(field+" >=", value)
	case model.GT:
		q.query = q.query.Filter(field+" >", value)
	case model.LE:
		q.query = q.query.Filter(field+" <=", value)
	case model.LT:
		q.query = q.query.Filter(field+" <", value)
	default:
		q.query = q.query.Filter(field+" =", value)
	}
	return q
}

func (q *Query) KeysOnly() model.Querier {
	q.query.KeysOnly()
	return q
}

func (q *Query) Limit(limit int) model.Querier {
	q.query = q.query.Limit(limit)
	return q
}

func (q *Query) Offset(offset int) model.Querier {
	q.query = q.query.Offset(offset)
	return q
}

func (q *Query) Order(field string) model.Querier {
	q.query = q.query.Order(field)
	return q
}

func (q *Query) OrderDescending(field string) model.Querier {
	q.query = q.query.Order("-" + field)
	return q
}

func (q *Query) GetFirst(dst interface{}) (model.Identifier, error) {
	t := q.query.Run(q.storage.conn.Context())
	key, err := t.Next(dst)
	if err == datastore.Done {
		return key, model.ErrNotFound
	}
	return key, err
}

func (q *Query) GetAll(dst interface{}) ([]model.Identifier, error) {
	results, err := q.query.GetAll(q.storage.conn.Context(), dst)
	if err != nil {
		if err == datastore.Done {
			return nil, model.ErrNotFound
		}
		return nil, err
	}
	n := len(results)
	if n == 0 {
		return nil, model.ErrNotFound
	}
	keys := make([]model.Identifier, n)
	for i := 0; i < n; i++ {
		keys[i] = results[i]
	}
	return keys, err
}

func (q *Query) DeleteFirst() (model.Identifier, error) {
	result, err := q.GetFirst(nil)
	if err != nil {
		return nil, err
	}
	return result, q.storage.Delete(result)
}

func (q *Query) DeleteAll() ([]model.Identifier, error) {
	results, err := q.query.KeysOnly().GetAll(q.storage.conn.context, nil)
	if err != nil {
		if err == datastore.Done {
			return nil, model.ErrNotFound
		}
		return nil, err
	}
	n := len(results)
	if n == 0 {
		return nil, model.ErrNotFound
	}
	keys := make([]model.Identifier, n)
	for i := 0; i < n; i++ {
		keys[i] = results[i]
	}
	return keys, datastore.DeleteMulti(q.storage.conn.context, results)
}

func (q *Query) Count() (int, error) {
	return q.query.Count(q.storage.conn.context)
}

func (q *Query) Iter() model.Iter {
	return &Iter{q.query.Run(q.storage.conn.context)}
}

type Iter struct {
	iter *datastore.Iterator
}

func (i *Iter) Next(dst interface{}) (model.Identifier, error) {
	return i.iter.Next(dst)
}