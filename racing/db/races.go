package db

import (
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/golang/protobuf/ptypes"
	_ "github.com/mattn/go-sqlite3"

	"git.neds.sh/matty/entain/racing/proto/racing"
)

// RacesRepo provides repository access to races.
type RacesRepo interface {
	// Init will initialise our races repository.
	Init() error

	// List will return a list of races.
	List(filter *racing.ListRacesRequestFilter, order *racing.ListRacesRequestOrder) ([]*racing.Race, error)
	Get(Id string) (*racing.Race, error)
}

type racesRepo struct {
	db   *sql.DB
	init sync.Once
}

// NewRacesRepo creates a new races repository.
func NewRacesRepo(db *sql.DB) RacesRepo {
	return &racesRepo{db: db}
}

// Init prepares the race repository dummy data.
func (r *racesRepo) Init() error {
	var err error

	r.init.Do(func() {
		// For test/example purposes, we seed the DB with some dummy races.
		err = r.seed()
	})

	return err
}

func (r *racesRepo) Get(Id string) (*racing.Race, error) {
	var (
		race  *racing.Race
		err   error
		args  []interface{}
		query string
	)
	// Build SQL GetRace by ID
	query = getRaceQueries()[racesList]
	query += " WHERE id = ? "
	args = append(args, Id)

	rows, err := r.db.Query(query, args...)

	if err != nil {
		return nil, err
	}
	// Find first race
	rows.Next()
	race, err = r.getDBRace(rows)

	// If find no race, return customised error
	if race == nil {
		err = fmt.Errorf("cannot find race id: %s", Id)
	}
	return race, err
}

func (r *racesRepo) List(filter *racing.ListRacesRequestFilter, order *racing.ListRacesRequestOrder) ([]*racing.Race, error) {
	var (
		err   error
		query string
		args  []interface{}
	)

	query = getRaceQueries()[racesList]
	query, args = r.applyFilter(query, filter)
	query = r.applyOrder(query, order)

	rows, err := r.db.Query(query, args...)
	if err != nil {
		return nil, err
	}

	return r.scanRaces(rows)
}

func (r *racesRepo) applyFilter(query string, filter *racing.ListRacesRequestFilter) (string, []interface{}) {
	var (
		clauses []string
		args    []interface{}
	)

	if filter == nil {
		return query, args
	}
	if len(filter.MeetingIds) > 0 {
		clauses = append(clauses, "meeting_id IN ("+strings.Repeat("?,", len(filter.MeetingIds)-1)+"?)")

		for _, meetingID := range filter.MeetingIds {
			args = append(args, meetingID)
		}
	}

	// Optional filter -> bool Visible
	if filter.Visible != nil {
		clauses = append(clauses, "visible = ?")
		args = append(args, *filter.Visible)
	}

	if len(clauses) != 0 {
		query += " WHERE " + strings.Join(clauses, " AND ")
	}

	return query, args
}

func (r *racesRepo) applyOrder(query string, order *racing.ListRacesRequestOrder) string {

	if order != nil {
		// Provide orderby column name
		if len(order.OrderBy) != 0 {
			query += fmt.Sprintf(" ORDER BY  %s ", order.OrderBy)
		}
		// Provide orderType (ASC or DESC) only when orderby column was given.
		if len(order.OrderBy) != 0 && (order.OrderType == "ASC" || order.OrderType == "DESC") {
			query += order.OrderType
		}
	} else {
		//by default, order by advertised_start_time
		query += " ORDER BY advertised_start_time "
	}
	return query
}

func (m *racesRepo) getDBRace(rows *sql.Rows) (*racing.Race, error) {
	var race racing.Race
	var advertisedStart time.Time

	if err := rows.Scan(&race.Id, &race.MeetingId, &race.Name, &race.Number, &race.Visible, &advertisedStart); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}

		return nil, err
	}

	ts, err := ptypes.TimestampProto(advertisedStart)
	if err != nil {
		return nil, err
	}

	race.AdvertisedStartTime = ts

	if time.Now().After(advertisedStart) {
		// if advertised_start_time is in the past, status set to CLOSE
		race.Status = "CLOSE"
	} else {
		// if advertised_start_time is in the future, status set to OPEN
		race.Status = "OPEN"
	}

	return &race, nil
}

func (m *racesRepo) scanRaces(
	rows *sql.Rows,
) ([]*racing.Race, error) {
	var races []*racing.Race

	for rows.Next() {
		// -----------------------------------------------------------------
		// Refactored this section with extracted function - getDBRace
		// -----------------------------------------------------------------
		race, err := m.getDBRace(rows)
		if err != nil {
			return nil, err
		}
		races = append(races, race)
	}

	return races, nil
}
