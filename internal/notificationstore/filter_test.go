package notificationstore

import (
	"strings"
	"testing"
)

func TestFilterBindsEveryValueAsParameter(t *testing.T) {
	where, args := Filter{TenantID: "acme", Status: "failed; DROP TABLE notifications", Text: "50%_off"}.whereClause()

	if strings.Contains(where, "DROP TABLE") {
		t.Fatalf("user input leaked into SQL: %s", where)
	}

	if len(args) != 4 || args[1] != "failed; DROP TABLE notifications" || args[3] != `%50\%\_off%` {
		t.Fatalf("unexpected arguments: %#v", args)
	}
}

func TestEmptyFilterMatchesEverything(t *testing.T) {
	where, args := Filter{}.whereClause()

	if where != "TRUE" || len(args) != 0 {
		t.Fatalf("got %q %v", where, args)
	}
}
