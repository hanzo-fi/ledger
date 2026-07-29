package ledger

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hanzo-fi/go-libs/v5/pkg/query"

	"github.com/hanzo-fi/ledger/internal/storage/dialect"
)

// filterAccountAddressOnTransactions states the address filter as the server
// engine writes it, which is what these cases are written against.
func filterAccountAddressOnTransactions(address string, source, destination bool) string {
	store := &Store{dialect: dialect.SQL{}}
	return store.filterAddressOnTransactions(address, source, destination).SQL
}

// filterAddressCarries reports whether an address reaches the query as a bound
// argument rather than as text written into the statement.
func filterAddressCarries(address string, source, destination bool) (string, []any) {
	store := &Store{dialect: dialect.SQL{}}
	fragment := store.filterAddressOnTransactions(address, source, destination)
	return fragment.SQL, fragment.Args
}

func TestCanPushAddressFilterToLateral(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		builder  query.Builder
		expected bool
	}{
		{
			name:     "nil builder",
			builder:  nil,
			expected: true,
		},
		{
			name:     "simple $match on address",
			builder:  query.Match("address", "users:"),
			expected: true,
		},
		{
			name:     "simple $match on account alias",
			builder:  query.Match("account", "users:"),
			expected: true,
		},
		{
			name:     "$and with address and metadata",
			builder:  query.And(query.Match("address", "users:"), query.Match("metadata[category]", "premium")),
			expected: true,
		},
		{
			name:     "$not with address - unsafe",
			builder:  query.Not(query.Match("address", "users:")),
			expected: false,
		},
		{
			name:     "$not with metadata only - safe",
			builder:  query.Not(query.Match("metadata[x]", "y")),
			expected: true,
		},
		{
			name:     "$or with two addresses - safe (all branches have address)",
			builder:  query.Or(query.Match("address", "users:"), query.Match("address", "world")),
			expected: true,
		},
		{
			name:     "$or single item - safe (no-op wrapper)",
			builder:  query.Or(query.Match("address", "users:")),
			expected: true,
		},
		{
			name:     "$or with address and balance - unsafe (mixed)",
			builder:  query.Or(query.Match("address", "users:"), query.Lte("balance[USD]", 0)),
			expected: false,
		},
		{
			name:     "$and with address and $not on metadata - safe (production case)",
			builder:  query.And(query.And(query.Or(query.Match("account", "xxx:")), query.Not(query.Match("metadata[wallet_transaction_method]", "hold_settlement")))),
			expected: true,
		},
		{
			name:     "$and with address and $not on address - unsafe",
			builder:  query.And(query.Match("address", "users:"), query.Not(query.Match("address", "world"))),
			expected: false,
		},
		{
			name:     "nested $not on metadata inside $and - safe",
			builder:  query.And(query.Match("address", "users:"), query.Not(query.Match("metadata[x]", "y"))),
			expected: true,
		},
		{
			name:     "nested $or on metadata inside $and - safe (no address in $or)",
			builder:  query.And(query.Match("address", "users:"), query.Or(query.Match("metadata[x]", "y"), query.Match("metadata[z]", "w"))),
			expected: true,
		},
		{
			name:     "$not wrapping $or with address - unsafe",
			builder:  query.Not(query.Or(query.Match("address", "a:"), query.Match("address", "b:"))),
			expected: false,
		},
		{
			name:     "$not wrapping $and with address - unsafe",
			builder:  query.Not(query.And(query.Match("address", "a:"), query.Match("metadata[x]", "y"))),
			expected: false,
		},
		{
			name:     "$not wrapping $or without address - safe",
			builder:  query.Not(query.Or(query.Match("metadata[x]", "y"), query.Match("metadata[z]", "w"))),
			expected: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			result := canPushAddressFilterToLateral(tc.builder)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestBuildAddressFilterForLateral(t *testing.T) {
	t.Parallel()

	t.Run("single exact address", func(t *testing.T) {
		t.Parallel()
		result := buildAddressFilterForLateral([]string{"world"})
		assert.Equal(t, "address = 'world'", result)
	})

	t.Run("single partial address", func(t *testing.T) {
		t.Parallel()
		result := buildAddressFilterForLateral([]string{"users:"})
		assert.Contains(t, result, "address_array @@")
	})

	t.Run("single partial address with fixed segment", func(t *testing.T) {
		t.Parallel()
		result := buildAddressFilterForLateral([]string{"system:cashier:"})
		assert.Contains(t, result, `address_array @@ ('$[0] == "system"')::jsonpath`)
		assert.Contains(t, result, `address_array @@ ('$[1] == "cashier"')::jsonpath`)
	})

	t.Run("multiple addresses joined with OR", func(t *testing.T) {
		t.Parallel()
		result := buildAddressFilterForLateral([]string{"world", "users:"})
		assert.Contains(t, result, "(address = 'world')")
		assert.Contains(t, result, " OR ")
		assert.Contains(t, result, "address_array @@")
	})
}

func TestCollectAddressFilters(t *testing.T) {
	t.Parallel()

	t.Run("no address filter", func(t *testing.T) {
		t.Parallel()
		mock := &mockUseFilter{filters: map[string][]any{}}
		addresses, needSegments := collectAddressFilters(mock)
		assert.Empty(t, addresses)
		assert.False(t, needSegments)
	})

	t.Run("single exact address", func(t *testing.T) {
		t.Parallel()
		mock := &mockUseFilter{filters: map[string][]any{
			"address": {"world"},
		}}
		addresses, needSegments := collectAddressFilters(mock)
		require.Len(t, addresses, 1)
		assert.Equal(t, "world", addresses[0])
		assert.False(t, needSegments)
	})

	t.Run("single partial address", func(t *testing.T) {
		t.Parallel()
		mock := &mockUseFilter{filters: map[string][]any{
			"address": {"users:"},
		}}
		addresses, needSegments := collectAddressFilters(mock)
		require.Len(t, addresses, 1)
		assert.Equal(t, "users:", addresses[0])
		assert.True(t, needSegments)
	})

	t.Run("mixed exact and partial addresses", func(t *testing.T) {
		t.Parallel()
		mock := &mockUseFilter{filters: map[string][]any{
			"address": {"world", "users:"},
		}}
		addresses, needSegments := collectAddressFilters(mock)
		require.Len(t, addresses, 2)
		assert.Equal(t, "world", addresses[0])
		assert.Equal(t, "users:", addresses[1])
		assert.True(t, needSegments)
	})
}

func TestEscapeSQL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input    string
		expected string
	}{
		{"world", "world"},
		{"it's", "it''s"},
		{"a'b'c", "a''b''c"},
		{"no quotes here", "no quotes here"},
		{"' OR '1'='1", "'' OR ''1''=''1"},
	}
	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.expected, escapeSQL(tc.input))
		})
	}
}

func TestEscapeJSONPath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"plain segment", "foo", "foo"},
		{"double quote", `fo"o`, `fo\"o`},
		{"backslash", `fo\o`, `fo\\o`},
		{"backslash then double quote", `\""`, `\\\"` + `\"`},
		{"single quote", "fo'o", "fo''o"},
		{"double quote and single quote", `f"o'b`, `f\"o''b`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.expected, escapeJSONPath(tc.input))
		})
	}
}

func TestFilterAccountAddress_NormalCases(t *testing.T) {
	t.Parallel()

	t.Run("exact address produces equality condition", func(t *testing.T) {
		t.Parallel()
		result := filterAccountAddress("world", "address")
		assert.Equal(t, "address = 'world'", result)
	})

	t.Run("two-segment exact address", func(t *testing.T) {
		t.Parallel()
		result := filterAccountAddress("users:alice", "address")
		assert.Equal(t, "address = 'users:alice'", result)
	})

	t.Run("partial address with trailing colon matches on segment", func(t *testing.T) {
		t.Parallel()
		result := filterAccountAddress("users:", "address")
		assert.Equal(t, `address_array @@ ('$[0] == "users"')::jsonpath`, result)
	})

	t.Run("partial address with two fixed segments", func(t *testing.T) {
		t.Parallel()
		result := filterAccountAddress("users:alice:", "address")
		assert.Contains(t, result, `$[0] == "users"`)
		assert.Contains(t, result, `$[1] == "alice"`)
	})

	t.Run("wildcard suffix constrains length and segment", func(t *testing.T) {
		t.Parallel()
		// "users:..." means exactly 2 segments with "users" at position 0
		result := filterAccountAddress("users:...", "address")
		assert.Contains(t, result, "jsonb_array_length(address_array) = 2")
		assert.Contains(t, result, `$[0] == "users"`)
	})

	t.Run("key prefix is applied correctly", func(t *testing.T) {
		t.Parallel()
		result := filterAccountAddress("world", "destination")
		assert.Equal(t, "destination = 'world'", result)
	})
}

func TestFilterAccountAddressOnTransactions_NormalCases(t *testing.T) {
	t.Parallel()

	t.Run("exact address source only", func(t *testing.T) {
		t.Parallel()
		sql, args := filterAddressCarries("world", true, false)
		assert.Equal(t, "sources @> ?", sql)
		assert.Equal(t, []any{`["world"]`}, args)
	})

	t.Run("exact address destination only", func(t *testing.T) {
		t.Parallel()
		sql, args := filterAddressCarries("world", false, true)
		assert.Equal(t, "destinations @> ?", sql)
		assert.Equal(t, []any{`["world"]`}, args)
	})

	t.Run("exact address source and destination joined with or", func(t *testing.T) {
		t.Parallel()
		sql, args := filterAddressCarries("world", true, true)
		assert.Equal(t, "sources @> ? or destinations @> ?", sql)
		assert.Equal(t, []any{`["world"]`, `["world"]`}, args)
	})

	t.Run("partial address matches the exploded array", func(t *testing.T) {
		t.Parallel()
		sql, args := filterAddressCarries("users:", true, false)
		assert.Equal(t, "sources_arrays @> ?", sql)
		assert.Equal(t, []any{`[{"0":"users","2":null}]`}, args)
	})
}

func TestFilterAccountAddress_SQLInjection(t *testing.T) {
	t.Parallel()

	t.Run("exact address with single quote is escaped", func(t *testing.T) {
		t.Parallel()
		result := filterAccountAddress("it's:me", "address")
		// Must not contain an unescaped lone single quote that would break the literal
		assert.Contains(t, result, "''s")
		assert.NotContains(t, result, "address = 'it's")
	})

	t.Run("injection payload in exact address is neutralised", func(t *testing.T) {
		t.Parallel()
		payload := "doesnt_exist' OR '1'='1"
		result := filterAccountAddress(payload, "address")
		assert.Equal(t, "address = 'doesnt_exist'' OR ''1''=''1'", result)
	})

	t.Run("partial address segment with double quote is escaped in jsonpath", func(t *testing.T) {
		t.Parallel()
		result := filterAccountAddress(`users:"admin":`, "address")
		assert.Contains(t, result, `$[1] == "\"admin\""`)
	})

	t.Run("partial address segment with backslash is escaped", func(t *testing.T) {
		t.Parallel()
		result := filterAccountAddress(`users:\foo:`, "address")
		assert.Contains(t, result, `$[1] == "\\foo"`)
	})

	t.Run("partial address segment with single quote is escaped", func(t *testing.T) {
		t.Parallel()
		result := filterAccountAddress("users:it's:", "address")
		assert.Contains(t, result, "''s")
	})
}

func TestFilterAccountAddressOnTransactions_SQLInjection(t *testing.T) {
	t.Parallel()

	// An address never becomes SQL text. It is bound, so a quote in it is data
	// on the wire and closes nothing - which is stronger than escaping it.
	for _, address := range []string{
		"it's",
		"world' OR '1'='1",
		"users:it's:",
		"users:x' OR '1'='1:",
	} {
		t.Run(address, func(t *testing.T) {
			t.Parallel()

			sql, args := filterAddressCarries(address, true, true)
			assert.NotContains(t, sql, "'", "the statement quotes nothing")
			for _, segment := range strings.Split(address, ":") {
				if segment != "" {
					assert.NotContains(t, sql, segment, "no part of the address is written into the statement")
				}
			}
			require.NotEmpty(t, args, "the address must ride as an argument")
		})
	}
}

// mockUseFilter implements the interface expected by collectAddressFilters.
// It simulates RepositoryHandlerBuildContext.UseFilter behavior:
// iterates all values for the given key and calls each matcher.
type mockUseFilter struct {
	filters map[string][]any
}

func (m *mockUseFilter) UseFilter(key string, matchers ...func(any) bool) bool {
	values, ok := m.filters[key]
	if !ok {
		return false
	}
	if len(matchers) == 0 {
		return true
	}
	for _, value := range values {
		allMatch := true
		for _, matcher := range matchers {
			if !matcher(value) {
				allMatch = false
				break
			}
		}
		if allMatch {
			return true
		}
	}
	return false
}
