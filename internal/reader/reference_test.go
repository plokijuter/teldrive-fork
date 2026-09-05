package reader

import (
	"errors"
	"testing"
)

// Le rattrapage sur reference perimee ne doit se declencher QUE sur cette
// erreur precise. S'il se declenchait sur n'importe quelle erreur, chaque
// echec reseau couterait 2 RPC supplementaires -- soit exactement le
// probleme qu'on vient de corriger, reintroduit par la porte de derriere.
func TestEstReferencePerimee(t *testing.T) {
	cas := []struct {
		nom    string
		err    error
		attend bool
	}{
		{"nil", nil, false},
		{"FILE_REFERENCE_EXPIRED brut", errors.New("FILE_REFERENCE_EXPIRED"), true},
		{"encapsule par gotd", errors.New("rpc error code 400: FILE_REFERENCE_EXPIRED"), true},
		{"variante INVALID", errors.New("FILE_REFERENCE_INVALID"), true},
		{"minuscules", errors.New("file_reference_expired"), true},
		{"FLOOD_WAIT ne doit PAS declencher", errors.New("FLOOD_WAIT (42)"), false},
		{"timeout ne doit PAS declencher", errors.New("context deadline exceeded"), false},
		{"AUTH_KEY ne doit PAS declencher", errors.New("AUTH_KEY_UNREGISTERED"), false},
		{"erreur reseau ne doit PAS declencher", errors.New("connection reset by peer"), false},
	}
	for _, c := range cas {
		got := estReferencePerimee(c.err)
		if got != c.attend {
			t.Errorf("%s : estReferencePerimee = %v, attendu %v", c.nom, got, c.attend)
		} else {
			t.Logf("OK  %-38s -> %v", c.nom, got)
		}
	}
}
