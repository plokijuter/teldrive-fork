package cache

import (
	"testing"
	"time"

	"github.com/gotd/td/tg"
)

// Reproduit EXACTEMENT le motif d'appel de
// internal/reader/tg_multi_reader.go:42-46 :
//
//	var location *tg.InputDocumentFileLocation   // nil
//	err = c.cache.Get(c.key, location)           // <-- sans &
//
// Si ce test montre que Get echoue systematiquement, alors le cache de
// location ne sert JAMAIS, et GetLocation (2 RPC) est rappele a chaque
// morceau de 1 Mio au lieu d'une fois par fichier.
func TestCacheLocation_MotifActuel_EchoueToujours(t *testing.T) {
	c := NewMemoryCache(1024 * 1024)
	cle := "location:12345:678"

	origine := &tg.InputDocumentFileLocation{
		ID:            12345,
		AccessHash:    678,
		FileReference: []byte{1, 2, 3, 4},
		ThumbSize:     "",
	}
	if err := c.Set(cle, origine, 30*time.Minute); err != nil {
		t.Fatalf("Set a echoue : %v", err)
	}

	// --- motif ACTUEL du code (sans &) ---
	var actuel *tg.InputDocumentFileLocation
	errActuel := c.Get(cle, actuel)

	// --- motif CORRIGE (avec &) ---
	var corrige *tg.InputDocumentFileLocation
	errCorrige := c.Get(cle, &corrige)

	t.Logf("motif ACTUEL  (Get(cle, location))  -> err=%v, valeur=%v", errActuel, actuel)
	t.Logf("motif CORRIGE (Get(cle, &location)) -> err=%v", errCorrige)

	if errActuel == nil {
		t.Error("INATTENDU : le motif actuel a reussi ; le diagnostic serait faux")
	} else {
		t.Logf("CONFIRME : le motif actuel echoue toujours -> cache inutilisable")
	}

	if errCorrige != nil {
		t.Fatalf("le motif corrige devrait reussir, or : %v", errCorrige)
	}
	if corrige == nil {
		t.Fatal("le motif corrige rend une valeur nil")
	}
	if corrige.ID != origine.ID || corrige.AccessHash != origine.AccessHash {
		t.Fatalf("valeur relue incorrecte : %+v", corrige)
	}
	if string(corrige.FileReference) != string(origine.FileReference) {
		t.Fatalf("FileReference corrompue : %v", corrige.FileReference)
	}
	t.Logf("CONFIRME : le motif corrige relit la location intacte (ID=%d, ref=%v)",
		corrige.ID, corrige.FileReference)
}
