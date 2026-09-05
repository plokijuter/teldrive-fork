package reader

import (
	"context"
	"fmt"
	"io"

	"github.com/gotd/td/tg"
	"github.com/tgdrive/teldrive/internal/cache"
	"github.com/tgdrive/teldrive/internal/config"
	"github.com/tgdrive/teldrive/internal/crypt"
	"github.com/tgdrive/teldrive/pkg/models"
	"github.com/tgdrive/teldrive/pkg/types"
)

type Range struct {
	Start, End int64
	PartNo     int64
}

type LinearReader struct {
	ctx         context.Context
	file        *models.File
	parts       []types.Part
	ranges      []Range
	pos         int
	reader      io.ReadCloser
	remaining   int64
	config      *config.TGConfig
	client      *tg.Client
	concurrency int
	cache       cache.Cacher
}

func calculatePartByteRanges(start, end, partSize int64) []Range {
	ranges := make([]Range, 0)
	startPart := start / partSize
	endPart := end / partSize

	for part := startPart; part <= endPart; part++ {
		partStart := max(start-part*partSize, 0)
		partEnd := min(partSize-1, end-part*partSize)
		ranges = append(ranges, Range{
			Start:  partStart,
			End:    partEnd,
			PartNo: part,
		})
	}
	return ranges
}

func NewLinearReader(ctx context.Context,
	client *tg.Client,
	cache cache.Cacher,
	file *models.File,
	parts []types.Part,
	start,
	end int64,
	config *config.TGConfig,
	concurrency int,
) (io.ReadCloser, error) {

	size := parts[0].Size
	if *file.Encrypted {
		size = parts[0].DecryptedSize
	}
	r := &LinearReader{
		ctx:         ctx,
		parts:       parts,
		file:        file,
		remaining:   end - start + 1,
		ranges:      calculatePartByteRanges(start, end, size),
		config:      config,
		client:      client,
		concurrency: concurrency,
		cache:       cache,
	}

	if err := r.initializeReader(); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *LinearReader) Read(p []byte) (int, error) {
	if r.remaining <= 0 {
		return 0, io.EOF
	}

	n, err := r.reader.Read(p)
	r.remaining -= int64(n)

	if err == io.EOF && r.remaining > 0 {
		if err := r.moveToNextPart(); err != nil {
			return n, err
		}
		err = nil
	}

	return n, err
}

func (r *LinearReader) Close() error {
	if r.reader != nil {
		err := r.reader.Close()
		r.reader = nil
		return err
	}
	return nil
}

func (r *LinearReader) initializeReader() error {
	reader, err := r.getPartReader()
	if err != nil {
		return err
	}
	r.reader = reader
	return nil
}

func (r *LinearReader) moveToNextPart() error {
	r.reader.Close()
	r.pos++
	if r.pos < len(r.ranges) {
		return r.initializeReader()
	}
	return io.EOF
}

func (r *LinearReader) getPartReader() (io.ReadCloser, error) {
	currentRange := r.ranges[r.pos]

	// GARDE (2026-08-29). calculatePartByteRanges deduit les indices de part
	// a partir de parts[0].Size en supposant toutes les parts de cette taille.
	// Si l'enregistrement du fichier est incoherent -- taille declaree
	// superieure a ce que ses parts couvrent reellement, ce qui arrive quand un
	// envoi est interrompu et commite avec une liste incomplete -- l'indice
	// calcule depasse la liste et le code paniquait :
	//   panic: index out of range [7] with length 7
	// a CHAQUE requete de lecture, rattrapee par chi mais repetee sans fin.
	// Mesure : S01E01 Thunderbirds, 3 789 981 350 octets declares, 7 parts,
	// lecture impossible au-dela de ~3,67 Go (unexpected EOF).
	//
	// Un enregistrement corrompu doit rendre une erreur exploitable, pas
	// faire tomber le handler.
	if currentRange.PartNo < 0 || int(currentRange.PartNo) >= len(r.parts) {
		return nil, fmt.Errorf("file parts mismatch: part %d requested but file %s has only %d parts (truncated upload -- re-upload needed)",
			currentRange.PartNo, r.file.ID, len(r.parts))
	}

	partId := r.parts[currentRange.PartNo].ID

	chunkSrc := &chunkSource{
		channelId:   *r.file.ChannelId,
		partId:      partId,
		client:      r.client,
		concurrency: r.concurrency,
		cache:       r.cache,
		useCache:    r.config.Stream.LocationCache,
		key:         cache.Key("files", "location", r.file.ID, partId),
	}

	var (
		reader io.ReadCloser
		err    error
	)
	if *r.file.Encrypted {
		// Meme garde qu'au-dessus : sur le chemin chiffre, l'indice vient du
		// meme calcul et deborderait de la meme facon sur un enregistrement
		// incoherent. Inatteignable tant que encrypted=false, mais le laisser
		// nu serait une panique en embuscade.
		if r.ranges[r.pos].PartNo < 0 || int(r.ranges[r.pos].PartNo) >= len(r.parts) {
			return nil, fmt.Errorf("file parts mismatch (encrypted path): part %d requested but file %s has only %d parts",
				r.ranges[r.pos].PartNo, r.file.ID, len(r.parts))
		}
		salt := r.parts[r.ranges[r.pos].PartNo].Salt
		cipher, _ := crypt.NewCipher(r.config.Uploads.EncryptionKey, salt)
		reader, err = cipher.DecryptDataSeek(r.ctx,
			func(ctx context.Context,
				underlyingOffset,
				underlyingLimit int64) (io.ReadCloser, error) {
				var end int64

				if underlyingLimit >= 0 {
					end = min(r.parts[r.ranges[r.pos].PartNo].Size-1, underlyingOffset+underlyingLimit-1)
				}

				if r.concurrency < 2 {
					return newTGReader(r.ctx, underlyingOffset, end, chunkSrc)
				}
				return newTGMultiReader(r.ctx, underlyingOffset, end, r.config, chunkSrc)

			}, currentRange.Start, currentRange.End-currentRange.Start+1)

	} else {
		if r.concurrency < 2 {
			reader, err = newTGReader(r.ctx, currentRange.Start, currentRange.End, chunkSrc)
		} else {
			reader, err = newTGMultiReader(r.ctx, currentRange.Start, currentRange.End, r.config, chunkSrc)
		}

	}
	return reader, err

}
