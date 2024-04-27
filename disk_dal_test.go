package aggd

import (
	"encoding/binary"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestIndexPageSize(t *testing.T) {
	page := indexPage{
		dataLocation: uuid.Must(uuid.NewRandom()),
	}

	assert.Equal(t, indexPageSize, binary.Size(page))
}
