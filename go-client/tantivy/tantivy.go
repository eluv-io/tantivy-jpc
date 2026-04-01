package tantivy

// #cgo linux,amd64 LDFLAGS:-L${SRCDIR}/packaged/lib/linux-amd64
// #cgo darwin,amd64 LDFLAGS:-L${SRCDIR}/packaged/lib/darwin-amd64
// #cgo darwin,arm64 LDFLAGS:-L${SRCDIR}/packaged/lib/darwin-aarch64
// #cgo CFLAGS: -I${SRCDIR}/packaged/include
// #cgo LDFLAGS: -ltantivy_jpc -lm -ldl -pthread
// #cgo linux,amd64 LDFLAGS: -Wl,--allow-multiple-definition
//
// #include "tantivy-jpc.h"
// #include <stdlib.h>
import "C"
import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"sync"
	"unsafe"

	"github.com/eluv-io/errors-go"
)

var doOnce sync.Once

func LibInit(directive ...string) {
	var initVal string
	doOnce.Do(func() {
		if len(directive) == 0 {
			initVal = "info"
		} else {
			initVal = directive[0]
		}
		os.Setenv("ELV_RUST_LOG", initVal)
		C.init()
	})
}

func ClearSession(sessionID string) {
	C.term(C.CString(sessionID))
}

func SetKB(k float64, b float64) {
	C.set_k_and_b(C.float(k), C.float(b))
}

type msi = map[string]interface{}

const defaultMemSize = uint32(500000000)

// The ccomsBuf is a raw byte buffer for tantivy-jpc to send results. A single mutex guards its use.
type JPCId struct {
	id       string
	TempDir  string
	ccomsBuf *C.char
	bufLen   int32
}

func (j *JPCId) ID() string {
	return j.id
}

func readResponseBuffer(ptr *C.uchar, length C.uintptr_t) ([]byte, error) {
	if ptr == nil {
		if length == 0 {
			return nil, nil
		}
		return nil, fmt.Errorf("tantivy_jpc returned a nil buffer with length %d", uint64(length))
	}
	if uint64(length) > uint64(math.MaxInt32) {
		return nil, fmt.Errorf("tantivy_jpc returned %d bytes, exceeding the Go cgo byte limit", uint64(length))
	}
	return C.GoBytes(unsafe.Pointer(ptr), C.int(length)), nil
}

func (jpc *JPCId) callTantivy(object, method string, params msi) (string, error) {
	f := map[string]interface{}{
		"id":     jpc.id,
		"jpc":    "1.0",
		"obj":    object,
		"method": method,
		"params": params,
	}
	b, err := json.Marshal(f)
	if err != nil {
		return "", err
	}
	sb := string(b)
	pcJPCParams := C.CString(sb)
	defer C.free(unsafe.Pointer(pcJPCParams))
	var pCDesctination *C.uchar
	var blen C.uintptr_t
	cJPCParams := (*C.uchar)(unsafe.Pointer(pcJPCParams))
	ttret := C.tantivy_jpc(cJPCParams, C.uintptr_t(len(sb)), &pCDesctination, &blen)
	if ttret >= 0 {
		defer C.free_data(ttret)
	}
	returnBytes, readErr := readResponseBuffer(pCDesctination, blen)
	if ttret < 0 {
		if readErr != nil {
			return "", errors.E("Tantivy JPC Failed", errors.K.Invalid, "desc", readErr.Error())
		}
		return "", errors.E("Tantivy JPC Failed", errors.K.Invalid, "desc", string(returnBytes))
	}
	if readErr != nil {
		return "", errors.E("Tantivy JPC Failed", errors.K.Invalid, "desc", readErr.Error())
	}
	returnData := string(returnBytes)
	return returnData, nil
}
