package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/eluv-io/tantivy-jpc/go-client/tantivy"
)

const ofMiceAndMen = `A few miles south of Soledad, the Salinas River drops in close to the hillside
bank and runs deep and green. The water is warm too, for it has slipped twinkling
over the yellow sands in the sunlight before reaching the narrow pool. On one
side of the river the golden foothill slopes curve up to the strong and rocky
Gabilan Mountains, but on the valley side the water is lined with trees—willows
fresh and green with every spring, carrying in their lower leaf junctures the
debris of the winter's flooding; and sycamores with mottled, white, recumbent
limbs and branches that arch over the pool`
const oldMan = "He was an old man who fished alone in a skiff in the Gulf Stream and he had gone eighty-four days now without taking a fish."

func doRun() {
	tantivy.LibInit("info")
	tmpDir, err := os.MkdirTemp("", "tantivy-vector-sync*")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(tmpDir)
	builder, err := tantivy.NewBuilder(tmpDir)
	if err != nil {
		panic(err)
	}
	idxFieldTitle, err := builder.AddTextField("title", tantivy.TEXT, true, true, "", false)
	if err != nil {
		panic(err)
	}
	idxFieldBody, err := builder.AddTextField("body", tantivy.TEXT, true, true, "", false)
	if err != nil {
		panic(err)
	}

	idxFieldOrder, err := builder.AddI64Field("order", tantivy.INT, true, true, true)
	if err != nil {
		panic(err)
	}

	doc, err := builder.Build()
	if err != nil {
		panic(err)
	}
	bodySeed := strings.Repeat(ofMiceAndMen+" ", 24)
	for i := 0; i < 12; i++ {
		docID, err := doc.Create()
		if err != nil {
			panic(err)
		}
		title := fmt.Sprintf("Vector Candidate %02d", i)
		body := fmt.Sprintf("vector-sync-%02d %s %s", i, oldMan, bodySeed)
		if _, err = doc.AddText(idxFieldTitle, title, docID); err != nil {
			panic(err)
		}
		if _, err = doc.AddText(idxFieldBody, body, docID); err != nil {
			panic(err)
		}
		if _, err = doc.AddInt(idxFieldOrder, int64(1000+i), docID); err != nil {
			panic(err)
		}
	}

	idx, err := doc.CreateIndex()
	if err != nil {
		panic(err)
	}
	_, err = idx.SetMultiThreadExecutor(8)
	if err != nil {
		panic(err)
	}

	idw, err := idx.CreateIndexWriter()
	if err != nil {
		panic(err)
	}
	for docID := uint(1); docID <= 12; docID++ {
		if _, err = idw.AddDocument(docID); err != nil {
			panic(err)
		}
	}

	_, err = idw.Commit()
	if err != nil {
		panic(err)
	}

	rb, err := idx.ReaderBuilder()
	if err != nil {
		panic(err)
	}

	qp, err := rb.Searcher()
	if err != nil {
		panic(err)
	}

	_, err = qp.ForIndex([]string{"title", "body", "order"})
	if err != nil {
		panic(err)
	}

	searcher, err := qp.ParseQuery("body:vector-sync")
	if err != nil {
		panic(err)
	}
	docsetJSON, err := searcher.DocsetAll(true, 0)
	if err != nil {
		panic(err)
	}
	var refs struct {
		Docset []tantivy.SearchResultRef `json:"docset"`
	}
	if err = json.Unmarshal([]byte(docsetJSON), &refs); err != nil {
		panic(err)
	}

	vectorSyncJSON, err := searcher.GetDocumentsWithOptions(refs.Docset, tantivy.GetDocumentsOptions{
		SelectFields: []string{"title", "order"},
	})
	if err != nil {
		panic(err)
	}
	batchedJSON, err := searcher.SearchWithOptionsBatched(tantivy.SearchOptions{
		Ordered:      true,
		SelectFields: []string{"title", "order"},
	}, 4)
	if err != nil {
		panic(err)
	}

	var projected []map[string]interface{}
	if err = json.Unmarshal([]byte(vectorSyncJSON), &projected); err != nil {
		panic(err)
	}
	var batched []map[string]interface{}
	if err = json.Unmarshal([]byte(batchedJSON), &batched); err != nil {
		panic(err)
	}

	fmt.Printf("vector-sync refs=%d hydrated=%d direct-bytes=%d batched-bytes=%d\n", len(refs.Docset), len(projected), len(vectorSyncJSON), len(batchedJSON))
	fmt.Printf("first hydrated doc: %s\n", projected[0]["doc"].(map[string]interface{})["title"].([]interface{})[0])

	tantivy.ClearSession(builder.ID())
	fmt.Println("vector sync demo complete")

}

func main() {
	doRun()
}
