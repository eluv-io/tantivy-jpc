package tantivy

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"github.com/eluv-io/log-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const resultSet1 = `[{"doc":{"body":["He was an old man who fished alone in a skiff in the Gulf Stream and he had gone eighty-four days now without taking a fish. The water was warm but fishless."],"test":[555],"title":["The Old Man and the Sea"]},"score":,"explain":"noexplain"}]`
const resultSetNick = "[{\"doc\":{\"body\":[\"He was an old man who fished alone in a skiff in the Gulf Stream and he had gone eighty-four days now without taking a fish. The water was warm but fishless.\"],\"order\":[1],\"title\":[\"The Old Man and the Sea\"]},\"score\":1.3338714,\"explain\":\"noexplain\"}]"

type jm = map[string]interface{}

func makeFuzzyIndex(tb testing.TB, td string, useExisting bool) *TIndex {
	tb.Helper()
	builder, err := NewBuilder(td)
	require.NoError(tb, err)
	idxFieldTitle, err := builder.AddTextField("title", TEXT, true, true, "", false)
	require.NoError(tb, err)
	require.EqualValues(tb, 0, idxFieldTitle)
	idxFieldInt, err := builder.AddI64Field("test", INT, true, true, false)
	require.NoError(tb, err)
	require.EqualValues(tb, 1, idxFieldInt)

	doc, err := builder.Build()
	require.NoError(tb, err)
	doc1, err := doc.Create()
	require.NoError(tb, err)
	require.EqualValues(tb, 1, doc1)
	doc2, err := doc.Create()
	require.NoError(tb, err)
	require.EqualValues(tb, 2, doc2)
	doc3, err := doc.Create()
	require.NoError(tb, err)
	require.EqualValues(tb, 3, doc3)
	doc4, err := doc.Create()
	require.NoError(tb, err)
	require.EqualValues(tb, 4, doc4)

	_, err = doc.AddText(idxFieldTitle, "The Name of the Wind", doc1)
	require.NoError(tb, err)
	_, err = doc.AddInt(idxFieldInt, 444, doc1)
	require.NoError(tb, err)
	_, err = doc.AddText(idxFieldTitle, "The Diary of Muadib", doc2)
	require.NoError(tb, err)
	_, err = doc.AddInt(idxFieldInt, 555, doc2)
	require.NoError(tb, err)
	_, err = doc.AddText(idxFieldTitle, "A Dairy Cow", doc3)
	require.NoError(tb, err)
	_, err = doc.AddInt(idxFieldInt, 666, doc3)
	require.NoError(tb, err)
	_, err = doc.AddText(idxFieldTitle, "The Diary of a Young Girl", doc4)
	require.NoError(tb, err)
	_, err = doc.AddInt(idxFieldInt, 777, doc4)
	require.NoError(tb, err)

	idx, err := doc.CreateIndex()
	require.NoError(tb, err)
	if !useExisting {
		idw, err := idx.CreateIndexWriter()
		require.NoError(tb, err)
		opst1, err := idw.AddDocument(doc1)
		require.NoError(tb, err)
		require.EqualValues(tb, 0, opst1)
		opst2, err := idw.AddDocument(doc2)
		require.NoError(tb, err)
		require.EqualValues(tb, 1, opst2)
		opst3, err := idw.AddDocument(doc3)
		require.NoError(tb, err)
		require.EqualValues(tb, 2, opst3)
		opst4, err := idw.AddDocument(doc4)
		require.NoError(tb, err)
		require.EqualValues(tb, 3, opst4)

		fmt.Printf("op1 = %v op2 = %v op3 = %v op4 = %v\n ", opst1, opst2, opst3, opst4)
		idCommit, err := idw.Commit()
		require.NoError(tb, err)
		fmt.Printf("commit id = %v", idCommit)
	}
	return idx
}

func makeIndex(tb testing.TB, td string, useExisting bool) *TIndex {
	tb.Helper()
	builder, err := NewBuilder(td)
	require.NoError(tb, err)
	idxFieldTitle, err := builder.AddTextField("title", TEXT, true, true, "", false)
	require.NoError(tb, err)
	require.EqualValues(tb, 0, idxFieldTitle)
	idxFieldBody, err := builder.AddTextField("body", TEXT, true, true, "", false)
	require.NoError(tb, err)
	require.EqualValues(tb, 1, idxFieldBody)
	idxFieldInt, err := builder.AddI64Field("test", INT, true, true, true)
	require.NoError(tb, err)
	require.EqualValues(tb, 2, idxFieldInt)
	doc, err := builder.Build()
	require.NoError(tb, err)
	doc1, err := doc.Create()
	require.NoError(tb, err)
	require.EqualValues(tb, 1, doc1)
	doc2, err := doc.Create()
	require.NoError(tb, err)
	require.EqualValues(tb, 2, doc2)
	_, err = doc.AddText(idxFieldTitle, "The Old Man and the Sea", doc1)
	require.NoError(tb, err)
	_, err = doc.AddText(idxFieldBody, "He was an old man who fished alone in a skiff in the Gulf Stream and he had gone eighty-four days now without taking a fish. The water was warm but fishless.", doc1)
	require.NoError(tb, err)
	_, err = doc.AddInt(idxFieldInt, 555, doc1)
	require.NoError(tb, err)
	_, err = doc.AddText(idxFieldTitle, "Of Mice and Men", doc2)
	require.NoError(tb, err)
	_, err = doc.AddText(idxFieldBody, `A few miles south of Soledad, the Salinas River drops in close to the hillside
	bank and runs deep and green. The water is warm too, for it has slipped twinkling
	over the yellow sands in the sunlight before reaching the narrow pool. On one
	side of the river the golden foothill slopes curve up to the strong and rocky
	Gabilan Mountains, but on the valley side the water is lined with trees—willows
	fresh and green with every spring, carrying in their lower leaf junctures the
	debris of the winter's flooding; and sycamores with mottled, white, recumbent
	limbs and branches that arch over the pool`, doc2)
	require.NoError(tb, err)
	_, err = doc.AddInt(idxFieldInt, 666, doc2)
	require.NoError(tb, err)

	idx, err := doc.CreateIndex()
	require.NoError(tb, err)
	if !useExisting {
		idw, err := idx.CreateIndexWriter()
		require.NoError(tb, err)
		opst1, err := idw.AddDocument(doc1)
		require.NoError(tb, err)
		require.EqualValues(tb, 0, opst1)
		opst2, err := idw.AddDocument(doc2)
		require.NoError(tb, err)
		require.EqualValues(tb, 1, opst2)
		fmt.Printf("op1 = %v op2 = %v\n", opst1, opst2)
		idCommit, err := idw.Commit()
		require.NoError(tb, err)
		fmt.Printf("commit id = %v", idCommit)
	}
	return idx
}

func makeLargeIndex(tb testing.TB, td string, docCount int, bodyRepeat int) *TIndex {
	tb.Helper()
	builder, err := NewBuilder(td)
	require.NoError(tb, err)
	idxFieldTitle, err := builder.AddTextField("title", TEXT, true, true, "", false)
	require.NoError(tb, err)
	require.EqualValues(tb, 0, idxFieldTitle)
	idxFieldBody, err := builder.AddTextField("body", TEXT, true, true, "", false)
	require.NoError(tb, err)
	require.EqualValues(tb, 1, idxFieldBody)
	idxFieldInt, err := builder.AddI64Field("test", INT, true, true, true)
	require.NoError(tb, err)
	require.EqualValues(tb, 2, idxFieldInt)

	doc, err := builder.Build()
	require.NoError(tb, err)

	bodyChunk := strings.Repeat("payload token search demo chunk ", bodyRepeat)
	docIDs := make([]uint, 0, docCount)
	for i := 0; i < docCount; i++ {
		docID, err := doc.Create()
		require.NoError(tb, err)
		docIDs = append(docIDs, docID)

		title := fmt.Sprintf("Large Search Demo %03d", i)
		body := fmt.Sprintf("doc-%03d %s tail-marker-%03d", i, bodyChunk, i)
		_, err = doc.AddText(idxFieldTitle, title, docID)
		require.NoError(tb, err)
		_, err = doc.AddText(idxFieldBody, body, docID)
		require.NoError(tb, err)
		_, err = doc.AddInt(idxFieldInt, int64(1000+i), docID)
		require.NoError(tb, err)
	}

	idx, err := doc.CreateIndex()
	require.NoError(tb, err)
	idw, err := idx.CreateIndexWriter()
	require.NoError(tb, err)
	for i, docID := range docIDs {
		opstamp, err := idw.AddDocument(docID)
		require.NoError(tb, err)
		require.EqualValues(tb, i, opstamp)
	}
	_, err = idw.Commit()
	require.NoError(tb, err)
	return idx
}

func loadIndex(t *testing.T, td string) *TIndex {
	builder, err := NewBuilder(td)
	require.NoError(t, err)
	doc, err := builder.Build()
	require.NoError(t, err)
	idx, err := doc.CreateIndex()
	require.NoError(t, err)
	return idx

}

func testExpectedIndex(t *testing.T, idx *TIndex) {
	rb, err := idx.ReaderBuilder()
	require.NoError(t, err)
	expectedBody := "He was an old man who fished alone in a skiff in the Gulf Stream and he had gone eighty-four days now without taking a fish. The water was warm but fishless."
	qp, err := rb.Searcher()
	require.NoError(t, err)

	_, err = qp.ForIndex([]string{"title", "body"})
	require.NoError(t, err)

	searcher, err := qp.ParseQuery("title:Sea")
	require.NoError(t, err)
	s, err := searcher.Search(false, 0, 0, true)
	require.NoError(t, err)
	results := []map[string]interface{}{}
	err = json.Unmarshal([]byte(s), &results)
	require.NoError(t, err)
	require.EqualValues(t, expectedBody, results[0]["doc"].(map[string]interface{})["body"].([]interface{})[0].(string))

	searcherAgain, err := qp.ParseQuery("body:mottled")
	require.NoError(t, err)
	sAgain, err := searcherAgain.Search(true, 0, 0, true)
	require.NoError(t, err)
	err = json.Unmarshal([]byte(sAgain), &results)
	require.NoError(t, err)
	exp, ok := results[0]["doc"].(map[string]interface{})["title"].([]interface{})[0].(string)
	require.EqualValues(t, true, ok)
	require.EqualValues(t, "Of Mice and Men", exp)
}

func testAltExpectedIndex(t *testing.T, idx *TIndex) {
	rb, err := idx.ReaderBuilder()
	require.NoError(t, err)
	qp, err := rb.Searcher()
	require.NoError(t, err)

	_, err = qp.ForIndex([]string{"title", "body", "test"})
	require.NoError(t, err)

	searcher, err := qp.ParseQuery("title:Sea AND test:555")
	require.NoError(t, err)
	s, err := searcher.Search(false, 0, 0, true)
	require.NoError(t, err)
	results := []map[string]interface{}{}
	err = json.Unmarshal([]byte(s), &results)
	require.NoError(t, err)
	require.EqualValues(t, "The Old Man and the Sea", results[0]["doc"].(map[string]interface{})["title"].([]interface{})[0].(string))

	searcherAgain, err := qp.ParseQuery("body:mottled AND test:666")
	require.NoError(t, err)
	s, err = searcherAgain.Search(true, 0, 0, true)
	require.NoError(t, err)
	err = json.Unmarshal([]byte(s), &results)
	require.NoError(t, err)
	require.EqualValues(t, "Of Mice and Men", results[0]["doc"].(map[string]interface{})["title"].([]interface{})[0].(string))
}

func testExpectedTopIndex(t *testing.T, idx *TIndex) {
	rb, err := idx.ReaderBuilder()
	require.NoError(t, err)

	qp, err := rb.Searcher()
	require.NoError(t, err)

	_, err = qp.ForIndex([]string{"title", "body"})
	require.NoError(t, err)

	searcher, err := qp.ParseQuery("title:Mice OR title:Man")
	require.NoError(t, err)
	s, err := searcher.Search(false, uint64(1), 0, true)
	require.NoError(t, err)
	var res []interface{}
	err = json.Unmarshal([]byte(s), &res)
	require.NoError(t, err)
	require.EqualValues(t, 1, len(res))
}

func testFuzzyExpectedIndex(t *testing.T, idx *TIndex) {
	rb, err := idx.ReaderBuilder()
	require.NoError(t, err)

	qp, err := rb.Searcher()
	require.NoError(t, err)

	searcher, err := qp.ParseFuzzyQuery("title", "Diari")
	require.NoError(t, err)
	s, err := searcher.FuzzySearch()
	log.Info("return", s)
	require.NoError(t, err)
	resultSet := []interface{}{}
	err = json.Unmarshal([]byte(s), &resultSet)
	require.NoError(t, err)
	compareResults(t, resultSet)

}

func compareResults(t *testing.T, res []interface{}) {
	require.EqualValues(t, 2, len(res))
	for _, v := range res {
		innerArray := v.(jm)["doc"].(jm)["field_values"]
		innerMap := innerArray.([]interface{})[0].(jm)
		inner := innerMap["value"].(string)
		b := inner == "The Diary of a Young Girl" || inner == "The Diary of Muadib"
		require.EqualValues(t, true, b)
	}

}
func TestTantivyBasic(t *testing.T) {
	wd, err := os.Getwd()
	require.NoError(t, err)
	fmt.Printf("WD = %s", wd)
	t.Setenv("LD_LIBRARY_PATH", ".")
	LibInit()
	td, err := ioutil.TempDir("", "tindex*")
	defer func(err error) {
		if err == nil {
			if os.RemoveAll(td) != nil {
				log.Error("unable to cleanup temp dir", "val", td)
			}
		}
	}(err)
	assert.NoError(t, err)
	idx := makeIndex(t, td, false)
	testExpectedIndex(t, idx)
}

func TestTantivyIntField(t *testing.T) {
	t.Setenv("LD_LIBRARY_PATH", ".")
	LibInit()
	idx := makeIndex(t, "", false)
	testAltExpectedIndex(t, idx)
}

func TestSchema(t *testing.T) {
	builder, err := NewBuilder("")
	require.NoError(t, err)
	idxFieldTitle, err := builder.AddTextField("title", TEXT, true, true, "", true)
	require.NoError(t, err)
	require.EqualValues(t, 0, idxFieldTitle)
	idxFieldBody, err := builder.AddTextField("body", TEXT, true, true, "", true)
	require.NoError(t, err)
	require.EqualValues(t, 1, idxFieldBody)
	idxFieldBody2, err := builder.AddTextField("body2", TEXT, true, true, "", true)
	require.NoError(t, err)
	require.EqualValues(t, 2, idxFieldBody2)
	doc, err := builder.Build()
	require.NoError(t, err)
	doc1, err := doc.Create()
	require.NoError(t, err)
	require.EqualValues(t, 1, doc1)
	doc2, err := doc.Create()
	require.NoError(t, err)
	require.EqualValues(t, 2, doc2)
	_, err = doc.AddText(idxFieldTitle, "The Old Man and the Sea", doc1)
	require.NoError(t, err)
	_, err = doc.AddText(idxFieldBody, "He was an old man who fished alone in a skiff in the Gulf Stream and he had gone eighty-four days now without taking a fish. The water was warm but fishless.", doc1)
	require.NoError(t, err)
	_, err = doc.AddText(idxFieldBody2, "He was an old man who fished alone in a skiff in the Gulf Stream and he had gone eighty-four days now without taking a fish. The water was warm but fishless.", doc1)
	require.NoError(t, err)
	_, err = doc.AddText(idxFieldTitle, "Of Mice and Men", doc2)
	require.NoError(t, err)
	_, err = doc.AddText(idxFieldBody, `A few miles south of Soledad, the Salinas River drops in close to the hillside
	bank and runs deep and green. The water is warm too, for it has slipped twinkling
	over the yellow sands in the sunlight before reaching the narrow pool. On one
	side of the river the golden foothill slopes curve up to the strong and rocky
	Gabilan Mountains, but on the valley side the water is lined with trees—willows
	fresh and green with every spring, carrying in their lower leaf junctures the
	debris of the winter's flooding; and sycamores with mottled, white, recumbent
	limbs and branches that arch over the pool`, doc2)
	require.NoError(t, err)
	_, err = doc.AddText(idxFieldBody2, `A few miles south of Soledad, the Salinas River drops in close to the hillside
	bank and runs deep and green. The water is warm too, for it has slipped twinkling
	over the yellow sands in the sunlight befjmore reaching the narrow pool. On one
	side of the river the golden foothill slopes curve up to the strong and rocky
	Gabilan Mountains, but on the valley side the water is lined with trees—willows
	fresh and green with every spring, carrying in their lower leaf junctures the
	debris of the winter's flooding; and sycamores with mottled, white, recumbent
	limbs and branches that arch over the pool`, doc2)
	require.NoError(t, err)

	indexer, err := doc.CreateIndex()
	require.NoError(t, err)
	schema := indexer.GetSchema()
	n, err := schema.NumFields()
	require.NoError(t, err)
	require.EqualValues(t, 3, n)
	log.Info("fields", "val", n)
	fe, err := schema.GetFieldEntry("body2")
	require.NoError(t, err)
	require.EqualValues(t, "body2", fe.Name)
	require.EqualValues(t, "text", fe.Type)
	require.EqualValues(t, true, fe.Options["stored"])
	fields, err := schema.Fields()
	require.NoError(t, err)
	require.EqualValues(t, "title", fields["0"].(msi)["name"])
	require.EqualValues(t, "body", fields["1"].(msi)["name"])
	require.EqualValues(t, "body2", fields["2"].(msi)["name"])
	log.Info("fields=", fields)
	afield, err := schema.GetField("body2")
	require.NoError(t, err)
	require.EqualValues(t, 2, afield)

}

func TestSnippetSearch(t *testing.T) {
	builder, err := NewBuilder("")
	require.NoError(t, err)
	idxFieldTitle, err := builder.AddTextField("title", TEXT, true, true, "", false)
	require.NoError(t, err)
	require.EqualValues(t, 0, idxFieldTitle)
	idxFieldBody, err := builder.AddTextField("body", TEXT, true, true, "", false)
	require.NoError(t, err)
	require.EqualValues(t, 1, idxFieldBody)
	doc, err := builder.Build()
	require.NoError(t, err)
	doc1, err := doc.Create()
	require.NoError(t, err)
	require.EqualValues(t, 1, doc1)
	doc2, err := doc.Create()
	require.NoError(t, err)
	require.EqualValues(t, 2, doc2)
	_, err = doc.AddText(idxFieldTitle, "The Old Man and the Sea", doc1)
	require.NoError(t, err)
	_, err = doc.AddText(idxFieldBody, "He was an old man who fished alone in a skiff in the Gulf Stream and he had gone eighty-four days now without taking a fish. The water was warm but fishless.", doc1)
	require.NoError(t, err)
	_, err = doc.AddText(idxFieldTitle, "Of Mice and Men", doc2)
	require.NoError(t, err)
	_, err = doc.AddText(idxFieldBody, `A few miles south of Soledad, the Salinas River drops in close to the hillside
	bank and runs deep and green. The water is warm too, for it has slipped twinkling
	over the yellow sands in the sunlight before reaching the narrow pool. On one
	side of the river the golden foothill slopes curve up to the strong and rocky
	Gabilan Mountains, but on the valley side the water is lined with trees—willows
	fresh and green with every spring, carrying in their lower leaf junctures the
	debris of the winter's flooding; and sycamores with mottled, white, recumbent
	limbs and branches that arch over the pool`, doc2)
	require.NoError(t, err)
	indexer, err := doc.CreateIndex()
	require.NoError(t, err)

	idw, err := indexer.CreateIndexWriter()
	require.NoError(t, err)
	opst1, err := idw.AddDocument(doc1)
	require.NoError(t, err)
	require.EqualValues(t, 0, opst1)
	opst2, err := idw.AddDocument(doc2)
	require.NoError(t, err)
	require.EqualValues(t, 1, opst2)
	fmt.Printf("op1 = %v op2 = %v\n", opst1, opst2)
	idCommit, err := idw.Commit()
	require.NoError(t, err)
	fmt.Printf("commit id = %v", idCommit)

	rb, err := indexer.ReaderBuilder()
	require.NoError(t, err)
	qp, err := rb.Searcher()
	require.NoError(t, err)

	_, err = qp.ForIndex([]string{"title", "body"})
	require.NoError(t, err)

	searcher, err := qp.ParseQuery("sycamores")
	require.NoError(t, err)
	s, err := searcher.Search(false, 4, 0, false, "body")
	require.NoError(t, err)
	results := []map[string]interface{}{}
	err = json.Unmarshal([]byte(s), &results)
	require.NoError(t, err)
	require.EqualValues(t, "Of Mice and Men", results[0]["doc"].(map[string]interface{})["title"].([]interface{})[0].(string))
	require.EqualValues(t, "A few miles south of Soledad, the Salinas River drops in close to the hillside\n\tbank and runs deep and green. The water is warm too, for it has slipped twinkling\n\tover the yellow sands in the sunlight before reaching the narrow pool. On one\n\tside of the river the golden foothill slopes curve up to the strong and rocky\n\tGabilan Mountains, but on the valley side the water is lined with trees—willows\n\tfresh and green with every spring, carrying in their lower leaf junctures the\n\tdebris of the winter&#x27;s flooding; and <b>sycamores</b> with mottled, white, recumbent\n\tlimbs and branches that arch over the pool", results[0]["snippet_html"].(jm)["body"])
}

func TestRawSearch(t *testing.T) {
	builder, err := NewBuilder("")
	require.NoError(t, err)
	idxFieldTitle, err := builder.AddTextField("title", TEXT, true, true, "", false)
	require.NoError(t, err)
	require.EqualValues(t, 0, idxFieldTitle)
	idxFieldBody, err := builder.AddTextField("body", TEXT, true, true, "", false)
	require.NoError(t, err)
	require.EqualValues(t, 1, idxFieldBody)
	idxFieldInt, err := builder.AddI64Field("order", INT, true, true, true)
	require.NoError(t, err)
	require.EqualValues(t, 2, idxFieldInt)
	doc, err := builder.Build()
	require.NoError(t, err)
	doc1, err := doc.Create()
	require.NoError(t, err)
	require.EqualValues(t, 1, doc1)
	doc2, err := doc.Create()
	require.NoError(t, err)
	require.EqualValues(t, 2, doc2)
	_, err = doc.AddText(idxFieldTitle, "The Old Man and the Sea", doc1)
	require.NoError(t, err)
	_, err = doc.AddText(idxFieldBody, "He was an old man who fished alone in a skiff in the Gulf Stream and he had gone eighty-four days now without taking a fish. The water was warm but fishless.", doc1)
	require.NoError(t, err)
	_, err = doc.AddInt(idxFieldInt, 1, doc1)
	require.NoError(t, err)
	_, err = doc.AddText(idxFieldTitle, "Of Mice and Men", doc2)
	require.NoError(t, err)
	_, err = doc.AddText(idxFieldBody, `A few miles south of Soledad, the Salinas River drops in close to the hillside
	bank and runs deep and green. The water is warm too, for it has slipped twinkling
	over the yellow sands in the sunlight before reaching the narrow pool. On one
	side of the river the golden foothill slopes curve up to the strong and rocky
	Gabilan Mountains, but on the valley side the water is lined with trees—willows
	fresh and green with every spring, carrying in their lower leaf junctures the
	debris of the winter's flooding; and sycamores with mottled, white, recumbent
	limbs and branches that arch over the pool`, doc2)
	require.NoError(t, err)
	_, err = doc.AddInt(idxFieldInt, 2, doc2)
	require.NoError(t, err)

	indexer, err := doc.CreateIndex()
	require.NoError(t, err)

	idw, err := indexer.CreateIndexWriter()
	require.NoError(t, err)
	opst1, err := idw.AddDocument(doc1)
	require.NoError(t, err)
	require.EqualValues(t, 0, opst1)
	opst2, err := idw.AddDocument(doc2)
	require.NoError(t, err)
	require.EqualValues(t, 1, opst2)
	fmt.Printf("op1 = %v op2 = %v\n", opst1, opst2)
	idCommit, err := idw.Commit()
	require.NoError(t, err)
	fmt.Printf("commit id = %v", idCommit)

	rb, err := indexer.ReaderBuilder()
	require.NoError(t, err)
	qp, err := rb.Searcher()
	require.NoError(t, err)

	_, err = qp.ForIndex([]string{"title", "body", "order"})
	require.NoError(t, err)

	searcher, err := qp.ParseQuery("order:1")
	require.NoError(t, err)
	s, err := searcher.SearchRaw()
	require.NoError(t, err)
	results := []map[string]interface{}{}
	err = json.Unmarshal([]byte(s), &results)
	require.NoError(t, err)
	require.EqualValues(t, "The Old Man and the Sea", results[0]["title"].([]interface{})[0].(string))

	searcherAgain, err := qp.ParseQuery("order:2")
	require.NoError(t, err)
	s, err = searcherAgain.SearchRaw()
	require.NoError(t, err)
	err = json.Unmarshal([]byte(s), &results)
	require.NoError(t, err)
	require.EqualValues(t, "Of Mice and Men", results[0]["title"].([]interface{})[0].(string))
}

func TestDocsetSearch(t *testing.T) {
	builder, err := NewBuilder("")
	require.NoError(t, err)
	idxFieldTitle, err := builder.AddTextField("title", TEXT, true, true, "", false)
	require.NoError(t, err)
	require.EqualValues(t, 0, idxFieldTitle)
	idxFieldBody, err := builder.AddTextField("body", TEXT, true, true, "", false)
	require.NoError(t, err)
	require.EqualValues(t, 1, idxFieldBody)
	idxFieldInt, err := builder.AddI64Field("order", INT, true, true, true)
	require.NoError(t, err)
	require.EqualValues(t, 2, idxFieldInt)
	doc, err := builder.Build()
	require.NoError(t, err)
	doc1, err := doc.Create()
	require.NoError(t, err)
	require.EqualValues(t, 1, doc1)
	doc2, err := doc.Create()
	require.NoError(t, err)
	require.EqualValues(t, 2, doc2)
	_, err = doc.AddText(idxFieldTitle, "The Old Man and the Sea", doc1)
	require.NoError(t, err)
	_, err = doc.AddText(idxFieldBody, "He was an old man who fished alone in a skiff in the Gulf Stream and he had gone eighty-four days now without taking a fish. The water was warm but fishless.", doc1)
	require.NoError(t, err)
	_, err = doc.AddInt(idxFieldInt, 1, doc1)
	require.NoError(t, err)
	_, err = doc.AddText(idxFieldTitle, "Of Mice and Men", doc2)
	require.NoError(t, err)
	_, err = doc.AddText(idxFieldBody, `A few miles south of Soledad, the Salinas River drops in close to the hillside
	bank and runs deep and green. The water is warm too, for it has slipped twinkling
	over the yellow sands in the sunlight before reaching the narrow pool. On one
	side of the river the golden foothill slopes curve up to the strong and rocky
	Gabilan Mountains, but on the valley side the water is lined with trees—willows
	fresh and green with every spring, carrying in their lower leaf junctures the
	debris of the winter's flooding; and sycamores with mottled, white, recumbent
	limbs and branches that arch over the pool`, doc2)
	require.NoError(t, err)
	_, err = doc.AddInt(idxFieldInt, 2, doc2)
	require.NoError(t, err)

	indexer, err := doc.CreateIndex()
	require.NoError(t, err)

	idw, err := indexer.CreateIndexWriter()
	require.NoError(t, err)
	opst1, err := idw.AddDocument(doc1)
	require.NoError(t, err)
	require.EqualValues(t, 0, opst1)
	opst2, err := idw.AddDocument(doc2)
	require.NoError(t, err)
	require.EqualValues(t, 1, opst2)
	fmt.Printf("op1 = %v op2 = %v\n", opst1, opst2)
	idCommit, err := idw.Commit()
	require.NoError(t, err)
	fmt.Printf("commit id = %v", idCommit)

	rb, err := indexer.ReaderBuilder()
	require.NoError(t, err)
	qp, err := rb.Searcher()
	require.NoError(t, err)

	_, err = qp.ForIndex([]string{"title", "body", "order"})
	require.NoError(t, err)

	searcher, err := qp.ParseQuery("title:Sea OR title:Mice")
	require.NoError(t, err)
	s, err := searcher.Docset(true, 20, 0)
	require.NoError(t, err)
	results := map[string]interface{}{}
	err = json.Unmarshal([]byte(s), &results)
	require.NoError(t, err)
	resElement, ok := results["docset"].([]interface{})[0].(jm)
	require.EqualValues(t, true, ok)
	sDoc, err := searcher.GetDocument(true, float32(resElement["score"].(float64)), uint32(resElement["doc_id"].(float64)), uint32(resElement["segment_ord"].(float64)))
	require.NoError(t, err)
	log.Info(sDoc)
	err = json.Unmarshal([]byte(sDoc), &results)
	require.NoError(t, err)

	require.EqualValues(t, "Of Mice and Men", results["doc"].(jm)["title"].([]interface{})[0].(string))
	resElement, ok = results["docset"].([]interface{})[1].(jm)
	require.EqualValues(t, true, ok)
	//using NOSNIPPET to show it works as well
	sDoc, err = searcher.GetDocument(true, float32(resElement["score"].(float64)), uint32(resElement["doc_id"].(float64)), uint32(resElement["segment_ord"].(float64)))
	require.NoError(t, err)
	log.Info(sDoc)
	err = json.Unmarshal([]byte(sDoc), &results)
	require.NoError(t, err)

	require.EqualValues(t, "The Old Man and the Sea", results["doc"].(jm)["title"].([]interface{})[0].(string))

}

func TestDocsetSnippetSearch(t *testing.T) {
	builder, err := NewBuilder("")
	require.NoError(t, err)
	idxFieldTitle, err := builder.AddTextField("title", TEXT, true, true, "", false)
	require.NoError(t, err)
	require.EqualValues(t, 0, idxFieldTitle)
	idxFieldBody, err := builder.AddTextField("body", TEXT, true, true, "", false)
	require.NoError(t, err)
	require.EqualValues(t, 1, idxFieldBody)
	idxFieldBody2, err := builder.AddTextField("body2", TEXT, true, true, "", false)
	require.NoError(t, err)
	require.EqualValues(t, 2, idxFieldBody2)
	doc, err := builder.Build()
	require.NoError(t, err)
	doc1, err := doc.Create()
	require.NoError(t, err)
	require.EqualValues(t, 1, doc1)
	doc2, err := doc.Create()
	require.NoError(t, err)
	require.EqualValues(t, 2, doc2)
	_, err = doc.AddText(idxFieldTitle, "The Old Man and the Sea", doc1)
	require.NoError(t, err)
	_, err = doc.AddText(idxFieldBody, "He was an old man who fished alone in a skiff in the Gulf Stream and he had gone eighty-four days now without taking a fish. The water was warm but fishless.", doc1)
	require.NoError(t, err)
	_, err = doc.AddText(idxFieldBody2, "He was an old man who fished alone in a skiff in the Gulf Stream and he had gone eighty-four days now without taking a fish. The water was warm but fishless.", doc1)
	require.NoError(t, err)
	_, err = doc.AddText(idxFieldTitle, "Of Mice and Men", doc2)
	require.NoError(t, err)
	_, err = doc.AddText(idxFieldBody, `A few miles south of Soledad, the Salinas River drops in close to the hillside
	bank and runs deep and green. The water is warm too, for it has slipped twinkling
	over the yellow sands in the sunlight before reaching the narrow pool. On one
	side of the river the golden foothill slopes curve up to the strong and rocky
	Gabilan Mountains, but on the valley side the water is lined with trees—willows
	fresh and green with every spring, carrying in their lower leaf junctures the
	debris of the winter's flooding; and sycamores with mottled, white, recumbent
	limbs and branches that arch over the pool`, doc2)
	require.NoError(t, err)
	_, err = doc.AddText(idxFieldBody2, `A few miles south of Soledad, the Salinas River drops in close to the hillside
	bank and runs deep and green. The water is warm too, for it has slipped twinkling
	over the yellow sands in the sunlight before reaching the narrow pool. On one
	side of the river the golden foothill slopes curve up to the strong and rocky
	Gabilan Mountains, but on the valley side the water is lined with trees—willows
	fresh and green with every spring, carrying in their lower leaf junctures the
	debris of the winter's flooding; and sycamores with mottled, white, recumbent
	limbs and branches that arch over the pool`, doc2)
	require.NoError(t, err)

	indexer, err := doc.CreateIndex()
	require.NoError(t, err)

	idw, err := indexer.CreateIndexWriter()
	require.NoError(t, err)
	opst1, err := idw.AddDocument(doc1)
	require.NoError(t, err)
	require.EqualValues(t, 0, opst1)
	opst2, err := idw.AddDocument(doc2)
	require.NoError(t, err)
	require.EqualValues(t, 1, opst2)
	fmt.Printf("op1 = %v op2 = %v\n", opst1, opst2)
	idCommit, err := idw.Commit()
	require.NoError(t, err)
	fmt.Printf("commit id = %v", idCommit)

	rb, err := indexer.ReaderBuilder()
	require.NoError(t, err)
	qp, err := rb.Searcher()
	require.NoError(t, err)

	_, err = qp.ForIndex([]string{"title", "body", "body2"})
	require.NoError(t, err)

	searcher, err := qp.ParseQuery("twinkling")
	require.NoError(t, err)
	s, err := searcher.Docset(true, 20, 0)
	require.NoError(t, err)
	results := map[string]interface{}{}
	err = json.Unmarshal([]byte(s), &results)
	require.NoError(t, err)
	resElement, ok := results["docset"].([]interface{})[0].(jm)
	require.EqualValues(t, true, ok)
	sDoc, err := searcher.GetDocument(true, float32(resElement["score"].(float64)), uint32(resElement["doc_id"].(float64)), uint32(resElement["segment_ord"].(float64)), "body", "body2")
	require.NoError(t, err)
	log.Info(sDoc)
	err = json.Unmarshal([]byte(sDoc), &results)
	require.NoError(t, err)
	resDoc := results["doc"].(jm)
	require.EqualValues(t, "Of Mice and Men", resDoc["title"].([]interface{})[0].(string))
	require.EqualValues(t, "A few miles south of Soledad, the Salinas River drops in close to the hillside\n\tbank and runs deep and green. The water is warm too, for it has slipped <b>twinkling</b>\n\tover the yellow sands in the sunlight before reaching the narrow pool. On one\n\tside of the river the golden foothill slopes curve up to the strong and rocky\n\tGabilan Mountains, but on the valley side the water is lined with trees—willows\n\tfresh and green with every spring, carrying in their lower leaf junctures the\n\tdebris of the winter&#x27;s flooding; and sycamores with mottled, white, recumbent\n\tlimbs and branches that arch over the pool", results["snippet_html"].(jm)["body"])
}

func TestSearchSelectFields(t *testing.T) {
	t.Setenv("LD_LIBRARY_PATH", ".")
	LibInit()
	td, err := ioutil.TempDir("", "tindex*")
	defer func(err error) {
		if err == nil {
			if os.RemoveAll(td) != nil {
				log.Error("unable to cleanup temp dir", "val", td)
			}
		}
	}(err)
	require.NoError(t, err)

	idx := makeIndex(t, td, false)
	rb, err := idx.ReaderBuilder()
	require.NoError(t, err)
	qp, err := rb.Searcher()
	require.NoError(t, err)

	_, err = qp.ForIndex([]string{"title", "body", "test"})
	require.NoError(t, err)

	searcher, err := qp.ParseQuery("title:Sea")
	require.NoError(t, err)

	s, err := searcher.SearchWithOptions(SearchOptions{
		TopLimit:     1,
		Ordered:      true,
		SelectFields: []string{"title"},
	})
	require.NoError(t, err)

	results := []map[string]interface{}{}
	err = json.Unmarshal([]byte(s), &results)
	require.NoError(t, err)
	require.EqualValues(t, "The Old Man and the Sea", results[0]["doc"].(map[string]interface{})["title"].([]interface{})[0].(string))
	_, hasBody := results[0]["doc"].(map[string]interface{})["body"]
	require.False(t, hasBody)

	docset, err := searcher.Docset(true, 1, 0)
	require.NoError(t, err)
	var docsetResults map[string]interface{}
	err = json.Unmarshal([]byte(docset), &docsetResults)
	require.NoError(t, err)
	docRef := docsetResults["docset"].([]interface{})[0].(jm)

	sDoc, err := searcher.GetDocumentWithOptions(GetDocumentOptions{
		Score:        float32(docRef["score"].(float64)),
		DocID:        uint32(docRef["doc_id"].(float64)),
		SegmentOrd:   uint32(docRef["segment_ord"].(float64)),
		SelectFields: []string{"title"},
	})
	require.NoError(t, err)

	var documentResult map[string]interface{}
	err = json.Unmarshal([]byte(sDoc), &documentResult)
	require.NoError(t, err)
	require.EqualValues(t, "The Old Man and the Sea", documentResult["doc"].(map[string]interface{})["title"].([]interface{})[0].(string))
	_, hasDocumentBody := documentResult["doc"].(map[string]interface{})["body"]
	require.False(t, hasDocumentBody)
}

func TestLargeSearchWithOptions(t *testing.T) {
	t.Setenv("LD_LIBRARY_PATH", ".")
	LibInit()
	td, err := ioutil.TempDir("", "tindex-large*")
	defer func(err error) {
		if err == nil {
			if os.RemoveAll(td) != nil {
				log.Error("unable to cleanup temp dir", "val", td)
			}
		}
	}(err)
	require.NoError(t, err)

	const docCount = 24
	idx := makeLargeIndex(t, td, docCount, 180)
	rb, err := idx.ReaderBuilder()
	require.NoError(t, err)
	qp, err := rb.Searcher()
	require.NoError(t, err)

	_, err = qp.ForIndex([]string{"title", "body", "test"})
	require.NoError(t, err)

	searcher, err := qp.ParseQuery("body:payload")
	require.NoError(t, err)

	fullResponse, err := searcher.Search(false, docCount, 0, true)
	require.NoError(t, err)
	projectedResponse, err := searcher.SearchWithOptions(SearchOptions{
		TopLimit:     docCount,
		Ordered:      true,
		SelectFields: []string{"title", "test"},
	})
	require.NoError(t, err)

	var fullResults []map[string]interface{}
	err = json.Unmarshal([]byte(fullResponse), &fullResults)
	require.NoError(t, err)
	require.Len(t, fullResults, docCount)

	var projectedResults []map[string]interface{}
	err = json.Unmarshal([]byte(projectedResponse), &projectedResults)
	require.NoError(t, err)
	require.Len(t, projectedResults, docCount)

	firstFullDoc := fullResults[0]["doc"].(map[string]interface{})
	require.NotEmpty(t, firstFullDoc["body"].([]interface{})[0].(string))
	for _, result := range projectedResults {
		projectedDoc := result["doc"].(map[string]interface{})
		_, hasProjectedBody := projectedDoc["body"]
		require.False(t, hasProjectedBody)
		require.True(t, strings.HasPrefix(projectedDoc["title"].([]interface{})[0].(string), "Large Search Demo "))
		testValue := projectedDoc["test"].([]interface{})[0].(float64)
		require.GreaterOrEqual(t, testValue, float64(1000))
		require.Less(t, testValue, float64(1000+docCount))
	}

	fullLen := len(fullResponse)
	projectedLen := len(projectedResponse)
	require.Greater(t, fullLen, projectedLen)
	require.GreaterOrEqual(t, fullLen/projectedLen, 8)
}

func TestLargeDocsetAllGetDocumentsWithOptions(t *testing.T) {
	t.Setenv("LD_LIBRARY_PATH", ".")
	LibInit()
	td, err := ioutil.TempDir("", "tindex-docset-large*")
	defer func(err error) {
		if err == nil {
			if os.RemoveAll(td) != nil {
				log.Error("unable to cleanup temp dir", "val", td)
			}
		}
	}(err)
	require.NoError(t, err)

	const docCount = 24
	idx := makeLargeIndex(t, td, docCount, 180)
	rb, err := idx.ReaderBuilder()
	require.NoError(t, err)
	qp, err := rb.Searcher()
	require.NoError(t, err)

	_, err = qp.ForIndex([]string{"title", "body", "test"})
	require.NoError(t, err)

	searcher, err := qp.ParseQuery("body:payload")
	require.NoError(t, err)

	docsetJSON, err := searcher.DocsetAll(true, 0)
	require.NoError(t, err)
	var docset docsetEnvelope
	err = json.Unmarshal([]byte(docsetJSON), &docset)
	require.NoError(t, err)
	require.Len(t, docset.Docset, docCount)

	documentsJSON, err := searcher.GetDocumentsWithOptions(docset.Docset, GetDocumentsOptions{
		SelectFields: []string{"title", "test"},
	})
	require.NoError(t, err)

	var documents []map[string]interface{}
	err = json.Unmarshal([]byte(documentsJSON), &documents)
	require.NoError(t, err)
	require.Len(t, documents, docCount)
	for _, document := range documents {
		doc := document["doc"].(map[string]interface{})
		_, hasBody := doc["body"]
		require.False(t, hasBody)
		require.True(t, strings.HasPrefix(doc["title"].([]interface{})[0].(string), "Large Search Demo "))
	}
}

func TestLargeSearchWithOptionsBatched(t *testing.T) {
	t.Setenv("LD_LIBRARY_PATH", ".")
	LibInit()
	td, err := ioutil.TempDir("", "tindex-batched-large*")
	defer func(err error) {
		if err == nil {
			if os.RemoveAll(td) != nil {
				log.Error("unable to cleanup temp dir", "val", td)
			}
		}
	}(err)
	require.NoError(t, err)

	const docCount = 24
	idx := makeLargeIndex(t, td, docCount, 180)
	rb, err := idx.ReaderBuilder()
	require.NoError(t, err)
	qp, err := rb.Searcher()
	require.NoError(t, err)

	_, err = qp.ForIndex([]string{"title", "body", "test"})
	require.NoError(t, err)

	searcher, err := qp.ParseQuery("body:payload")
	require.NoError(t, err)

	fullResponse, err := searcher.Search(false, docCount, 0, true)
	require.NoError(t, err)
	batchedResponse, err := searcher.SearchWithOptionsBatched(SearchOptions{
		Ordered:      true,
		SelectFields: []string{"title", "test"},
	}, 5)
	require.NoError(t, err)

	var batchedResults []map[string]interface{}
	err = json.Unmarshal([]byte(batchedResponse), &batchedResults)
	require.NoError(t, err)
	require.Len(t, batchedResults, docCount)
	for _, result := range batchedResults {
		doc := result["doc"].(map[string]interface{})
		_, hasBody := doc["body"]
		require.False(t, hasBody)
		require.NotEmpty(t, doc["title"].([]interface{})[0].(string))
	}
	require.Greater(t, len(fullResponse), len(batchedResponse))
	require.GreaterOrEqual(t, len(fullResponse)/len(batchedResponse), 8)
}

func benchmarkLargeSearchSetup(b *testing.B) *TSearcher {
	b.Helper()
	LibInit("error")
	idx := makeLargeIndex(b, "", 96, 180)
	rb, err := idx.ReaderBuilder()
	require.NoError(b, err)
	qp, err := rb.Searcher()
	require.NoError(b, err)
	_, err = qp.ForIndex([]string{"title", "body", "test"})
	require.NoError(b, err)
	searcher, err := qp.ParseQuery("body:payload")
	require.NoError(b, err)
	return searcher
}

func BenchmarkLargeSearchFull(b *testing.B) {
	b.Setenv("LD_LIBRARY_PATH", ".")
	searcher := benchmarkLargeSearchSetup(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		response, err := searcher.Search(false, 96, 0, true)
		if err != nil {
			b.Fatal(err)
		}
		if len(response) == 0 {
			b.Fatal("empty full search response")
		}
	}
}

func BenchmarkLargeSearchProjected(b *testing.B) {
	b.Setenv("LD_LIBRARY_PATH", ".")
	searcher := benchmarkLargeSearchSetup(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		response, err := searcher.SearchWithOptions(SearchOptions{
			TopLimit:     96,
			Ordered:      true,
			SelectFields: []string{"title", "test"},
		})
		if err != nil {
			b.Fatal(err)
		}
		if len(response) == 0 {
			b.Fatal("empty projected search response")
		}
	}
}

func BenchmarkLargeSearchBatched(b *testing.B) {
	b.Setenv("LD_LIBRARY_PATH", ".")
	searcher := benchmarkLargeSearchSetup(b)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		response, err := searcher.SearchWithOptionsBatched(SearchOptions{
			Ordered:      true,
			SelectFields: []string{"title", "test"},
		}, 16)
		if err != nil {
			b.Fatal(err)
		}
		if len(response) == 0 {
			b.Fatal("empty batched search response")
		}
	}
}

func TestStops(t *testing.T) {
	builder, err := NewBuilder("")
	require.NoError(t, err)
	idxFieldTitle, err := builder.AddTextField("title", TEXT, true, true, "", false)
	require.NoError(t, err)
	require.EqualValues(t, 0, idxFieldTitle)
	idxFieldBody, err := builder.AddTextField("body", TEXT, true, true, "", false)
	require.NoError(t, err)
	require.EqualValues(t, 1, idxFieldBody)
	idxFieldInt, err := builder.AddI64Field("order", INT, true, true, true)
	require.NoError(t, err)
	require.EqualValues(t, 2, idxFieldInt)
	doc, err := builder.Build()
	require.NoError(t, err)
	doc1, err := doc.Create()
	require.NoError(t, err)
	require.EqualValues(t, 1, doc1)
	doc2, err := doc.Create()
	require.NoError(t, err)
	require.EqualValues(t, 2, doc2)
	_, err = doc.AddText(idxFieldTitle, "The Old Man and the Sea", doc1)
	require.NoError(t, err)
	_, err = doc.AddText(idxFieldBody, "He was an old man who fished alone in a skiff in the Gulf Stream and he had gone eighty-four days now without taking a fish. The water was warm but fishless.", doc1)
	require.NoError(t, err)
	_, err = doc.AddInt(idxFieldInt, 1, doc1)
	require.NoError(t, err)
	_, err = doc.AddText(idxFieldTitle, "Of Mice and Men", doc2)
	require.NoError(t, err)
	_, err = doc.AddText(idxFieldBody, `A few miles south of Soledad, the Salinas River drops in close to the hillside
	bank and runs deep and green. The water is warm too, for it has slipped twinkling
	over the yellow sands in the sunlight before reaching the narrow pool. On one
	side of the river the golden foothill slopes curve up to the strong and rocky
	Gabilan Mountains, but on the valley side the water is lined with trees—willows
	fresh and green with every spring, carrying in their lower leaf junctures the
	debris of the winter's flooding; and sycamores with mottled, white, recumbent
	limbs and branches that arch over the pool`, doc2)
	require.NoError(t, err)
	_, err = doc.AddInt(idxFieldInt, 2, doc2)
	require.NoError(t, err)

	indexer, err := doc.CreateIndex()
	require.NoError(t, err)

	idw, err := indexer.CreateIndexWriter()
	require.NoError(t, err)
	opst1, err := idw.AddDocument(doc1)
	require.NoError(t, err)
	require.EqualValues(t, 0, opst1)
	opst2, err := idw.AddDocument(doc2)
	require.NoError(t, err)
	require.EqualValues(t, 1, opst2)
	fmt.Printf("op1 = %v op2 = %v\n", opst1, opst2)
	idCommit, err := idw.Commit()
	require.NoError(t, err)
	fmt.Printf("commit id = %v", idCommit)

	rb, err := indexer.ReaderBuilder()
	require.NoError(t, err)
	qp, err := rb.Searcher()
	require.NoError(t, err)

	_, err = qp.ForIndex([]string{"title", "body"})
	require.NoError(t, err)

	searcher, err := qp.ParseQuery("title:the")
	require.NoError(t, err)
	s, err := searcher.Search(false, 0, 0, true)
	require.NoError(t, err)
	results := []map[string]interface{}{}
	err = json.Unmarshal([]byte(s), &results)
	require.NoError(t, err)
	require.EqualValues(t, 0, len(results))
}

func TestBasicIndexing(t *testing.T) {
	builder, err := NewBuilder("")
	require.NoError(t, err)
	idxFieldTitle, err := builder.AddTextField("title", TEXT, true, true, "", false)
	require.NoError(t, err)
	require.EqualValues(t, 0, idxFieldTitle)
	idxFieldBody, err := builder.AddTextField("body", TEXT, true, true, "", true)
	require.NoError(t, err)
	require.EqualValues(t, 1, idxFieldBody)
	idxFieldInt, err := builder.AddI64Field("order", INT, true, true, true)
	require.NoError(t, err)
	require.EqualValues(t, 2, idxFieldInt)
	doc, err := builder.Build()
	require.NoError(t, err)
	doc1, err := doc.Create()
	require.NoError(t, err)
	require.EqualValues(t, 1, doc1)
	doc2, err := doc.Create()
	require.NoError(t, err)
	require.EqualValues(t, 2, doc2)
	_, err = doc.AddText(idxFieldTitle, "The Old Man and the Sea", doc1)
	require.NoError(t, err)
	_, err = doc.AddText(idxFieldBody, "He was an old man who fished alone in a skiff in the Gulf Stream and he had gone eighty-four days now without taking a fish. The water was warm but fishless.", doc1)
	require.NoError(t, err)
	_, err = doc.AddInt(idxFieldInt, 1, doc1)
	require.NoError(t, err)
	_, err = doc.AddText(idxFieldTitle, "Of Mice and Men", doc2)
	require.NoError(t, err)
	_, err = doc.AddText(idxFieldBody, `A few miles south of Soledad, the Salinas River drops in close to the hillside
	bank and runs deep and green. The water is warm too, for it has slipped twinkling
	over the yellow sands in the sunlight before reaching the narrow pool. On one
	side of the river the golden foothill slopes curve up to the strong and rocky
	Gabilan Mountains, but on the valley side the water is lined with trees—willows
	fresh and green with every spring, carrying in their lower leaf junctures the
	debris of the winter's flooding; and sycamores with mottled, white, recumbent
	limbs and branches that arch over the pool`, doc2)
	require.NoError(t, err)
	_, err = doc.AddInt(idxFieldInt, 2, doc2)
	require.NoError(t, err)

	indexer, err := doc.CreateIndex()
	require.NoError(t, err)

	idw, err := indexer.CreateIndexWriter()
	require.NoError(t, err)
	opst1, err := idw.AddDocument(doc1)
	require.NoError(t, err)
	require.EqualValues(t, 0, opst1)
	opst2, err := idw.AddDocument(doc2)
	require.NoError(t, err)
	require.EqualValues(t, 1, opst2)
	fmt.Printf("op1 = %v op2 = %v\n", opst1, opst2)
	idCommit, err := idw.Commit()
	require.NoError(t, err)
	fmt.Printf("commit id = %v", idCommit)

	rb, err := indexer.ReaderBuilder()
	require.NoError(t, err)
	qp, err := rb.Searcher()
	require.NoError(t, err)

	_, err = qp.ForIndex([]string{"title", "body"})
	require.NoError(t, err)

	searcher, err := qp.ParseQuery("order:1")
	require.NoError(t, err)
	s, err := searcher.Search(false, 0, 0, true)
	require.NoError(t, err)
	results := []map[string]interface{}{}
	err = json.Unmarshal([]byte(s), &results)
	require.NoError(t, err)
	require.EqualValues(t, "The Old Man and the Sea", results[0]["doc"].(map[string]interface{})["title"].([]interface{})[0].(string))

	searcherAgain, err := qp.ParseQuery("order:2")
	require.NoError(t, err)
	s, err = searcherAgain.Search(true, 0, 0, true)
	require.NoError(t, err)
	err = json.Unmarshal([]byte(s), &results)
	require.NoError(t, err)
	require.EqualValues(t, "Of Mice and Men", results[0]["doc"].(map[string]interface{})["title"].([]interface{})[0].(string))

	searcher, err = qp.ParseQuery("\"of mice\"")
	require.NoError(t, err)
	s, err = searcher.Search(false, 0, 0, true)
	require.NoError(t, err)
	results = []map[string]interface{}{}
	err = json.Unmarshal([]byte(s), &results)
	require.NoError(t, err)
	require.EqualValues(t, "Of Mice and Men", results[0]["doc"].(map[string]interface{})["title"].([]interface{})[0].(string))

	searcher, err = qp.ParseQuery("fished alone")
	require.NoError(t, err)
	s, err = searcher.Search(false, 0, 0, true)
	require.NoError(t, err)
	results = []map[string]interface{}{}
	err = json.Unmarshal([]byte(s), &results)
	require.NoError(t, err)
	require.EqualValues(t, "The Old Man and the Sea", results[0]["doc"].(map[string]interface{})["title"].([]interface{})[0].(string))
}

func TestIndexer(t *testing.T) {
	builder, err := NewBuilder("")
	require.NoError(t, err)
	idxFieldTitle, err := builder.AddTextField("title", TEXT, true, true, "", false)
	require.NoError(t, err)
	require.EqualValues(t, 0, idxFieldTitle)
	idxFieldBody, err := builder.AddTextField("body", TEXT, true, true, "", false)
	require.NoError(t, err)
	require.EqualValues(t, 1, idxFieldBody)
	idxFieldInt, err := builder.AddI64Field("order", INT, true, true, true)
	require.NoError(t, err)
	require.EqualValues(t, 2, idxFieldInt)
	doc, err := builder.Build()
	require.NoError(t, err)
	doc1, err := doc.Create()
	require.NoError(t, err)
	require.EqualValues(t, 1, doc1)
	doc2, err := doc.Create()
	require.NoError(t, err)
	require.EqualValues(t, 2, doc2)
	_, err = doc.AddText(idxFieldTitle, "The Old Man and the Sea", doc1)
	require.NoError(t, err)
	_, err = doc.AddText(idxFieldBody, "He was an old man who fished alone in a skiff in the Gulf Stream and he had gone eighty-four days now without taking a fish. The water was warm but fishless.", doc1)
	require.NoError(t, err)
	_, err = doc.AddInt(idxFieldInt, 1, doc1)
	require.NoError(t, err)
	_, err = doc.AddText(idxFieldTitle, "Of Mice and Men", doc2)
	require.NoError(t, err)
	_, err = doc.AddText(idxFieldBody, `A few miles south of Soledad, the Salinas River drops in close to the hillside
	bank and runs deep and green. The water is warm too, for it has slipped twinkling
	over the yellow sands in the sunlight before reaching the narrow pool. On one
	side of the river the golden foothill slopes curve up to the strong and rocky
	Gabilan Mountains, but on the valley side the water is lined with trees—willows
	fresh and green with every spring, carrying in their lower leaf junctures the
	debris of the winter's flooding; and sycamores with mottled, white, recumbent
	limbs and branches that arch over the pool`, doc2)
	require.NoError(t, err)
	_, err = doc.AddInt(idxFieldInt, 2, doc2)
	require.NoError(t, err)

	indexer, err := doc.CreateIndex()
	require.NoError(t, err)

	idw, err := indexer.CreateIndexWriter()
	require.NoError(t, err)
	opst1, err := idw.AddDocument(doc1)
	require.NoError(t, err)
	require.EqualValues(t, 0, opst1)
	opst2, err := idw.AddDocument(doc2)
	require.NoError(t, err)
	require.EqualValues(t, 1, opst2)
	fmt.Printf("op1 = %v op2 = %v\n", opst1, opst2)
	idCommit, err := idw.Commit()
	require.NoError(t, err)
	fmt.Printf("commit id = %v", idCommit)

	rb, err := indexer.ReaderBuilder()
	require.NoError(t, err)
	qp, err := rb.Searcher()
	require.NoError(t, err)

	_, err = qp.ForIndex([]string{"title", "body"})
	require.NoError(t, err)

	searcher, err := qp.ParseQuery("order:1")
	require.NoError(t, err)
	s, err := searcher.Search(false, 0, 0, true)
	require.NoError(t, err)
	results := []map[string]interface{}{}
	err = json.Unmarshal([]byte(s), &results)
	require.NoError(t, err)
	require.EqualValues(t, "The Old Man and the Sea", results[0]["doc"].(map[string]interface{})["title"].([]interface{})[0].(string))

	searcherAgain, err := qp.ParseQuery("order:2")
	require.NoError(t, err)
	s, err = searcherAgain.Search(true, 0, 0, true)
	require.NoError(t, err)
	err = json.Unmarshal([]byte(s), &results)
	require.NoError(t, err)
	require.EqualValues(t, "Of Mice and Men", results[0]["doc"].(map[string]interface{})["title"].([]interface{})[0].(string))
}

func TestIndexerJsonField(t *testing.T) {
	builder, err := NewBuilder("")
	require.NoError(t, err)
	idxFieldTitle, err := builder.AddTextField("title", TEXT, true, true, "", false)
	require.NoError(t, err)
	require.EqualValues(t, 0, idxFieldTitle)
	idxFieldBody, err := builder.AddJsonField("body", JSON, true, true, "", false)
	require.NoError(t, err)
	require.EqualValues(t, 1, idxFieldBody)
	idxFieldInt, err := builder.AddI64Field("order", INT, true, true, true)
	require.NoError(t, err)
	require.EqualValues(t, 2, idxFieldInt)
	doc, err := builder.Build()
	require.NoError(t, err)
	doc1, err := doc.Create()
	require.NoError(t, err)
	require.EqualValues(t, 1, doc1)
	doc2, err := doc.Create()
	require.NoError(t, err)
	require.EqualValues(t, 2, doc2)
	_, err = doc.AddText(idxFieldTitle, "The Old Man and the Sea", doc1)
	require.NoError(t, err)
	_, err = doc.AddJson(idxFieldBody, msi{"contents": "He was an old man who fished alone in a skiff in the Gulf Stream and he had gone eighty-four days now without taking a fish. The water was warm but fishless."}, doc1)
	require.NoError(t, err)
	_, err = doc.AddInt(idxFieldInt, 1, doc1)
	require.NoError(t, err)
	_, err = doc.AddText(idxFieldTitle, "Of Mice and Men", doc2)
	require.NoError(t, err)
	_, err = doc.AddJson(idxFieldBody, msi{"contents": `A few miles south of Soledad, the Salinas River drops in close to the hillside
	bank and runs deep and green. The water is warm too, for it has slipped twinkling
	over the yellow sands in the sunlight before reaching the narrow pool. On one
	side of the river the golden foothill slopes curve up to the strong and rocky
	Gabilan Mountains, but on the valley side the water is lined with trees—willows
	fresh and green with every spring, carrying in their lower leaf junctures the
	debris of the winter's flooding; and sycamores with mottled, white, recumbent
	limbs and branches that arch over the pool`}, doc2)
	require.NoError(t, err)
	_, err = doc.AddInt(idxFieldInt, 2, doc2)
	require.NoError(t, err)

	indexer, err := doc.CreateIndex()
	require.NoError(t, err)

	idw, err := indexer.CreateIndexWriter()
	require.NoError(t, err)
	opst1, err := idw.AddDocument(doc1)
	require.NoError(t, err)
	require.EqualValues(t, 0, opst1)
	opst2, err := idw.AddDocument(doc2)
	require.NoError(t, err)
	require.EqualValues(t, 1, opst2)
	fmt.Printf("op1 = %v op2 = %v\n", opst1, opst2)
	idCommit, err := idw.Commit()
	require.NoError(t, err)
	fmt.Printf("commit id = %v", idCommit)

	rb, err := indexer.ReaderBuilder()
	require.NoError(t, err)
	qp, err := rb.Searcher()
	require.NoError(t, err)

	_, err = qp.ForIndex([]string{"title", "body"})
	require.NoError(t, err)

	searcher, err := qp.ParseQuery("order:1")
	require.NoError(t, err)
	s, err := searcher.Search(false, 0, 0, true)
	require.NoError(t, err)
	results := []map[string]interface{}{}
	err = json.Unmarshal([]byte(s), &results)
	require.NoError(t, err)
	require.EqualValues(t, "The Old Man and the Sea", results[0]["doc"].(map[string]interface{})["title"].([]interface{})[0].(string))
	contents := results[0]["doc"].(map[string]interface{})["body"].([]interface{})[0].(string)
	contentsJson := map[string]interface{}{}
	err = json.Unmarshal([]byte(contents), &contentsJson)
	require.NoError(t, err)
	require.EqualValues(t, "He was an old man who fished alone in a skiff in the Gulf Stream and he had gone eighty-four days now without taking a fish. The water was warm but fishless.", contentsJson["contents"].(string))

	searcherAgain, err := qp.ParseQuery("order:2")
	require.NoError(t, err)
	s, err = searcherAgain.Search(true, 0, 0, true)
	require.NoError(t, err)
	err = json.Unmarshal([]byte(s), &results)
	require.NoError(t, err)
	require.EqualValues(t, "Of Mice and Men", results[0]["doc"].(map[string]interface{})["title"].([]interface{})[0].(string))
}

func TestTantivyFuzzy(t *testing.T) {
	wd, err := os.Getwd()
	require.NoError(t, err)
	fmt.Printf("WD = %s", wd)
	t.Setenv("LD_LIBRARY_PATH", ".")
	LibInit()
	td, err := ioutil.TempDir("", "tindex*")
	defer func(err error) {
		if err == nil {
			if os.RemoveAll(td) != nil {
				log.Error("unable to cleanup temp dir", "val", td)
			}
		}
	}(err)
	assert.NoError(t, err)
	idx := makeFuzzyIndex(t, td, false)
	testFuzzyExpectedIndex(t, idx)
}

func TestTantivyTopLimit(t *testing.T) {
	idx := makeIndex(t, "", false)
	testExpectedTopIndex(t, idx)

}
func TestTantivyIndexReuse(t *testing.T) {
	wd, err := os.Getwd()
	require.NoError(t, err)
	fmt.Printf("WD = %s", wd)
	t.Setenv("LD_LIBRARY_PATH", ".")
	LibInit()
	td, err := ioutil.TempDir("", "tindex*")
	defer func(err error) {
		if err == nil {
			if os.RemoveAll(td) != nil {
				log.Error("unable to cleanup temp dir", "val", td)
			}
		}
	}(err)
	assert.NoError(t, err)
	_ = makeIndex(t, td, false)

	idx := loadIndex(t, td)
	testExpectedIndex(t, idx)
}

func TestTantivyStress(t *testing.T) {
	builder, err := NewBuilder("")
	require.NoError(t, err)
	fieldIds := map[string]int{}
	fields := []string{"title", "body", "speech", "shot", "action", "logo", "segment", "celeb", "cast"}
	fieldsLong := []string{"description", "has_field"}
	for _, f := range fields {
		fieldIds[f], err = builder.AddTextField(f, TEXT, true, true, "", false)
		require.NoError(t, err)
	}
	for _, f := range fieldsLong {
		fieldIds[f], err = builder.AddTextField(f, TEXT, true, true, "", false)
		require.NoError(t, err)
	}

	doc, err := builder.Build()
	require.NoError(t, err)
	ti, err := doc.CreateIndex()
	require.NoError(t, err)
	tiw, err := ti.CreateIndexWriter()
	require.NoError(t, err)

	text := "He was an old man who fished alone in a skiff in the Gulf Stream and he had gone eighty-four days now without taking a fish."
	text2 := `A few miles south of Soledad, the Salinas River drops in close to the hillside
	bank and runs deep and green. The water is warm too, for it has slipped twinkling
	over the yellow sands in the sunlight before reaching the narrow pool. On one
	side of the river the golden foothill slopes curve up to the strong and rocky
	Gabilan Mountains, but on the valley side the water is lined with trees—willows
	fresh and green with every spring, carrying in their lower leaf junctures the
	debris of the winter's flooding; and sycamores with mottled, white, recumbent
	limbs and branches that arch over the pool`
	for i := 0; i < 1041; i++ {
		newDoc, err := doc.Create()
		require.NoError(t, err)
		for _, f := range fields {
			_, err = doc.AddText(fieldIds[f], text, newDoc)
			require.NoError(t, err)
		}
		for _, f := range fieldsLong {
			_, err = doc.AddText(fieldIds[f], text2, newDoc)
			require.NoError(t, err)
		}
		_, err = tiw.AddDocument(newDoc)
		require.NoError(t, err)
	}
	_, err = tiw.Commit()
	require.NoError(t, err)
}

func TestTantivyDeleteTerm(t *testing.T) {
	builder, err := NewBuilder("")
	require.NoError(t, err)
	idxFieldTitle, err := builder.AddTextField("title", TEXT, true, true, "", false)
	require.NoError(t, err)
	require.EqualValues(t, 0, idxFieldTitle)
	idxFieldInt, err := builder.AddI64Field("test", INT, true, true, true)
	require.NoError(t, err)
	require.EqualValues(t, 1, idxFieldInt)
	doc, err := builder.Build()
	require.NoError(t, err)
	d1, err := doc.Create()
	require.NoError(t, err)
	ti, err := doc.CreateIndex()
	require.NoError(t, err)
	tiw, err := ti.CreateIndexWriter()
	require.NoError(t, err)
	_, err = doc.AddText(idxFieldTitle, "FooFoo", d1)
	require.NoError(t, err)
	_, err = doc.AddInt(idxFieldInt, 444, d1)
	require.NoError(t, err)
	_, err = tiw.DeleteTerm("test", 444)
	require.NoError(t, err)

}

func TestFilenameTokenizer(t *testing.T) {
	builder, err := NewBuilder("")
	require.NoError(t, err)
	idxFieldTitle, err := builder.AddTextField("title", TEXT, true, true, "filename", false)
	require.NoError(t, err)
	require.EqualValues(t, 0, idxFieldTitle)
	idxFieldBody, err := builder.AddTextField("body", TEXT, true, true, "filename", false)
	require.NoError(t, err)
	require.EqualValues(t, 1, idxFieldBody)
	idxFieldInt, err := builder.AddI64Field("order", INT, true, true, true)
	require.NoError(t, err)
	require.EqualValues(t, 2, idxFieldInt)
	doc, err := builder.Build()
	require.NoError(t, err)
	doc1, err := doc.Create()
	require.NoError(t, err)
	require.EqualValues(t, 1, doc1)
	doc2, err := doc.Create()
	require.NoError(t, err)
	require.EqualValues(t, 2, doc2)
	_, err = doc.AddText(idxFieldTitle, "The OldMan and theSea", doc1)
	require.NoError(t, err)
	_, err = doc.AddText(idxFieldBody, "He was an old man who fished alone in a skiff in the Gulf Stream and he had gone eighty-four days now without taking a fish. The water was warm but fishless.", doc1)
	require.NoError(t, err)
	_, err = doc.AddInt(idxFieldInt, 1, doc1)
	require.NoError(t, err)
	_, err = doc.AddText(idxFieldTitle, "Of Mice and Men", doc2)
	require.NoError(t, err)
	_, err = doc.AddText(idxFieldBody, `A few miles south of Soledad, the Salinas River drops in close to the hillside
	bank and runs deep and green. The water is warm too, for it has slipped twinkling
	over the yellow sands in the sunlight before reaching the narrow pool. On one
	side of the river the golden foothill slopes curve up to the strong and rocky
	Gabilan Mountains, but on the valley side the water is lined with trees—willows
	fresh and green with every spring, carrying in their lower leaf junctures the
	debris of the winter's flooding; and sycamores with mottled, white, recumbent
	limbs and branches that arch over the pool`, doc2)
	require.NoError(t, err)
	_, err = doc.AddInt(idxFieldInt, 2, doc2)
	require.NoError(t, err)

	indexer, err := doc.CreateIndex()
	require.NoError(t, err)

	idw, err := indexer.CreateIndexWriter()
	require.NoError(t, err)
	opst1, err := idw.AddDocument(doc1)
	require.NoError(t, err)
	require.EqualValues(t, 0, opst1)
	opst2, err := idw.AddDocument(doc2)
	require.NoError(t, err)
	require.EqualValues(t, 1, opst2)
	fmt.Printf("op1 = %v op2 = %v\n", opst1, opst2)
	idCommit, err := idw.Commit()
	require.NoError(t, err)
	fmt.Printf("commit id = %v", idCommit)

	rb, err := indexer.ReaderBuilder()
	require.NoError(t, err)
	qp, err := rb.Searcher()
	require.NoError(t, err)

	_, err = qp.ForIndex([]string{"title", "body"})
	require.NoError(t, err)

	searcher, err := qp.ParseQuery("title:man")
	require.NoError(t, err)
	s, err := searcher.Search(false, 0, 0, true)
	require.NoError(t, err)
	results := []map[string]interface{}{}
	err = json.Unmarshal([]byte(s), &results)
	require.NoError(t, err)
	require.EqualValues(t, "The OldMan and theSea", results[0]["doc"].(map[string]interface{})["title"].([]interface{})[0].(string))

	searcherAgain, err := qp.ParseQuery("mice")
	require.NoError(t, err)
	s, err = searcherAgain.Search(true, 0, 0, true)
	require.NoError(t, err)
	err = json.Unmarshal([]byte(s), &results)
	require.NoError(t, err)
	require.EqualValues(t, "Of Mice and Men", results[0]["doc"].(map[string]interface{})["title"].([]interface{})[0].(string))
}

func TestChangeKB(t *testing.T) {
	LibInit()
	SetKB(1.0, 0.80)

}
