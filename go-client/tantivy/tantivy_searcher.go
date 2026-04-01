package tantivy

import (
	"bytes"
	"encoding/json"
	"fmt"
)

type TSearcher struct {
	*TQueryParser
}

const NOSNIPPET = -1

type SearchOptions struct {
	Explain       bool
	TopLimit      uint64
	Offset        uint64
	Ordered       bool
	SnippetFields []string
	SelectFields  []string
}

type GetDocumentOptions struct {
	Explain       bool
	Score         float32
	DocID         uint32
	SegmentOrd    uint32
	SnippetFields []string
	SelectFields  []string
}

type GetDocumentsOptions struct {
	Explain       bool
	SnippetFields []string
	SelectFields  []string
}

type SearchResultRef struct {
	Score      float32 `json:"score"`
	SegmentOrd uint32  `json:"segment_ord"`
	DocID      uint32  `json:"doc_id"`
}

type docsetEnvelope struct {
	Docset []SearchResultRef `json:"docset"`
}

func (s *TSearcher) Docset(scoring bool, topLimit uint64, offset uint64) (string, error) {
	return s.callTantivy("searcher", "docset", msi{
		"top_limit": topLimit,
		"offset":    offset,
		"scoring":   scoring,
	})
}

func (s *TSearcher) DocsetAll(scoring bool, offset uint64) (string, error) {
	return s.callTantivy("searcher", "docset", msi{
		"top_limit": uint64(0),
		"offset":    offset,
		"scoring":   scoring,
	})
}

func (s *TSearcher) GetDocument(explain bool, score float32, docId uint32, segOrd uint32, snippetField ...string) (string, error) {
	return s.GetDocumentWithOptions(GetDocumentOptions{
		Explain:       explain,
		Score:         score,
		DocID:         docId,
		SegmentOrd:    segOrd,
		SnippetFields: snippetField,
	})
}

func (s *TSearcher) Search(explain bool, topLimit uint64, offset uint64, ordered bool, snippetField ...string) (string, error) {
	return s.SearchWithOptions(SearchOptions{
		Explain:       explain,
		TopLimit:      topLimit,
		Offset:        offset,
		Ordered:       ordered,
		SnippetFields: snippetField,
	})
}

func (s *TSearcher) SearchWithOptions(options SearchOptions) (string, error) {
	args := msi{
		"scoring": options.Ordered,
		"offset":  options.Offset,
	}
	if len(options.SnippetFields) > 0 {
		args["snippet_field"] = options.SnippetFields
	}
	if len(options.SelectFields) > 0 {
		args["select_fields"] = options.SelectFields
	}
	if options.TopLimit >= 1 {
		args["top_limit"] = options.TopLimit
	}
	if options.Explain {
		args["explain"] = true
	}
	return s.callTantivy("searcher", "search", args)
}

func (s *TSearcher) GetDocumentWithOptions(options GetDocumentOptions) (string, error) {
	args := msi{
		"segment_ord": options.SegmentOrd,
		"doc_id":      options.DocID,
		"score":       options.Score,
		"explain":     options.Explain,
	}
	if len(options.SnippetFields) > 0 {
		args["snippet_field"] = options.SnippetFields
	}
	if len(options.SelectFields) > 0 {
		args["select_fields"] = options.SelectFields
	}
	return s.callTantivy("searcher", "get_document", args)
}

func (s *TSearcher) GetDocumentsWithOptions(docs []SearchResultRef, options GetDocumentsOptions) (string, error) {
	args := msi{
		"docs":    docs,
		"explain": options.Explain,
	}
	if len(options.SnippetFields) > 0 {
		args["snippet_field"] = options.SnippetFields
	}
	if len(options.SelectFields) > 0 {
		args["select_fields"] = options.SelectFields
	}
	return s.callTantivy("searcher", "get_documents", args)
}

func (s *TSearcher) SearchWithOptionsBatched(options SearchOptions, batchSize uint64) (string, error) {
	docsetResponse, err := s.docsetForOptions(options)
	if err != nil {
		return "", err
	}
	if len(docsetResponse.Docset) == 0 {
		return "[]", nil
	}
	if batchSize == 0 {
		batchSize = 256
	}

	var out bytes.Buffer
	out.WriteByte('[')
	first := true
	for start := 0; start < len(docsetResponse.Docset); start += int(batchSize) {
		end := start + int(batchSize)
		if end > len(docsetResponse.Docset) {
			end = len(docsetResponse.Docset)
		}
		batchJSON, err := s.GetDocumentsWithOptions(docsetResponse.Docset[start:end], GetDocumentsOptions{
			Explain:       options.Explain,
			SnippetFields: options.SnippetFields,
			SelectFields:  options.SelectFields,
		})
		if err != nil {
			return "", err
		}
		if err := appendJSONArray(&out, batchJSON, &first); err != nil {
			return "", err
		}
	}
	out.WriteByte(']')
	return out.String(), nil
}

func (s *TSearcher) SearchRaw(limit ...uint64) (string, error) {
	args := msi{}
	if len(limit) >= 1 && limit[0] >= 1 {
		args["limit"] = limit[0]
	}
	return s.callTantivy("searcher", "search_raw", args)
}

func (s *TSearcher) FuzzySearch(topLimit ...uint64) (string, error) {
	args := msi{}
	if len(topLimit) >= 1 {
		args["top_limit"] = topLimit[0]
	}
	return s.callTantivy("fuzzy_searcher", "fuzzy_searcher", msi{})
}

func (s *TSearcher) docsetForOptions(options SearchOptions) (*docsetEnvelope, error) {
	args := msi{
		"offset":  options.Offset,
		"scoring": options.Ordered,
	}
	if options.TopLimit > 0 {
		args["top_limit"] = options.TopLimit
	} else {
		args["top_limit"] = uint64(0)
	}
	docsetJSON, err := s.callTantivy("searcher", "docset", args)
	if err != nil {
		return nil, err
	}
	var envelope docsetEnvelope
	if err := json.Unmarshal([]byte(docsetJSON), &envelope); err != nil {
		return nil, fmt.Errorf("failed to decode docset response: %w", err)
	}
	return &envelope, nil
}

func appendJSONArray(out *bytes.Buffer, arrayJSON string, first *bool) error {
	var docs []json.RawMessage
	if err := json.Unmarshal([]byte(arrayJSON), &docs); err != nil {
		return fmt.Errorf("failed to decode batch documents: %w", err)
	}
	for _, doc := range docs {
		if !*first {
			out.WriteByte(',')
		} else {
			*first = false
		}
		out.Write(doc)
	}
	return nil
}
